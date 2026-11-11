package main

import (
	"database/sql"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/authnull0/database-agent/src/pkg"
	"github.com/authnull0/database-agent/utils"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/kardianos/service"
	_ "github.com/lib/pq"
	_ "github.com/sijms/go-ora"
	"github.com/spf13/viper"
)

var config pkg.DBConfig

type program struct {
	exit       chan struct{}
	dbUserName string
	dbPassword string
	dbHost     string
	// apiKey     string
}

func (p *program) Start(s service.Service) error {
	p.exit = make(chan struct{})
	go p.Run()
	return nil
}

func (p *program) Run() {
	startAgent(p.exit)
}

func (p *program) Stop(s service.Service) error {
	close(p.exit)
	return nil
}

func loadConfig(path string) (pkg.DBConfig, error) {
	viper.AddConfigPath(path)
	viper.SetConfigName("db")
	viper.SetConfigType("env")
	viper.AutomaticEnv()

	log.Printf("Looking for config file in path: %s", path)

	err := viper.ReadInConfig()
	if err != nil {
		return pkg.DBConfig{}, err
	}

	// The rotation interval is set in the AuthNull console now (AUTH-233). Say so, rather than
	// let an installer believe the value in db.env still does something.
	if viper.IsSet("PASSWORD_ROTATION_HOURS") {
		log.Printf("PASSWORD_ROTATION_HOURS in db.env is no longer read: the password rotation " +
			"interval is set per tenant in the AuthNull console")
	}

	// LOG_LEVEL=debug, in db.env or the environment, adds the per-user, per-table and per-payload
	// detail (pkg/logdebug.go). Unset, the log carries failures, changes and one line per database.
	pkg.SetLogLevel(viper.GetString("LOG_LEVEL"))

	var config pkg.DBConfig
	err = viper.Unmarshal(&config)
	log.Printf("Org ID: %s", config.OrgID)
	log.Printf("Tenant ID: %s", config.TenantID)

	return config, err
}

// LoadDataSource loads the data-source.yaml configuration file
func LoadDataSource() (pkg.DataSourceConfig, error) {
	viper.Reset()
	viper.SetConfigName("data-source")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		return pkg.DataSourceConfig{}, err
	}

	var ds pkg.DataSourceConfig
	err := viper.Unmarshal(&ds)
	return ds, err
}

func startAgent(exit chan struct{}) {
	log.Println("Starting Authnull Database Agent V1.1")

	// Load the configuration
	var err error
	var timeInterval int
	var conn *sql.DB
	var cfg pkg.DBConfig
	config, err = loadConfig("./")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Configuration loaded: %v", config)
	// The install's credential, for every call to the control plane (pkg/apiauth.go).
	pkg.SetInstallCredential(config.API, config.ProxyClientID, config.ProxyClientSecret)

	timeInterval, err = strconv.Atoi(config.TimeInterval)
	if err != nil {
		log.Default().Println(err)
	}
	dsCfg, err := LoadDataSource()
	if err != nil {
		log.Fatalf("Failed to load data-source.yaml: %v", err)
	}

	if len(dsCfg.Databases) == 0 {
		log.Fatal("No databases configured")
	}

	// Hold all active DB connections
	type DBHandle struct {
		Conn *sql.DB
		Cfg  pkg.DBConfig
	}

	var dbHandles []DBHandle

	for _, db := range dsCfg.Databases {
		log.Printf("Connecting to %s:%s", db.Host, db.Port)

		password, err := utils.DecryptPassword(db.Password, config.Key)
		if err != nil {
			log.Printf("Password decrypt failed for %s: %v", db.Host, err)
			continue
		}

		cfg = pkg.DBConfig{
			OrgID:      config.OrgID,
			TenantID:   config.TenantID,
			MachineKey: config.MachineKey,
			Key:        config.Key,
			Host:       db.Host,
			API:        config.API,
			User:       db.Username,
			Password:   password,
			Port:       db.Port,
			DBType:     db.Type,

			ProxysqlAdminUser:     config.ProxysqlAdminUser,
			ProxysqlAdminPassword: config.ProxysqlAdminPassword,
			ProxysqlAdminAddr:     config.ProxysqlAdminAddr,
		}

		conn, err = pkg.ConnectToDB(cfg)
		if err != nil {
			log.Printf("DB connection failed %s: %v", db.Host, err)
			continue
		}

		log.Printf("Connected successfully to %s", db.Host)

		// Initialize ProxySQL with host group 0 for first time
		log.Default().Printf("Initializing ProxySQL...")
		if err := pkg.InitializeProxySQL(cfg, 0); err != nil {
			log.Printf("Warning: ProxySQL initialization failed: %v", err)
			// Continue anyway - ProxySQL might not be available or already initialized
		}

		dbHandles = append(dbHandles, DBHandle{Conn: conn, Cfg: cfg})

	}
	if len(dbHandles) == 0 {
		log.Fatal("No database connections could be established")
	}

	// Live sessions: reported every 10 seconds, so a kill from the console lands within seconds
	// (pkg/sessions.go, authnull-service docs/DB-LIVE-SESSIONS.md).
	var sessionTargets []pkg.DBConfig
	for _, h := range dbHandles {
		sessionTargets = append(sessionTargets, h.Cfg)
	}
	stopSessions := make(chan struct{})
	defer close(stopSessions)
	go pkg.RunSessionsLoop(sessionTargets, 10*time.Second, stopSessions)
	// Session recording: ProxySQL's spool of recorded statements, uploaded as each 5-second file
	// closes (pkg/recordings.go, authnull-service docs/DB-SESSION-RECORDING.md).
	go pkg.RunRecordingsLoop(sessionTargets, pkg.RecordingDir(), 5*time.Second, stopSessions)

	// Ticker to run the synchronization every minute
	ticker := time.NewTicker(time.Duration(timeInterval) * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pkg.Debugf("DB Synchronization Started...")

			for _, h := range dbHandles {
				pkg.Debugf("Syncing database %s", h.Cfg.Host)

				if err := pkg.FetchDatabaseDetails(h.Conn, h.Cfg); err != nil {
					log.Printf("Sync failed for %s: %v", h.Cfg.Host, err)
				}
				if err := pkg.RotateDuePasswordsForHost(h.Conn, h.Cfg, time.Now()); err != nil {
					log.Printf("Password rotation failed for %s: %v", h.Cfg.Host, err)
				}
				// Passwords the backend holds in its vault, for roles it manages there.
				if err := pkg.ProcessVaultJobsForHost(h.Conn, h.Cfg, time.Now()); err != nil {
					log.Printf("Vault password jobs failed for %s: %v", h.Cfg.Host, err)
				}
			}
			log.Default().Println("DB Synchronization Completed.")
		case <-exit:
			log.Println("Stopping agent...")
			for _, h := range dbHandles {
				h.Conn.Close()
			}
			return
		}
	}
}

// openLogFile opens the agent's log for appending, 0640: it names every database user and the
// agent's addresses. A log an older agent created 0644 is tightened. It is rotated by logrotate
// with copytruncate (packaging/logrotate/authnull-db-agent), which O_APPEND keeps correct.
func openLogFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0640)
	if err != nil {
		return nil, err
	}
	if err := f.Chmod(0640); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func main() {
	// Command-line flags for user inputs
	// dbPort := flag.String("port", "", "Database port")
	// dbHost := flag.String("host", "", "Database host")
	// dbUserName := flag.String("username", "", "Database username")
	// dbPassword := flag.String("password", "", "Database password")
	// apiKey := flag.String("apikey", "", "API key")
	// mode := flag.String("mode", "", "Mode of operation: install, start, stop, restart, uninstall, debug,service")

	// flag.Parse()

	// // Validate required inputs
	// if *dbHost == "" || *dbUserName == "" || *dbPassword == "" {
	// 	fmt.Println("Missing required arguments. Ensure all values are provided (host, username, password,  apikey, mode).")
	// 	os.Exit(1)
	// }

	fileName := "/var/log/authnull-db-agent.log"
	logFile, err := openLogFile(fileName)
	if err != nil {
		log.Panic(err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	// Signal handling to allow graceful shutdown
	exit := make(chan struct{})
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %s, stopping the agent...", sig)
		close(exit)
	}()

	startAgent(exit)

}
