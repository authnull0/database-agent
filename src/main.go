package main

import (
	"database/sql"
	"fmt"
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
	startAgent(p.exit, p.dbUserName, p.dbPassword, p.dbHost)
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

	var config pkg.DBConfig
	err = viper.Unmarshal(&config)
	log.Printf("MAIN FUNCTION")
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

func startAgent(exit chan struct{}, dbUserName string, dbPassword string, dbHost string) {
	fmt.Println("Starting Authnull Database Agent...")

	// Load the configuration
	var err error
	var timeInterval int
	var conn *sql.DB
	config, err = loadConfig("./")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Configuration loaded: %v", config)

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

	for _, db := range dsCfg.Databases {
		log.Printf("Connecting to %s:%d", db.Host, db.Port)

		password, err := utils.DecryptPassword(db.Password, config.Key)
		if err != nil {
			log.Printf("Password decrypt failed for %s: %v", db.Host, err)
			continue
		}

		cfg := pkg.DBConfig{
			Host:     db.Host,
			User:     db.Username,
			Password: password,
			Port:     db.Port,
			DBType:   db.Type,
		}

		conn, err = pkg.ConnectToDB(cfg)
		if err != nil {
			log.Printf("DB connection failed %s: %v", db.Host, err)
			continue
		}

		log.Printf("Connected successfully to %s", db.Host)

	}
	// Initialize ProxySQL with one-time setup queries
	log.Default().Printf("Initializing ProxySQL...")
	if err := pkg.InitializeProxySQL(config); err != nil {
		log.Printf("Warning: ProxySQL initialization failed: %v", err)
		// Continue anyway - ProxySQL might not be available or already initialized
	}

	// Ticker to run the synchronization every minute
	ticker := time.NewTicker(time.Duration(timeInterval) * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Default().Println("DB Synchronization Started...")

			// Fall back to single-host mode
			err = pkg.FetchDatabaseDetails(conn, config)
			if err != nil {
				log.Printf("Failed to fetch database details: %v", err)
			}
		case <-exit:
			log.Println("Stopping agent...")
			return
		}
	}
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
	logFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
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

	startAgent(exit, config.Host, config.User, config.Password)

}
