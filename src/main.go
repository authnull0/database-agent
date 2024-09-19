package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/kardianos/service"
	_ "github.com/lib/pq"
	_ "github.com/sijms/go-ora"
	"github.com/spf13/viper"

	"github.com/authnull0/database-agent/src/pkg"
)

var config pkg.DBConfig

type program struct {
	exit       chan struct{}
	dbUserName string
	dbPassword string
	dbHost     string
	apiKey     string
}

func (p *program) Start(s service.Service) error {
	p.exit = make(chan struct{})
	go p.Run()
	return nil
}

func (p *program) Run() {
	startAgent(p.exit, config.Port, p.dbUserName, p.dbPassword, p.dbHost, p.apiKey)
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

func startAgent(exit chan struct{}, Port, dbUserName, dbPassword, dbHost, apiKey string) {
	fmt.Println("Starting Authnull Database Agent...")

	// Load the configuration
	var err error
	var timeInterval int
	config, err = loadConfig("C:\\authnull-db-agent\\")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Configuration loaded: %v", config)

	timeInterval, err = strconv.Atoi(config.TimeInterval)
	if err != nil {
		log.Default().Println(err)
	}
	// Connect to the database
	db, err := pkg.ConnectToDB(config, dbUserName, dbPassword, dbHost)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}
	defer db.Close()

	// Ticker to run the synchronization every minute
	ticker := time.NewTicker(time.Duration(timeInterval) * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Default().Println("DB Synchronization Started...")
			// Fetch database details and their privileges
			err = pkg.FetchDatabaseDetails(db, config, dbHost, apiKey)
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
	//dbPort := flag.String("port", "", "Database port")
	dbHost := flag.String("host", "", "Database host")
	dbUserName := flag.String("username", "", "Database username")
	dbPassword := flag.String("password", "", "Database password")
	apiKey := flag.String("apikey", "", "API key")
	mode := flag.String("mode", "", "Mode of operation: install, start, stop, restart, uninstall, debug,service")

	flag.Parse()

	// Validate required inputs
	if *dbHost == "" || *dbUserName == "" || *dbPassword == "" || *apiKey == "" || *mode == "" {
		fmt.Println("Missing required arguments. Ensure all values are provided (host, username, password,  apikey, mode).")
		os.Exit(1)
	}

	fileName := "C:\\authnull-db-agent\\agent.log"
	logFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Panic(err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	// Service configuration
	svcConfig := &service.Config{
		Name:        "AuthnullDatabaseService",
		DisplayName: "Authnull Database Service",
		Description: "A service to synchronize database information.",
	}

	prg := &program{

		dbUserName: *dbUserName,
		dbPassword: *dbPassword,
		dbHost:     *dbHost,
		apiKey:     *apiKey,
	}

	svc, err := service.New(prg, svcConfig)
	if err != nil {
		log.Fatal(err)
	}

	// Handle modes (install, start, stop, restart, uninstall, debug)
	switch *mode {
	case "install":
		err = svc.Install()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Service installed successfully.")
		return
	case "start":
		err = svc.Start()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Service started.")
		return
	case "stop":
		err = svc.Stop()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Service stopped.")
		return
	case "restart":
		err = svc.Restart()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Service restarted.")
		return
	case "uninstall":
		err = svc.Uninstall()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Service uninstalled.")
		return
	case "debug":
		// Debug mode, run the agent without installing the service
		exit := make(chan struct{})
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		go func() {
			sig := <-sigChan
			log.Printf("Received signal: %s, stopping the agent...", sig)
			close(exit)
		}()

		startAgent(exit, config.Port, *dbUserName, *dbPassword, *dbHost, *apiKey)
		return

	case "service":
		err = svc.Run()
		if err != nil {
			log.Fatal(err)
		}

	default:
		fmt.Println("Invalid mode. Usage: install | start | stop | restart | uninstall | debug")
		os.Exit(1)
	}

}
