package main

import (
	//"database/sql"
	"fmt"
	"log"
	"os"
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
	exit chan struct{}
}

func (p *program) Start(s service.Service) error {
	p.exit = make(chan struct{})
	go p.run()
	return nil
}

func (p *program) run() {
	startAgent()
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
func startAgent() {
	fmt.Println("Starting Authnull Database Agent...")

	// Load the configuration
	var err error
	config, err = loadConfig("C:\\authnull-db-agent\\")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Configuration loaded: %v", config)

	// Connect to the database
	db, err := pkg.ConnectToDB(config)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}
	defer db.Close()

	//Synchronization loop every one minute
	for {
		log.Default().Println("Restarting the DB Synchronization...")
		// Fetch database details and their privileges
		err = pkg.FetchDb(db, config)
		if err != nil {
			log.Fatalf("Failed to fetch database details: %v", err)
		}
		time.Sleep(1 * time.Minute)
	}
}

func main() {
	fileName := "C:\\authnull-db-agent\\agent.log"
	logFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Panic(err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	svcConfig := &service.Config{
		Name:        "AuthnullDatabaseService",
		DisplayName: "Authnull Database Service",
		Description: "A service to synchronize database information.",
	}

	prg := &program{}
	svc, err := service.New(prg, svcConfig)
	if err != nil {
		log.Fatal(err)
	}

	if len(os.Args) > 1 {
		if os.Args[1] == "install" {
			err = svc.Install()
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println("Service installed successfully.")
			return
		} else if os.Args[1] == "debug" {
			startAgent()
			return
		}
		err = service.Control(svc, os.Args[1])
		if err != nil {
			log.Fatalf("Valid actions: %q\n", service.ControlAction)
			log.Fatal(err)
		}
		return
	}

	err = svc.Run()
	if err != nil {
		log.Fatal(err)
	}
}
