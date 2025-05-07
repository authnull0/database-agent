package pkg

// DBConfig contains the configuration for connecting to databases
// For PostgreSQL connections, DBType should be set to "postgres"
type DBConfig struct {
	OrgID        string `mapstructure:"ORG_ID"`        // Organization ID
	TenantID     string `mapstructure:"TENANT_ID"`     // Tenant ID
	DBType       string `mapstructure:"DB_TYPE"`       // Database type ("postgres" for PostgreSQL)
	Port         string `mapstructure:"DB_PORT"`       // Database port (typically "5432" for PostgreSQL)
	TimeInterval string `mapstructure:"TIME_INTERVAL"` // Polling interval
	API          string `mapstructure:"API"`           // API endpoint
	Host         string `mapstructure:"DB_HOST"`       // Database host
	User         string `mapstructure:"DB_USER"`       // Database username
	Password     string `mapstructure:"DB_PASSWORD"`   // Database password
	Key          string `mapstructure:"KEY"`           // Authentication key
	MachineKey   string `mapstructure:"MACHINE_KEY"`   // Machine identifier
}

// InstanceCreatedResponse contains the response from the register agent API
type InstanceCreatedResponse struct {
	InstanceId string // Database instance ID
	Code       string // Response code
	Message    string // Response message
}
