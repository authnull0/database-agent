package pkg

type DBConfig struct {
	OrgID        string `mapstructure:"ORG_ID"`
	TenantID     string `mapstructure:"TENANT_ID"`
	DBType       string `mapstructure:"DB_TYPE"`
	Port         string `mapstructure:"DB_PORT"`
	TimeInterval string `mapstructure:"TIME_INTERVAL"`
	API          string `mapstructure:"API"`
	Host         string `mapstructure:"DB_HOST"`
	User         string `mapstructure:"DB_USER"`
	Password     string `mapstructure:"DB_PASSWORD"`
	Key          string `mapstructure:"KEY"`
	MachineKey   string `mapstructure:"MACHINE_KEY"`
}

type InstanceCreatedResponse struct {
	// instance_id
	InstanceId string
	// code
	Code string
	// message
	Message string
}

