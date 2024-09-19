package pkg

type DBConfig struct {
	OrgID        string `mapstructure:"ORG_ID"`
	TenantID     string `mapstructure:"TENANT_ID"`
	DBType       string `mapstructure:"DB_TYPE"`
	Port         string `mapstructure:"DB_PORT"`
	TimeInterval string `mapstructure:"TIME_INTERVAL"`
	API          string `mapstructure:"API"`
}
