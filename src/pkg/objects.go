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
	Host         string `mapstructure:"DB_HOST"`       // Database host (legacy single-host mode)
	User         string `mapstructure:"DB_USER"`       // Database username
	Password     string `mapstructure:"DB_PASSWORD"`   // Database password
	Key          string `mapstructure:"KEY"`           // Authentication key
	MachineKey   string `mapstructure:"MACHINE_KEY"`   // Machine identifier
	AgentVMIP    string `mapstructure:"AGENT_VM_IP"`   // Agent VM IP address (for multi-host mode)
}

// InstanceCreatedResponse contains the response from the register agent API
type InstanceCreatedResponse struct {
	InstanceId string // Database instance ID
	Code       string // Response code
	Message    string // Response message
}

// DatabaseHost represents a database host configuration for multi-host support
type DatabaseHost struct {
	HostID      int    `json:"host_id"`      // Host ID from database
	AgentVMIP   string `json:"agent_vm_ip"`  // IP of the agent/proxysql VM
	HostVMIP    string `json:"host_vm_ip"`   // IP of the actual database host
	HostgroupID int    `json:"hostgroup_id"` // ProxySQL hostgroup ID
	Port        int    `json:"port"`         // Database port
	DBType      string `json:"db_type"`      // Database type (postgres, mysql, etc.)
	Status      string `json:"status"`       // Host status (active, inactive)
}

// DatabaseHostsResponse contains the response from get hosts API
type DatabaseHostsResponse struct {
	Code    string         `json:"code"`
	Status  string         `json:"status"`
	Message string         `json:"message"`
	Data    []DatabaseHost `json:"data"`
}
type HostConfig struct {
	Id       int    `json:"id"`
	HostVMIP string `json:"host_vm_ip"`
	Port     string `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
}

// HostsFile represents the structure of db_hosts.json
type HostsFile struct {
	Hosts []HostConfig `json:"hosts"`
}

type DataSourceConfig struct {
	Databases []Databases `mapstructure:"databases"`
}

type Databases struct {
	Host     string `mapstructure:"host"`
	Type     string `mapstructure:"type"`
	Port     string `mapstructure:"port"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"` // encrypted
}
type DbSyncResponse struct {
	HostGroupId int    `json:"hostgroup_id"` // Database HostGroup ID
	Code        int    `json:"code"`         // Response code
	Message     string `json:"message"`      // Response message
}
