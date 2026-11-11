package pkg

import (
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func TestEngineNormalisesTheConfiguredType(t *testing.T) {
	for in, want := range map[string]string{
		"": EnginePostgres, "postgres": EnginePostgres, "PostgreSQL": EnginePostgres, " pg ": EnginePostgres,
		"mysql": EngineMySQL, "MySQL": EngineMySQL, "mariadb": EngineMySQL,
		"oracle": "oracle",
	} {
		if got := Engine(in); got != want {
			t.Errorf("Engine(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTargetPortDefaultsPerEngine(t *testing.T) {
	cases := []struct {
		cfg  DBConfig
		want int
	}{
		{DBConfig{DBType: "mysql"}, 3306},
		{DBConfig{DBType: "postgres"}, 5432},
		{DBConfig{}, 5432},
		{DBConfig{DBType: "mysql", Port: "3307"}, 3307},
		{DBConfig{DBType: "postgres", Port: "nonsense"}, 5432},
	}
	for _, c := range cases {
		if got := targetPort(c.cfg); got != c.want {
			t.Errorf("targetPort(%+v) = %d, want %d", c.cfg, got, c.want)
		}
	}
}

// The DSN is built by the driver's own formatter, so a password with DSN syntax in it stays a password.
func TestMySQLDSNKeepsHostileValuesIntact(t *testing.T) {
	cfg := DBConfig{DBType: "mysql", Host: "10.0.0.5", User: "agent", Password: "p@ss:/w'd)(tcp"}
	c, err := mysql.ParseDSN(mysqlDSN(cfg, "shop"))
	if err != nil {
		t.Fatal(err)
	}
	if c.User != "agent" || c.Passwd != cfg.Password || c.Net != "tcp" || c.Addr != "10.0.0.5:3306" || c.DBName != "shop" {
		t.Fatalf("parsed back: user %q addr %q db %q net %q (password intact: %v)", c.User, c.Addr, c.DBName, c.Net, c.Passwd == cfg.Password)
	}
	if !c.ParseTime || c.Timeout <= 0 || c.Timeout > time.Minute {
		t.Fatalf("parseTime %v timeout %v", c.ParseTime, c.Timeout)
	}
	cfg.Port = "3307"
	if c, _ := mysql.ParseDSN(mysqlDSN(cfg, "")); c.Addr != "10.0.0.5:3307" || c.DBName != "" {
		t.Fatalf("port 3307: %q %q", c.Addr, c.DBName)
	}
}

func TestMySQLQuoting(t *testing.T) {
	if got := mysqlIdent("a`b"); got != "`a``b`" {
		t.Errorf("ident: %s", got)
	}
	// A quote is doubled and a backslash escaped: safe with or without NO_BACKSLASH_ESCAPES.
	got, err := mysqlString(`o'b\'); DROP USER x; --`)
	if err != nil || got != `'o''b\\''); DROP USER x; --'` {
		t.Errorf("string: %s %v", got, err)
	}
	if _, err := mysqlString("a\x00b"); err == nil {
		t.Error("want an error for a NUL byte")
	}
	acct, err := mysqlAccount("o'brien", "%")
	if err != nil || acct != `'o''brien'@'%'` {
		t.Errorf("account: %s %v", acct, err)
	}
	// In a database-level grant _ and % are wildcards: escaped, the grant names this database only.
	if got := mysqlDBGrantTarget("agent_it_db"); got != "`agent\\_it\\_db`.*" {
		t.Errorf("grant target: %s", got)
	}
	if got := mysqlDBGrantTarget("a%b`c"); got != "`a\\%b``c`.*" {
		t.Errorf("grant target: %s", got)
	}
}

// Against a real MySQL (DBAGENT_TEST_MYSQL_DSN): the agent's opener connects by engine.
func TestMySQLConnectByEngine(t *testing.T) {
	_, cfg, dbName := mysqlTestTarget(t)
	db, err := ConnectToDB(cfg)
	if err != nil {
		t.Fatalf("ConnectToDB: %v", err)
	}
	defer db.Close()
	var version string
	if err := db.QueryRow("SELECT VERSION()").Scan(&version); err != nil || !strings.HasPrefix(version, "8.") {
		t.Fatalf("version %q: %v", version, err)
	}
	one, err := openDatabase(cfg, dbName)
	if err != nil {
		t.Fatal(err)
	}
	defer one.Close()
	var current string
	if err := one.QueryRow("SELECT DATABASE()").Scan(&current); err != nil || current != dbName {
		t.Fatalf("DATABASE() = %q, %v", current, err)
	}
}
