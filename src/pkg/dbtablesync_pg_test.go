package pkg

import (
	"errors"
	"reflect"
	"testing"
)

// A table name is a value in the column query, not SQL: a quote in it neither breaks nor injects.
func TestPostgresTablesWithAQuoteInTheName(t *testing.T) {
	db, _ := agentTestDB(t)
	cleanup := func() { db.Exec(`DROP TABLE IF EXISTS "br_o'quote"`) }
	cleanup()
	t.Cleanup(cleanup)
	if _, err := db.Exec(`CREATE TABLE "br_o'quote" (a int, b text)`); err != nil {
		t.Fatal(err)
	}
	tables, err := pgTables(db)
	if err != nil {
		t.Fatal(err)
	}
	if got := tables["br_o'quote"]; !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("columns %v", got)
	}
	if got := tables["br_customers"]; !reflect.DeepEqual(got, []string{"id", "name"}) {
		t.Fatalf("br_customers columns %v", got)
	}
}

// Every schema but the system ones is discovered (B4): a table in public by its bare name, as
// before, any other as "schema.table".
func TestPostgresTablesOfEverySchema(t *testing.T) {
	db, _ := agentTestDB(t)
	cleanup := func() { db.Exec(`DROP SCHEMA IF EXISTS inv_sales, "inv_HR" CASCADE`) }
	cleanup()
	t.Cleanup(cleanup)
	for _, s := range []string{
		`CREATE SCHEMA inv_sales`,
		`CREATE SCHEMA "inv_HR"`,
		`CREATE TABLE inv_sales.orders (id int, total numeric, placed_at timestamptz)`,
		`CREATE TABLE inv_sales.br_customers (code text)`, // same name as public's: both kept
		`CREATE TABLE "inv_HR".staff (id int, salary int)`,
		`CREATE TABLE inv_sales.empty_one ()`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	tables, err := pgTables(db)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string][]string{
		"inv_sales.orders":       {"id", "total", "placed_at"},
		"inv_sales.br_customers": {"code"},
		"inv_HR.staff":           {"id", "salary"},
		"br_customers":           {"id", "name"},
	} {
		if got := tables[name]; !reflect.DeepEqual(got, want) {
			t.Errorf("%s: columns %v, want %v", name, got, want)
		}
	}
	if _, ok := tables["inv_sales.empty_one"]; !ok {
		t.Error("a table without columns is still a table")
	}
	for name := range tables {
		for _, sys := range []string{"pg_catalog.", "information_schema.", "public."} {
			if len(name) >= len(sys) && name[:len(sys)] == sys {
				t.Errorf("reported %s", name)
			}
		}
	}
}

// The health check's database is not synced, and a database the agent may not connect to is
// reported once, not every sync, and as errNoConnect (B7).
func TestHealthCheckDatabaseAndNoConnect(t *testing.T) {
	admin, base := agentTestDB(t)
	const hc, closed, agent = "inv_hc_monitor", "inv_noconnect", "inv_agent"
	cleanup := func() {
		admin.Exec(`DROP DATABASE IF EXISTS ` + hc)
		admin.Exec(`DROP DATABASE IF EXISTS ` + closed)
		admin.Exec(`DROP ROLE IF EXISTS ` + agent)
	}
	cleanup()
	t.Cleanup(cleanup)
	for _, s := range []string{
		`CREATE DATABASE ` + hc,
		`CREATE DATABASE ` + closed,
		`REVOKE CONNECT ON DATABASE ` + closed + ` FROM PUBLIC`,
		`CREATE ROLE ` + agent + ` LOGIN PASSWORD 'inv_agent_pw'`,
	} {
		if _, err := admin.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	cfg := DBConfig{DBType: "postgres", Host: base.Hostname(), Port: base.Port(), HealthCheckNames: " " + hc + " , other"}
	dbs, err := listDatabases(admin, cfg)
	if err != nil {
		t.Fatal(err)
	}
	sawClosed := false
	for _, d := range dbs {
		if d == hc {
			t.Errorf("the health check's database %s was listed", d)
		}
		sawClosed = sawClosed || d == closed
	}
	if !sawClosed {
		t.Errorf("%s (no CONNECT for PUBLIC) should still be listed: its logins are synced", closed)
	}

	// PUBLIC's CONNECT, for the control plane's risk warnings.
	if pc := publicConnect(admin, cfg, hc); pc == nil || !*pc {
		t.Errorf("a new database grants CONNECT to PUBLIC; got %v", pc)
	}
	if pc := publicConnect(admin, cfg, closed); pc == nil || *pc {
		t.Errorf("CONNECT revoked from PUBLIC; got %v", pc)
	}
	if pc := publicConnect(admin, DBConfig{DBType: "mysql"}, closed); pc != nil {
		t.Error("MySQL has no PUBLIC")
	}

	cfg.User, cfg.Password = agent, "inv_agent_pw"
	for i := 0; i < 2; i++ {
		_, err := openPGDatabase(cfg, closed)
		if !errors.Is(err, errNoConnect) {
			t.Fatalf("connecting without CONNECT: %v, want errNoConnect", err)
		}
	}
	if _, err := databaseTables(admin, cfg, closed); !errors.Is(err, errNoConnect) {
		t.Fatalf("databaseTables: %v", err)
	}
	if _, seen := noConnectLogged.Load(cfg.Host + "/" + closed); !seen {
		t.Error("not recorded as logged")
	}
}

func TestHealthCheckNames(t *testing.T) {
	if got := healthCheckNames(DBConfig{}); !reflect.DeepEqual(got, []string{"monitor"}) {
		t.Errorf("default %v", got)
	}
	if got := healthCheckNames(DBConfig{HealthCheckNames: "mon1, ,mon2"}); !reflect.DeepEqual(got, []string{"mon1", "mon2"}) {
		t.Errorf("configured %v", got)
	}
	pg, my := DBConfig{DBType: "postgres"}, DBConfig{DBType: "mysql"}
	for _, c := range []DBConfig{pg, my} {
		if !excludedAccount(c, "monitor") || !excludedAccount(c, "MONITOR") {
			t.Errorf("%s: monitor is reported", c.DBType)
		}
		if excludedAccount(c, "monitoring_app") {
			t.Errorf("%s: only the exact name is the health check's", c.DBType)
		}
	}
	users := filterExcludedAccounts(my, []databaseUser{{Username: "monitor", Host: "192.168.122.15"}, {Username: "app", Host: "%"}})
	if len(users) != 1 || users[0].Username != "app" {
		t.Errorf("monitor@host not filtered: %v", users)
	}
}
