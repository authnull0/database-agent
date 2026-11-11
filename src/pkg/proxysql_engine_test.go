package pkg

import (
	"database/sql/driver"
	"reflect"
	"strings"
	"testing"
)

// A MySQL target is registered in mysql_servers, at its own port, and nothing is said to pgsql_*:
// a MySQL and a PostgreSQL target on one IP are two servers in two tables.
func TestMySQLServerGoesToMySQLServers(t *testing.T) {
	db, f := newFakeAdmin(t, countAnswer(0))
	cfg := DBConfig{DBType: "mysql", Host: "10.0.0.5"}
	if err := updateHost(db, cfg, "10.0.0.5", 4, targetPort(cfg)); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"SELECT COUNT(*) FROM mysql_servers WHERE hostname = '10.0.0.5' AND port = 3306",
		"INSERT INTO mysql_servers (hostgroup_id, hostname, port) VALUES (4, '10.0.0.5', 3306)",
		"LOAD MYSQL SERVERS TO RUNTIME",
		"SAVE MYSQL SERVERS TO DISK",
	}
	if got := f.statements(); !reflect.DeepEqual(got, want) {
		t.Fatalf("statements:\n got %q\nwant %q", got, want)
	}
}

// A server already there in another hostgroup is moved; a hostname is a quoted value.
func TestMySQLServerMovesHostgroupAndQuotesTheHost(t *testing.T) {
	db, f := newFakeAdmin(t, func(q string) ([]string, [][]driver.Value, error) {
		if strings.Contains(q, "hostgroup_id = 5") {
			return []string{"n"}, [][]driver.Value{{int64(0)}}, nil
		}
		return countAnswer(1)(q)
	})
	host := "db'1.example"
	if err := updateHost(db, DBConfig{DBType: "mysql", Host: host, Port: "3307"}, host, 5, 3307); err != nil {
		t.Fatal(err)
	}
	got := f.statements()
	if len(got) != 5 || got[2] != "UPDATE mysql_servers SET hostgroup_id = 5 WHERE hostname = 'db''1.example' AND port = 3307" {
		t.Fatalf("statements: %q", got)
	}
}

// PostgreSQL: the same statements as before, on pgsql_servers, with clear-text auth re-asserted.
func TestPostgresServerStatementsUnchanged(t *testing.T) {
	db, f := newFakeAdmin(t, countAnswer(0))
	if err := updateHost(db, DBConfig{DBType: "postgres", Host: "10.0.0.6"}, "10.0.0.6", 2, 5432); err != nil {
		t.Fatal(err)
	}
	got := strings.Join(f.statements(), "\n")
	for _, s := range []string{
		"pgsql-authentication_method",
		"SELECT COUNT(*) FROM pgsql_servers WHERE hostname = '10.0.0.6' AND port = 5432",
		"INSERT INTO pgsql_servers (hostgroup_id, hostname, port) VALUES (2, '10.0.0.6', 5432)",
		"LOAD PGSQL SERVERS TO RUNTIME",
	} {
		if !strings.Contains(got, s) {
			t.Errorf("missing %q in:\n%s", s, got)
		}
	}
	if strings.Contains(got, "mysql_") || strings.Contains(got, "MYSQL") {
		t.Errorf("PostgreSQL target touched MySQL tables:\n%s", got)
	}
}

// Initialization registers the target at its own port with its own engine (it used to be 5432,
// pgsql_servers, whatever the target).
func TestInitializeUsesTheTargetsEngineAndPort(t *testing.T) {
	db, f := newFakeAdmin(t, countAnswer(0))
	initializeProxySQL(db, DBConfig{DBType: "mysql", Host: "10.0.0.5"}, 0)
	got := strings.Join(f.statements(), "\n")
	if !strings.Contains(got, "INSERT INTO mysql_servers (hostgroup_id, hostname, port) VALUES (0, '10.0.0.5', 3306)") || strings.Contains(got, "PGSQL") || strings.Contains(got, "pgsql") {
		t.Fatalf("MySQL init:\n%s", got)
	}
	db, f = newFakeAdmin(t, countAnswer(0))
	initializeProxySQL(db, DBConfig{DBType: "postgres", Host: "10.0.0.6", Port: "5433"}, 0)
	got = strings.Join(f.statements(), "\n")
	if !strings.Contains(got, "SET pgsql-authentication_method = 1") || !strings.Contains(got, "VALUES (0, '10.0.0.6', 5433)") {
		t.Fatalf("PostgreSQL init:\n%s", got)
	}
}

// mysql_users for a MySQL target: the same row shape, MySQL's table and commands.
func TestMySQLProxyUsers(t *testing.T) {
	db, f := newFakeAdmin(t, nil)
	p := newAdminProxy(db, DBConfig{DBType: "mysql", Host: "10.0.0.5"})
	u := proxyUser{Username: "o'brien", Password: "p'w", DefaultHostgroup: 3, Comment: "c'm"}
	if err := p.Put(u); err != nil {
		t.Fatal(err)
	}
	if err := p.Delete("o'brien"); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"SELECT password, default_hostgroup, comment FROM mysql_users WHERE username = 'o''brien' LIMIT 1",
		"INSERT INTO mysql_users (username, password, default_hostgroup, active, use_ssl, comment) VALUES ('o''brien', 'p''w', 3, 1, 0, 'c''m')",
		"LOAD MYSQL USERS TO RUNTIME",
		"SAVE MYSQL USERS TO DISK",
		"DELETE FROM mysql_users WHERE username = 'o''brien'",
		"LOAD MYSQL USERS TO RUNTIME",
		"SAVE MYSQL USERS TO DISK",
	}
	if got := f.statements(); !reflect.DeepEqual(got, want) {
		t.Fatalf("statements:\n got %q\nwant %q", got, want)
	}
	if _, err := p.List(); err != nil {
		t.Fatal(err)
	}
	if got := f.statements(); got[len(got)-1] != "SELECT username, password, default_hostgroup, comment FROM mysql_users" {
		t.Fatalf("list: %q", got[len(got)-1])
	}
}

// The hostgroup of a MySQL server comes from mysql_servers, matched on its port too.
func TestHostgroupForIsPerEngine(t *testing.T) {
	db, f := newFakeAdmin(t, func(q string) ([]string, [][]driver.Value, error) {
		return []string{"hostgroup_id"}, [][]driver.Value{{int64(9)}}, nil
	})
	hg, ok, err := newAdminProxy(db, DBConfig{DBType: "mysql", Host: "10.0.0.5", Port: "3307"}).HostgroupFor("10.0.0.5")
	if err != nil || !ok || hg != 9 {
		t.Fatalf("%d %v %v", hg, ok, err)
	}
	_, _, _ = newAdminProxy(db, DBConfig{DBType: "postgres", Host: "10.0.0.5"}).HostgroupFor("10.0.0.5")
	want := []string{
		"SELECT hostgroup_id FROM mysql_servers WHERE hostname = '10.0.0.5' AND port = 3307 ORDER BY hostgroup_id LIMIT 1",
		"SELECT hostgroup_id FROM pgsql_servers WHERE hostname = '10.0.0.5' ORDER BY hostgroup_id LIMIT 1",
	}
	if got := f.statements(); !reflect.DeepEqual(got, want) {
		t.Fatalf("statements:\n got %q\nwant %q", got, want)
	}
}

