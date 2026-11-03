package pkg

import (
	"bytes"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// The admin login comes from db.env, not the source: ProxySQL's admin password is the key to every
// role password it holds.
func TestProxyAdminDSN(t *testing.T) {
	// Defaults: what every install used until now.
	dsn := proxysqlAdminDSN(DBConfig{})
	c, err := mysql.ParseDSN(dsn)
	if err != nil || c.User != "admin,agent" || c.Passwd != "admin" || c.Addr != "127.0.0.1:6032" || c.Net != "tcp" {
		t.Fatalf("defaults: %q -> %+v %v", dsn, c, err)
	}
	// From db.env, with characters a DSN would otherwise misread.
	cfg := DBConfig{ProxysqlAdminUser: "authnull_admin", ProxysqlAdminPassword: "p@ss:w/rd?x=1", ProxysqlAdminAddr: "127.0.0.1:16032"}
	c, err = mysql.ParseDSN(proxysqlAdminDSN(cfg))
	if err != nil || c.User != "authnull_admin,agent" || c.Passwd != "p@ss:w/rd?x=1" || c.Addr != "127.0.0.1:16032" {
		t.Fatalf("from db.env: %+v %v", c, err)
	}
}

func TestDefaultAdminPasswordIsCalledOut(t *testing.T) {
	if !usingDefaultAdminPassword(DBConfig{}) || !usingDefaultAdminPassword(DBConfig{ProxysqlAdminPassword: "admin"}) {
		t.Error("the default admin password was not recognised")
	}
	if usingDefaultAdminPassword(DBConfig{ProxysqlAdminPassword: "Zq9-generated"}) {
		t.Error("a generated password was called the default")
	}
}

func TestAdminPasswordNeverPrinted(t *testing.T) {
	var buf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(prev) })
	cfg := DBConfig{ProxysqlAdminUser: "authnull_admin", ProxysqlAdminPassword: "very-secret-admin-pw"}
	log.Printf("%v %+v %#v %s", cfg, cfg, cfg, cfg)
	if strings.Contains(buf.String(), "very-secret-admin-pw") {
		t.Fatalf("the admin password is in the log: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "authnull_admin") {
		t.Error("the admin user is not a secret and should be visible")
	}
}

// Anything that can reach ProxySQL's admin interface can read every role password in it, then log
// in to the database directly. That is direct access, and the report says so.
func TestAdminExposureFindings(t *testing.T) {
	for name, c := range map[string]struct {
		ifaces     map[string]string
		defaultPw  bool
		wantIssues int
	}{
		"loopback only, own password":  {map[string]string{"admin-mysql_ifaces": "127.0.0.1:6032;/tmp/proxysql_admin.sock", "admin-pgsql_ifaces": "127.0.0.1:6132"}, false, 0},
		"ipv6 loopback":                {map[string]string{"admin-mysql_ifaces": "[::1]:6032"}, false, 0},
		"localhost name":               {map[string]string{"admin-mysql_ifaces": "localhost:6032"}, false, 0},
		"every address (the lab)":      {map[string]string{"admin-mysql_ifaces": "0.0.0.0:6032", "admin-pgsql_ifaces": "0.0.0.0:6132"}, false, 2},
		"a routable address":           {map[string]string{"admin-mysql_ifaces": "192.168.122.15:6032"}, false, 1},
		"default password, loopback":   {map[string]string{"admin-mysql_ifaces": "127.0.0.1:6032"}, true, 1},
		"default password and exposed": {map[string]string{"admin-mysql_ifaces": "0.0.0.0:6032"}, true, 2},
	} {
		got := adminExposure(c.ifaces, c.defaultPw)
		if len(got) != c.wantIssues {
			t.Errorf("%s: %d findings %v, want %d", name, len(got), got, c.wantIssues)
		}
	}
}

func TestExposureRaisesTheVerdict(t *testing.T) {
	at := time.Unix(1793800000, 0)
	for _, status := range []string{DirectAccessProtected, DirectAccessUnknown, DirectAccessExposed} {
		rep := DirectAccessReport{Status: status, Detail: "pg_hba.conf: fine", CheckedAt: at}
		got := withAdminExposure(rep, []string{"ProxySQL's admin interface listens on 0.0.0.0:6032"})
		if got.Status != DirectAccessExposed || !strings.Contains(got.Detail, "pg_hba.conf: fine") ||
			!strings.Contains(got.Detail, "0.0.0.0:6032") {
			t.Errorf("from %s: %+v", status, got)
		}
	}
	rep := DirectAccessReport{Status: DirectAccessProtected, Detail: "ok"}
	if got := withAdminExposure(rep, nil); got != rep {
		t.Errorf("no findings changed the report: %+v", got)
	}
}
