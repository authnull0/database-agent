package pkg

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"sort"
	"strings"
	"sync"

	"github.com/go-sql-driver/mysql"
)

// ProxySQL's admin interface is the key to every managed role's password: pgsql_users holds them in
// clear, because ProxySQL logs in to the database with them. The agent used to reach it as
// admin:admin, written into its source; the login now comes from db.env:
//
//	PROXYSQL_ADMIN_USER      default "admin"
//	PROXYSQL_ADMIN_PASSWORD  default "admin" -- reported as exposure until it is changed
//	PROXYSQL_ADMIN_ADDR      default "127.0.0.1:6032"
//
// The fork's login code splits every username at its first comma, on the admin port too, so the
// agent logs in as "<user>,agent".

const (
	defaultProxyAdminUser     = "admin"
	defaultProxyAdminPassword = "admin"
	defaultProxyAdminAddr     = "127.0.0.1:6032"
)

func proxysqlAdminDSN(config DBConfig) string {
	user, pass, addr := config.ProxysqlAdminUser, config.ProxysqlAdminPassword, config.ProxysqlAdminAddr
	if strings.TrimSpace(user) == "" {
		user = defaultProxyAdminUser
	}
	if pass == "" {
		pass = defaultProxyAdminPassword
	}
	if strings.TrimSpace(addr) == "" {
		addr = defaultProxyAdminAddr
	}
	c := mysql.NewConfig()
	c.User = strings.TrimSpace(user) + ",agent"
	c.Passwd = pass
	c.Net = "tcp"
	c.Addr = strings.TrimSpace(addr)
	return c.FormatDSN()
}

func usingDefaultAdminPassword(config DBConfig) bool {
	return config.ProxysqlAdminPassword == "" || config.ProxysqlAdminPassword == defaultProxyAdminPassword
}

var defaultAdminWarn sync.Once

// readAdminIfaces is where the admin interfaces listen, from ProxySQL itself.
func readAdminIfaces(db *sql.DB) map[string]string {
	out := map[string]string{}
	rows, err := db.Query("SELECT variable_name, variable_value FROM global_variables " +
		"WHERE variable_name IN ('admin-mysql_ifaces', 'admin-pgsql_ifaces')")
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var k, v string
		if rows.Scan(&k, &v) == nil {
			out[k] = v
		}
	}
	return out
}

func loopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// adminExposure lists what makes ProxySQL's admin interface reachable or guessable: an interface
// listening beyond loopback (a unix socket is local), and the default password.
func adminExposure(ifaces map[string]string, defaultPassword bool) []string {
	var out []string
	names := make([]string, 0, len(ifaces))
	for k := range ifaces {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, name := range names {
		for _, entry := range strings.Split(ifaces[name], ";") {
			entry = strings.TrimSpace(entry)
			if entry == "" || strings.HasPrefix(entry, "/") {
				continue
			}
			host, _, err := net.SplitHostPort(entry)
			if err != nil {
				host = entry
			}
			if !loopback(host) {
				out = append(out, fmt.Sprintf("ProxySQL's admin interface (%s) listens on %s: anything that reaches it can "+
					"read every managed role's password. Listen on 127.0.0.1 only.", name, entry))
				break
			}
		}
	}
	if defaultPassword {
		out = append(out, "ProxySQL's admin interface still has the default password. Set a generated one, and "+
			"PROXYSQL_ADMIN_PASSWORD in db.env.")
	}
	return out
}

// withAdminExposure raises the direct-access verdict to exposed and says why: whoever can read
// ProxySQL's admin interface has every managed role's password, and can log in directly.
func withAdminExposure(rep DirectAccessReport, findings []string) DirectAccessReport {
	if len(findings) == 0 {
		return rep
	}
	rep.Status = DirectAccessExposed
	rep.Detail = strings.TrimSpace(rep.Detail + "\n\n" + strings.Join(findings, "\n"))
	return rep
}

// warnDefaultAdmin says once, at the first connection, that the admin password is the default.
func warnDefaultAdmin(config DBConfig) {
	if usingDefaultAdminPassword(config) {
		defaultAdminWarn.Do(func() {
			log.Printf("WARNING: connecting to ProxySQL's admin interface with the default password. Set a generated " +
				"admin password in ProxySQL and PROXYSQL_ADMIN_PASSWORD in db.env")
		})
	}
}
