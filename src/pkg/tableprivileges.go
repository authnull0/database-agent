package pkg

import (
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// tablePrivilege is one table a login holds privileges on, as the dbUser payload's tablePrivileges
// lists it: "<schema>.<table>" for PostgreSQL, "<database>.<table>" for MySQL.
type tablePrivilege struct {
	Table      string   `json:"table"`
	Privileges []string `json:"privileges"`
}

// maxTablePrivileges caps a login's tablePrivileges: a database with thousands of tables would
// otherwise make every minute's payload that large. The privilege string still has the union.
const maxTablePrivileges = 500

// tablePrivilegesCapped remembers the logins whose list was cut, so each is logged once.
var tablePrivilegesCapped sync.Map

// normalizeTablePrivileges is in as the payload sends it: privileges upper-case, once each and
// sorted; tables sorted, those with no privilege dropped, the first maxTablePrivileges kept.
func normalizeTablePrivileges(in []tablePrivilege) []tablePrivilege {
	out := []tablePrivilege{}
	for _, tp := range in {
		privs := sortedUpper(tp.Privileges)
		if len(privs) == 0 {
			continue
		}
		out = append(out, tablePrivilege{Table: tp.Table, Privileges: privs})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Table < out[j].Table })
	if len(out) > maxTablePrivileges {
		out = out[:maxTablePrivileges]
	}
	return out
}

// capTablePrivilegesFor normalises a login's list and logs, once per login and database, that it
// was cut.
func capTablePrivilegesFor(user, dbName string, in []tablePrivilege) []tablePrivilege {
	out := normalizeTablePrivileges(in)
	if len(in) > maxTablePrivileges && len(out) == maxTablePrivileges {
		if _, seen := tablePrivilegesCapped.LoadOrStore(user+"\x00"+dbName, true); !seen {
			log.Printf("User %s holds privileges on more than %d tables of %s: sending the first %d",
				user, maxTablePrivileges, dbName, maxTablePrivileges)
		}
	}
	return out
}

// privilegeSummary is the payload's privilege string: the database-level privileges held and every
// privilege held on a table, once each, sorted, ", "-separated.
func privilegeSummary(database []string, tables []tablePrivilege) string {
	all := append([]string{}, database...)
	for _, tp := range tables {
		all = append(all, tp.Privileges...)
	}
	return strings.Join(sortedUpper(all), ", ")
}

// sortedUpper is privs upper-cased and trimmed, without blanks or duplicates, sorted.
func sortedUpper(privs []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, p := range privs {
		p = strings.ToUpper(strings.TrimSpace(p))
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// dbUserPayload is what the agent posts to /api/v1/databaseService/dbUser for one login on dbName.
// privilege is the database-level privileges plus the union of the table-level ones ("ALL
// PRIVILEGES" for a superuser, or a MySQL account holding everything); tablePrivileges is the
// per-table detail, an empty array when there is none to send (the console shows ALL for a
// superuser). For MySQL, global and database-wide (db.*) grants cover every table: they are in
// privilege only, and tablePrivileges lists only grants made on a single table.
func dbUserPayload(config DBConfig, dbName, instanceId, agentIP string, u databaseUser) map[string]interface{} {
	orgID, _ := strconv.Atoi(config.OrgID)
	tenantID, _ := strconv.Atoi(config.TenantID)
	tables := u.TablePrivileges
	if tables == nil {
		tables = []tablePrivilege{}
	}
	return map[string]interface{}{
		"orgId":        orgID,
		"tenantId":     tenantID,
		"databaseType": config.DBType,
		"databaseName": dbName,
		"userName":     u.Username,
		"host":         u.Host,
		"role":         u.Role,
		"privilege":    u.Privileges,
		// Per table, for the console's User Privileges page.
		"tablePrivileges": tables,
		// For policies' privilege condition: admin, elevated or standard (privilegelevel.go).
		"privilegeLevel": u.Level,
		"instanceId":     instanceId,
		"agentVmIp":      agentIP,     // Multi-host: Agent VM IP (where ProxySQL runs)
		"hostVmIp":       config.Host, // Multi-host: Database host VM IP
	}
}
