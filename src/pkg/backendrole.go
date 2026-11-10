package pkg

// Restricted backend roles: the database itself enforces a policy's privileges.
//
// A database policy grants a session READ, WRITE and/or EXECUTE on some tables. ProxySQL checks
// each query against those, but that check reads SQL text and cannot see what a function does
// inside. So for a policy whose privileges are READ, WRITE or both, the agent creates a login role
// holding exactly those privileges on exactly those tables, and ProxySQL connects to Postgres as
// that role for the session: whatever the user sends, Postgres refuses what was not granted.
//
// The role's name is derived from the policy's shape -- database, the policy's role, privileges,
// tables -- so the agent and ProxySQL arrive at the same name independently (BackendRoleName here,
// authnullBackendRoleName in proxysql-v3-alpha include/authnull_backend_role.h; shared test vectors
// in both). A policy with EXECUTE keeps its own role: DDL needs what the role already has.
//
// The agent's database account must be able to grant on the tables (own them, hold WITH GRANT
// OPTION, or be a superuser) and needs CREATEROLE. When a grant fails the role is not registered
// in ProxySQL, and ProxySQL falls back to the policy's own role (backend_roles = prefer) or refuses
// the session (backend_roles = enforce).

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/lib/pq"
)

// BackendRolesGroup holds every restricted role, so that pg_hba.conf can admit them all from the
// proxy with one line -- their names are not known in advance:
//
//	host <db> +authnull_backend_roles <proxy-ip>/32 scram-sha-256
//
// It cannot log in and holds no privileges; a member's privileges are only its own grants.
const BackendRolesGroup = "authnull_backend_roles"

// tableVerbs are the table privileges a restricted role can hold, in grant order.
var tableVerbs = []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"}

// verbSet reads a policy's privileges as table verbs: READ is SELECT, WRITE is INSERT, UPDATE and
// DELETE, a verb is itself (custom privileges, AUTH-227). ok is false for EXECUTE or an unknown
// privilege: no restricted role can hold those.
func verbSet(privileges []string) (map[string]bool, bool) {
	out := map[string]bool{}
	for _, p := range privileges {
		switch u := strings.ToUpper(strings.TrimSpace(p)); u {
		case "READ":
			out["SELECT"] = true
		case "WRITE":
			out["INSERT"], out["UPDATE"], out["DELETE"] = true, true, true
		case "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE":
			out[u] = true
		default:
			return nil, false
		}
	}
	return out, len(out) > 0
}

// backendRoleVerbs are the table privileges a restricted role gets for these policy privileges.
func backendRoleVerbs(privileges []string) []string {
	set, _ := verbSet(privileges)
	var out []string
	for _, v := range tableVerbs {
		if set[v] {
			out = append(out, v)
		}
	}
	return out
}

// BackendRoleName is the restricted role for a policy's shape, and false when the policy gets
// none: EXECUTE among its privileges, an unknown privilege, or none at all. A verb set that is
// exactly whole groups (SELECT; INSERT, UPDATE, DELETE) is named by the groups, as before verbs
// existed ("v1"); any other set is "v2" over the sorted verbs.
func BackendRoleName(database, role string, privileges, tables []string) (string, bool) {
	verbs, ok := verbSet(privileges)
	if !ok {
		return "", false
	}
	read := verbs["SELECT"]
	write := verbs["INSERT"] && verbs["UPDATE"] && verbs["DELETE"]
	grouped := 0
	if read {
		grouped++
	}
	if write {
		grouped += 3
	}
	version := "v2"
	var privs []string
	if grouped == len(verbs) {
		version = "v1"
		if read {
			privs = append(privs, "READ")
		}
		if write {
			privs = append(privs, "WRITE")
		}
	} else {
		for v := range verbs {
			privs = append(privs, v)
		}
		sort.Strings(privs)
	}

	canonical := version + "|" + strings.ToLower(strings.TrimSpace(database)) + "|" + strings.TrimSpace(role) +
		"|" + strings.Join(privs, ",") + "|" + strings.Join(canonicalTables(tables), ",")
	sum := sha256.Sum256([]byte(canonical))
	return "an_" + hex.EncodeToString(sum[:])[:16], true
}

// canonicalTables: lower case, deduplicated, sorted; a table in schema public is its bare name.
func canonicalTables(tables []string) []string {
	set := map[string]bool{}
	for _, t := range tables {
		if strings.TrimSpace(t) == "" {
			continue
		}
		schema, name := splitTable(t)
		if schema == "public" {
			set[name] = true
		} else {
			set[schema+"."+name] = true
		}
	}
	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// splitTable splits "schema.table" (schema public when absent), lower case.
func splitTable(t string) (schema, table string) {
	t = strings.TrimSpace(t)
	if i := strings.Index(t, "."); i >= 0 {
		return strings.ToLower(strings.TrimSpace(t[:i])), strings.ToLower(strings.TrimSpace(t[i+1:]))
	}
	return "public", strings.ToLower(t)
}

// ProvisionBackendRole makes the restricted role for a policy and registers it in ProxySQL.
//
// target must be connected to the policy's database (table grants are per database). proxy is
// ProxySQL's pgsql_users; nil skips registration (tests). host is the database server, recorded
// with the password so the scheduled rotation (RotateDuePasswords) picks the role up like any
// other managed role. Returns the role name, or "" when the policy gets no restricted role.
//
// Order matters: the role and every grant are made in Postgres first, and only then is the role
// registered in ProxySQL -- a role ProxySQL can find is always a complete one.
func ProvisionBackendRole(target *sql.DB, proxy proxyUsers, host, database, sourceRole string, privileges, tables []string,
	hostgroupID int, now time.Time, interval time.Duration) (string, error) {
	name, ok := BackendRoleName(database, sourceRole, privileges, tables)
	if !ok {
		return "", nil
	}
	verbs := backendRoleVerbs(privileges)
	// Writing rows into a table with a serial column draws from its sequence.
	write := false
	for _, v := range verbs {
		if v == "INSERT" || v == "UPDATE" {
			write = true
		}
	}

	// One password, held by ProxySQL and set on the role: keep ProxySQL's until it is due for
	// rotation, so a checkout doesn't reset the clock.
	password, comment := "", rotationComment(host, now)
	if proxy != nil {
		u, found, err := proxy.Get(name)
		if err != nil {
			return "", fmt.Errorf("read %s from ProxySQL: %w", name, err)
		}
		if found && !rotationDue(u, now, interval) {
			password, comment = u.Password, u.Comment
		}
	}
	if password == "" {
		p, err := GenerateRandomPassword(rolePasswordLength)
		if err != nil {
			return "", err
		}
		password = p
	}

	q := pq.QuoteIdentifier(name)
	var exists bool
	if err := target.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", name).Scan(&exists); err != nil {
		return "", fmt.Errorf("look up role %s: %w", name, err)
	}
	if !exists {
		if _, err := target.Exec(fmt.Sprintf("CREATE ROLE %s LOGIN NOINHERIT PASSWORD %s", q, pq.QuoteLiteral(password))); err != nil {
			return "", fmt.Errorf("create role %s: %w", name, err)
		}
	} else if _, err := target.Exec(fmt.Sprintf("ALTER ROLE %s WITH LOGIN NOINHERIT PASSWORD %s", q, pq.QuoteLiteral(password))); err != nil {
		// Always re-set: ProxySQL's copy and the role's must not drift apart.
		return "", fmt.Errorf("set password of %s: %w", name, err)
	}

	qg := pq.QuoteIdentifier(BackendRolesGroup)
	var groupExists bool
	if err := target.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", BackendRolesGroup).Scan(&groupExists); err != nil {
		return "", fmt.Errorf("look up role %s: %w", BackendRolesGroup, err)
	}
	if !groupExists {
		if _, err := target.Exec("CREATE ROLE " + qg + " NOLOGIN"); err != nil {
			return "", fmt.Errorf("create role %s: %w", BackendRolesGroup, err)
		}
	}

	// Also a managed role: pg_hba.conf admits the managed group from the proxy only (directaccess.go).
	if err := ensureManagedGroup(target, name); err != nil {
		return "", err
	}
	stmts := []string{
		fmt.Sprintf("GRANT %s TO %s", qg, q),
		fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", pq.QuoteIdentifier(database), q),
	}
	privList := strings.Join(verbs, ", ")

	if len(canonicalTables(tables)) == 0 {
		// No tables named: the policy covers the database, so its public schema.
		stmts = append(stmts,
			fmt.Sprintf("GRANT USAGE ON SCHEMA public TO %s", q),
			fmt.Sprintf("GRANT %s ON ALL TABLES IN SCHEMA public TO %s", privList, q))
		if write {
			stmts = append(stmts, fmt.Sprintf("GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO %s", q))
		}
	} else {
		schemas := map[string]bool{}
		for _, t := range tables {
			if strings.TrimSpace(t) == "" {
				continue
			}
			schema, table := splitTable(t)
			if !schemas[schema] {
				schemas[schema] = true
				stmts = append(stmts, fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s", pq.QuoteIdentifier(schema), q))
			}
			qt := pq.QuoteIdentifier(schema) + "." + pq.QuoteIdentifier(table)
			stmts = append(stmts, fmt.Sprintf("GRANT %s ON TABLE %s TO %s", privList, qt, q))
			if write {
				seqs, err := ownedSequences(target, schema, table)
				if err != nil {
					return "", err
				}
				for _, s := range seqs {
					stmts = append(stmts, fmt.Sprintf("GRANT USAGE ON SEQUENCE %s TO %s", s, q))
				}
			}
		}
	}
	for _, s := range stmts {
		if _, err := target.Exec(s); err != nil {
			return "", fmt.Errorf("%s: %w", s, err)
		}
	}

	if proxy != nil {
		u := proxyUser{Username: name, Password: password, DefaultHostgroup: hostgroupID, Comment: comment}
		if err := proxy.Put(u); err != nil {
			return "", fmt.Errorf("register %s in ProxySQL: %w", name, err)
		}
	}
	return name, nil
}

// ownedSequences: the sequences behind a table's serial and identity columns, quoted.
func ownedSequences(db *sql.DB, schema, table string) ([]string, error) {
	rows, err := db.Query(`
		SELECT quote_ident(sn.nspname) || '.' || quote_ident(s.relname)
		FROM pg_class t
		JOIN pg_namespace tn ON tn.oid = t.relnamespace
		JOIN pg_depend d ON d.refobjid = t.oid AND d.deptype IN ('a', 'i')
		JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S'
		JOIN pg_namespace sn ON sn.oid = s.relnamespace
		WHERE tn.nspname = $1 AND t.relname = $2`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("sequences of %s.%s: %w", schema, table, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
