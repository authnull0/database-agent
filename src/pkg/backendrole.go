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
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/lib/pq"
)

// BackendRolesGroup holds every restricted role, so that pg_hba.conf can admit them all from the
// proxy with one line -- their names are not known in advance:
//
//	host <db> +authnull_backend_roles <proxy-ip>/32 scram-sha-256
//
// It cannot log in and holds no privileges; a member's privileges are only its own grants.
const BackendRolesGroup = "authnull_backend_roles"

// BackendRoleName is the restricted role for a policy's shape, and false when the policy gets
// none: EXECUTE among its privileges, an unknown privilege, or none at all.
func BackendRoleName(database, role string, privileges, tables []string) (string, bool) {
	privSet := map[string]bool{}
	for _, p := range privileges {
		p = strings.ToUpper(strings.TrimSpace(p))
		if p != "READ" && p != "WRITE" {
			return "", false
		}
		privSet[p] = true
	}
	if len(privSet) == 0 {
		return "", false
	}
	privs := make([]string, 0, len(privSet))
	for p := range privSet {
		privs = append(privs, p)
	}
	sort.Strings(privs)

	canonical := "v1|" + strings.ToLower(strings.TrimSpace(database)) + "|" + strings.TrimSpace(role) +
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

// randomPassword is 32 hex characters from crypto/rand -- also safe to place in the SQL string
// literals the ProxySQL admin interface needs, since it has no quote or backslash.
func randomPassword() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// ProvisionBackendRole makes the restricted role for a policy and registers it in ProxySQL.
//
// target must be connected to the policy's database (table grants are per database). proxy is the
// ProxySQL admin connection; nil skips registration (tests). The caller loads ProxySQL users to
// runtime afterwards. Returns the role name, or "" when the policy gets no restricted role.
//
// Order matters: the role and every grant are made in Postgres first, and only then is the role
// registered in ProxySQL -- a role ProxySQL can find is always a complete one.
func ProvisionBackendRole(target, proxy *sql.DB, database, sourceRole string, privileges, tables []string, hostgroupID int) (string, error) {
	name, ok := BackendRoleName(database, sourceRole, privileges, tables)
	if !ok {
		return "", nil
	}
	read, write := false, false
	for _, p := range privileges {
		switch strings.ToUpper(strings.TrimSpace(p)) {
		case "READ":
			read = true
		case "WRITE":
			write = true
		}
	}

	// One password, held by ProxySQL and set on the role: reuse ProxySQL's when it has one.
	password := ""
	if proxy != nil {
		var existing sql.NullString
		err := proxy.QueryRow(fmt.Sprintf("SELECT password FROM pgsql_users WHERE username = '%s'", name)).Scan(&existing)
		if err != nil && err != sql.ErrNoRows {
			return "", fmt.Errorf("read %s from ProxySQL: %w", name, err)
		}
		if existing.Valid && isHex(existing.String) {
			password = existing.String
		}
	}
	if password == "" {
		p, err := randomPassword()
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
		if _, err := target.Exec(fmt.Sprintf("CREATE ROLE %s LOGIN NOINHERIT PASSWORD '%s'", q, password)); err != nil {
			return "", fmt.Errorf("create role %s: %w", name, err)
		}
	} else if _, err := target.Exec(fmt.Sprintf("ALTER ROLE %s WITH LOGIN NOINHERIT PASSWORD '%s'", q, password)); err != nil {
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

	stmts := []string{
		fmt.Sprintf("GRANT %s TO %s", qg, q),
		fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", pq.QuoteIdentifier(database), q),
	}
	var tablePrivs []string
	if read {
		tablePrivs = append(tablePrivs, "SELECT")
	}
	if write {
		tablePrivs = append(tablePrivs, "INSERT", "UPDATE", "DELETE")
	}
	privList := strings.Join(tablePrivs, ", ")

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
		var count int
		if err := proxy.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM pgsql_users WHERE username = '%s'", name)).Scan(&count); err != nil {
			return "", fmt.Errorf("look up %s in ProxySQL: %w", name, err)
		}
		var q2 string
		if count == 0 {
			q2 = fmt.Sprintf("INSERT INTO pgsql_users (username, password, default_hostgroup, active, use_ssl) VALUES ('%s', '%s', %d, 1, 0)", name, password, hostgroupID)
		} else {
			q2 = fmt.Sprintf("UPDATE pgsql_users SET password = '%s', default_hostgroup = %d, active = 1 WHERE username = '%s'", password, hostgroupID, name)
		}
		if _, err := proxy.Exec(q2); err != nil {
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

func isHex(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// provisionPolicyBackendRole connects to the policy's database and provisions its restricted role,
// logging the outcome. Grants are per database, so the agent's main connection (to "postgres")
// cannot make them.
func provisionPolicyBackendRole(cfg DBConfig, proxy *sql.DB, dbName, dbUserName string, policy *GetPolicyDetailsResponse, hostgroupID int) {
	if policy == nil {
		return
	}
	d := policy.Data.Database
	if _, ok := BackendRoleName(dbName, dbUserName, d.Privilege, d.Tables); !ok {
		log.Printf("Backend role: policy for %s on %s grants %v; the session keeps the policy's own role", dbUserName, dbName, d.Privilege)
		return
	}
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, dbName)
	target, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Printf("Backend role: cannot open %s: %v; sessions fall back to %s", dbName, err, dbUserName)
		return
	}
	defer target.Close()
	name, err := ProvisionBackendRole(target, proxy, dbName, dbUserName, d.Privilege, d.Tables, hostgroupID)
	if err != nil {
		log.Printf("Backend role for %s on %s %v not provisioned: %v; sessions fall back to %s "+
			"(the agent's database account needs CREATEROLE and the right to grant on the tables)",
			dbUserName, dbName, d.Privilege, err, dbUserName)
		return
	}
	log.Printf("Backend role %s provisioned for %s on %s with %v on %d table(s)", name, dbUserName, dbName, d.Privilege, len(d.Tables))
}
