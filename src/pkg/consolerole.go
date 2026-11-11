package pkg

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/lib/pq"
)

// Roles created from the AuthNull console (authnull-service docs/DB-ROLES.md). The backend sends
// apply_role and drop_role jobs; apply makes the role -- if it is missing -- and its grants exactly
// the payload's, drop removes it. The agent touches only roles it created: each carries
// consoleRoleMark as its comment, and a role without it is refused, whatever the job says.

const consoleRoleMark = "authnull:console"

func isConsoleRole(comment string) bool { return comment == consoleRoleMark }

type rolePayload struct {
	Version         int        `json:"version"`
	Create          bool       `json:"create"`
	Grants          roleGrants `json:"grants"`
	ValidUntil      string     `json:"validUntil,omitempty"`
	RevokeDatabases []string   `json:"revokeDatabases,omitempty"`
}

type roleGrants struct {
	Databases []roleDBGrants `json:"databases"`
}

type roleDBGrants struct {
	Database string            `json:"database"`
	Objects  []roleObjectGrant `json:"objects"`
}

type roleObjectGrant struct {
	Type       string   `json:"type"`
	Schema     string   `json:"schema"`
	Name       string   `json:"name,omitempty"`
	Privileges []string `json:"privileges"`
	Future     bool     `json:"future,omitempty"`
}

type dropPayload struct {
	Databases []string `json:"databases"`
}

// defaultACL is one pg_default_acl entry naming the role: privileges it gets on objects owner
// creates in schema. ObjType is pg_default_acl's: r tables, S sequences, f functions.
type defaultACL struct {
	Owner, Schema, ObjType string
}

var (
	roleIdent      = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]{0,62}$`)
	roleObjectPriv = map[string]map[string]bool{
		"schema":        {"USAGE": true, "CREATE": true},
		"table":         {"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true, "TRUNCATE": true, "REFERENCES": true, "TRIGGER": true},
		"all_tables":    {"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true, "TRUNCATE": true, "REFERENCES": true, "TRIGGER": true},
		"sequence":      {"USAGE": true, "SELECT": true, "UPDATE": true},
		"all_sequences": {"USAGE": true, "SELECT": true, "UPDATE": true},
		"function":      {"EXECUTE": true},
		"all_functions": {"EXECUTE": true},
	}
	defaultObjKeyword = map[string]string{"all_tables": "TABLES", "all_sequences": "SEQUENCES", "all_functions": "FUNCTIONS",
		"r": "TABLES", "S": "SEQUENCES", "f": "FUNCTIONS"}
)

func grantableDatabase(name string) bool {
	return roleIdent.MatchString(name) && !strings.HasPrefix(strings.ToLower(name), "template")
}

// checkRolePayload is the agent's own check of what it is asked to grant: the backend checks too,
// but only the privileges and object kinds below can ever reach a GRANT.
func checkRolePayload(p rolePayload) error {
	if len(p.Grants.Databases) == 0 {
		return errors.New("the job grants nothing")
	}
	for _, d := range p.Grants.Databases {
		if !grantableDatabase(d.Database) {
			return fmt.Errorf("database %q cannot be granted on", d.Database)
		}
		for _, o := range d.Objects {
			allowed, ok := roleObjectPriv[o.Type]
			if !ok {
				return fmt.Errorf("unknown object type %q", o.Type)
			}
			if !roleIdent.MatchString(o.Schema) || strings.HasPrefix(strings.ToLower(o.Schema), "pg_") || strings.EqualFold(o.Schema, "information_schema") {
				return fmt.Errorf("schema %q cannot be granted on", o.Schema)
			}
			named := o.Type == "table" || o.Type == "sequence" || o.Type == "function"
			if named != (o.Name != "") || (named && !roleIdent.MatchString(o.Name)) {
				return fmt.Errorf("%s %q: bad object name", o.Type, o.Name)
			}
			if len(o.Privileges) == 0 {
				return fmt.Errorf("%s grant with no privileges", o.Type)
			}
			for _, pr := range o.Privileges {
				if !allowed[pr] {
					return fmt.Errorf("%q is not a %s privilege", pr, o.Type)
				}
			}
		}
	}
	for _, d := range p.RevokeDatabases {
		if !grantableDatabase(d) {
			return fmt.Errorf("database %q cannot be revoked on", d)
		}
	}
	if p.ValidUntil != "" {
		if _, err := time.Parse(time.RFC3339, p.ValidUntil); err != nil {
			return fmt.Errorf("validUntil %q: %v", p.ValidUntil, err)
		}
	}
	return nil
}

// grantStatements are one database's GRANTs for role. owners holds, per schema, the roles whose
// future objects the role gets privileges on (ALTER DEFAULT PRIVILEGES FOR ROLE).
func grantStatements(role string, d roleDBGrants, owners map[string][]string) []string {
	q := pq.QuoteIdentifier(role)
	out := []string{fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", pq.QuoteIdentifier(d.Database), q)}
	for _, o := range d.Objects {
		privs := strings.Join(o.Privileges, ", ")
		s := pq.QuoteIdentifier(o.Schema)
		switch o.Type {
		case "schema":
			out = append(out, fmt.Sprintf("GRANT %s ON SCHEMA %s TO %s", privs, s, q))
		case "table", "sequence", "function":
			out = append(out, fmt.Sprintf("GRANT %s ON %s %s.%s TO %s", privs, strings.ToUpper(o.Type), s, pq.QuoteIdentifier(o.Name), q))
		default: // all_*
			kw := defaultObjKeyword[o.Type]
			out = append(out, fmt.Sprintf("GRANT %s ON ALL %s IN SCHEMA %s TO %s", privs, kw, s, q))
			if o.Future {
				for _, owner := range owners[o.Schema] {
					out = append(out, fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA %s GRANT %s ON %s TO %s",
						pq.QuoteIdentifier(owner), s, privs, kw, q))
				}
			}
		}
	}
	return out
}

// revokeStatements take back everything role holds in a database: on every schema's objects, the
// schemas themselves, its default privileges, and the database.
func revokeStatements(role, database string, schemas []string, acls []defaultACL) []string {
	q := pq.QuoteIdentifier(role)
	var out []string
	for _, sch := range schemas {
		s := pq.QuoteIdentifier(sch)
		out = append(out,
			fmt.Sprintf("REVOKE ALL ON ALL TABLES IN SCHEMA %s FROM %s", s, q),
			fmt.Sprintf("REVOKE ALL ON ALL SEQUENCES IN SCHEMA %s FROM %s", s, q),
			fmt.Sprintf("REVOKE ALL ON ALL FUNCTIONS IN SCHEMA %s FROM %s", s, q),
			fmt.Sprintf("REVOKE ALL ON SCHEMA %s FROM %s", s, q))
	}
	for _, a := range acls {
		kw, ok := defaultObjKeyword[a.ObjType]
		if !ok {
			continue
		}
		out = append(out, fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA %s REVOKE ALL ON %s FROM %s",
			pq.QuoteIdentifier(a.Owner), pq.QuoteIdentifier(a.Schema), kw, q))
	}
	return append(out, fmt.Sprintf("REVOKE ALL ON DATABASE %s FROM %s", pq.QuoteIdentifier(database), q))
}

// --- against the server ---

// dbOpener connects to one database on the agent's server, as the agent.
type dbOpener func(database string) (*sql.DB, error)

// consoleRoleState: does role exist, and is it one the console created?
func consoleRoleState(server *sql.DB, role string) (exists, ours bool, err error) {
	var comment sql.NullString
	err = server.QueryRow(`SELECT shobj_description(oid, 'pg_authid') FROM pg_roles WHERE rolname = $1`, role).Scan(&comment)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	return true, isConsoleRole(comment.String), nil
}

// applyConsoleRole makes role what p says: created if missing (with password), its grants in every
// named database exactly p's, revoked in p.RevokeDatabases, VALID UNTIL p.ValidUntil, and -- when
// it has a new password -- in ProxySQL. warn reports what was applied only in part.
func applyConsoleRole(server *sql.DB, open dbOpener, proxy proxyUsers, host string, hostgroup int, role string,
	p rolePayload, password string, now time.Time) (warn string, err error) {
	if err := ValidateRoleName(role); err != nil {
		return "", err
	}
	if err := checkRolePayload(p); err != nil {
		return "", err
	}
	q := pq.QuoteIdentifier(role)
	exists, ours, err := consoleRoleState(server, role)
	if err != nil {
		return "", fmt.Errorf("looking up role: %w", err)
	}
	switch {
	case exists && !ours:
		return "", fmt.Errorf("role %q exists and was not created by AuthNull; refusing to change it", role)
	case !exists && password == "":
		return "", fmt.Errorf("role %q does not exist and the job carries no password to create it with", role)
	case !exists:
		if _, err := server.Exec(fmt.Sprintf("CREATE ROLE %s LOGIN NOINHERIT PASSWORD %s", q, pq.QuoteLiteral(password))); err != nil {
			return "", fmt.Errorf("creating role: %v", strings.ReplaceAll(err.Error(), password, "[redacted]"))
		}
		if _, err := server.Exec(fmt.Sprintf("COMMENT ON ROLE %s IS %s", q, pq.QuoteLiteral(consoleRoleMark))); err != nil {
			return "", fmt.Errorf("marking role: %w", err)
		}
	case password != "":
		if _, err := server.Exec(fmt.Sprintf("ALTER ROLE %s WITH LOGIN NOINHERIT PASSWORD %s", q, pq.QuoteLiteral(password))); err != nil {
			return "", fmt.Errorf("setting the role's password: %v", strings.ReplaceAll(err.Error(), password, "[redacted]"))
		}
	}
	until := "infinity"
	if p.ValidUntil != "" {
		until = p.ValidUntil
	}
	if _, err := server.Exec(fmt.Sprintf("ALTER ROLE %s VALID UNTIL %s", q, pq.QuoteLiteral(until))); err != nil {
		return "", fmt.Errorf("setting VALID UNTIL: %w", err)
	}
	// pg_hba.conf admits the managed group from ProxySQL only (directaccess.go).
	if err := ensureManagedGroup(server, role); err != nil {
		return "", err
	}

	var warnings []string
	wanted := map[string]roleDBGrants{}
	for _, d := range p.Grants.Databases {
		wanted[d.Database] = d
	}
	databases := append([]string{}, p.RevokeDatabases...)
	for _, d := range p.Grants.Databases {
		databases = append(databases, d.Database)
	}
	for _, name := range databases {
		w, err := applyInDatabase(open, role, name, wanted[name], wanted[name].Database != "")
		if err != nil {
			return "", fmt.Errorf("database %s: %w", name, err)
		}
		warnings = append(warnings, w...)
	}

	if password != "" {
		row := proxyUser{Username: role, Password: password, DefaultHostgroup: hostgroup, Comment: vaultComment(host, now)}
		if err := proxy.Put(row); err != nil {
			if err2 := proxy.Put(row); err2 != nil {
				warnings = append(warnings, "the role has its password, but ProxySQL could not be updated: "+err2.Error())
			}
		}
	}
	return strings.Join(warnings, "; "), nil
}

// applyInDatabase revokes everything role holds in one database and, when grant, grants d -- in
// one transaction, so a failure leaves the previous grants.
func applyInDatabase(open dbOpener, role, name string, d roleDBGrants, grant bool) ([]string, error) {
	db, err := open(name)
	if err != nil {
		return nil, fmt.Errorf("connecting: %w", err)
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("connecting: %w", err)
	}
	defer tx.Rollback()
	schemas, err := queryStrings(tx, `SELECT nspname FROM pg_namespace
		WHERE nspname NOT LIKE 'pg\_%' AND nspname <> 'information_schema' ORDER BY nspname`)
	if err != nil {
		return nil, err
	}
	acls, err := roleDefaultACLs(tx, role)
	if err != nil {
		return nil, err
	}
	stmts := revokeStatements(role, name, schemas, acls)
	var warnings []string
	if grant {
		owners, skipped, err := futureOwners(tx, d)
		if err != nil {
			return nil, err
		}
		if len(skipped) > 0 {
			warnings = append(warnings, fmt.Sprintf("%s: future privileges not set for objects created by %s (the agent cannot act for them)",
				name, strings.Join(skipped, ", ")))
		}
		stmts = append(stmts, grantStatements(role, d, owners)...)
	}
	for _, s := range stmts {
		if _, err := tx.Exec(s); err != nil {
			return nil, fmt.Errorf("%s: %w", s, err)
		}
	}
	return warnings, tx.Commit()
}

type querier interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

func queryStrings(q querier, query string, args ...any) ([]string, error) {
	rows, err := q.Query(query, args...)
	if err != nil {
		return nil, err
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

func roleDefaultACLs(q querier, role string) ([]defaultACL, error) {
	rows, err := q.Query(`SELECT DISTINCT pg_get_userbyid(d.defaclrole), n.nspname, d.defaclobjtype::text
		FROM pg_default_acl d JOIN pg_namespace n ON n.oid = d.defaclnamespace, aclexplode(d.defaclacl) a
		WHERE a.grantee = (SELECT oid FROM pg_roles WHERE rolname = $1)
		ORDER BY 1, 2, 3`, role)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []defaultACL
	for rows.Next() {
		var a defaultACL
		if err := rows.Scan(&a.Owner, &a.Schema, &a.ObjType); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// futureOwners: for each schema with a future grant, the roles whose new objects the grant should
// cover -- the schema's owner and its tables' owners -- that the agent may act for. The rest are
// skipped and reported.
func futureOwners(q querier, d roleDBGrants) (map[string][]string, []string, error) {
	out := map[string][]string{}
	skipped := map[string]bool{}
	for _, o := range d.Objects {
		if !o.Future || out[o.Schema] != nil {
			continue
		}
		candidates, err := queryStrings(q, `
			SELECT DISTINCT r.rolname FROM pg_roles r
			 WHERE r.oid IN (SELECT nspowner FROM pg_namespace WHERE nspname = $1
			                 UNION SELECT c.relowner FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			                        WHERE n.nspname = $1 AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f'))
			 ORDER BY 1`, o.Schema)
		if err != nil {
			return nil, nil, err
		}
		actable, err := queryStrings(q, `
			SELECT r.rolname FROM pg_roles r
			 WHERE r.rolname = ANY($1) AND ((SELECT rolsuper FROM pg_roles WHERE rolname = current_user)
			       OR pg_has_role(current_user, r.oid, 'USAGE'))
			 ORDER BY 1`, pq.Array(candidates))
		if err != nil {
			return nil, nil, err
		}
		ok := map[string]bool{}
		for _, r := range actable {
			ok[r] = true
		}
		for _, c := range candidates {
			if !ok[c] {
				skipped[c] = true
			}
		}
		out[o.Schema] = append([]string{}, actable...)
	}
	names := make([]string, 0, len(skipped))
	for n := range skipped {
		names = append(names, n)
	}
	sort.Strings(names)
	return out, names, nil
}

// dropConsoleRole removes a console-created role: logins stop, its sessions end, what it owns
// passes to the agent's user (nothing is deleted), its privileges go, the role goes, and so does
// its ProxySQL row. A role that is already gone is done.
func dropConsoleRole(server *sql.DB, open dbOpener, proxy proxyUsers, role string, p dropPayload) error {
	return dropOwnRole(server, open, proxy, role, p, func() (bool, bool, error) { return consoleRoleState(server, role) },
		"was not created by AuthNull")
}

// dropOwnRole drops role, which state says exists and is AuthNull's -- or refuses, saying why not.
func dropOwnRole(server *sql.DB, open dbOpener, proxy proxyUsers, role string, p dropPayload,
	state func() (exists, ours bool, err error), notOurs string) error {
	if err := ValidateRoleName(role); err != nil {
		return err
	}
	for _, d := range p.Databases {
		if !grantableDatabase(d) {
			return fmt.Errorf("database %q cannot be dropped from", d)
		}
	}
	exists, ours, err := state()
	if err != nil {
		return fmt.Errorf("looking up role: %w", err)
	}
	if exists && !ours {
		return fmt.Errorf("role %q exists and %s; refusing to drop it", role, notOurs)
	}
	if exists {
		q := pq.QuoteIdentifier(role)
		if _, err := server.Exec(fmt.Sprintf("ALTER ROLE %s NOLOGIN", q)); err != nil {
			return fmt.Errorf("stopping logins: %w", err)
		}
		// Membership lets an agent that is not a superuser end the role's sessions and take over what
		// it owns (Postgres allows both to members); DROP ROLE removes it again.
		if _, err := server.Exec(fmt.Sprintf("GRANT %s TO CURRENT_USER", q)); err != nil {
			return fmt.Errorf("taking membership of the role: %w", err)
		}
		if _, err := server.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = $1`, role); err != nil {
			return fmt.Errorf("ending sessions: %w", err)
		}
		for _, name := range p.Databases {
			if err := dropInDatabase(open, role, name); err != nil {
				return fmt.Errorf("database %s: %w", name, err)
			}
		}
		if _, err := server.Exec("DROP ROLE " + q); err != nil {
			return fmt.Errorf("dropping role: %w", err)
		}
	}
	if err := proxy.Delete(role); err != nil {
		return fmt.Errorf("the role is dropped, but its ProxySQL user could not be removed: %w", err)
	}
	return nil
}

func dropInDatabase(open dbOpener, role, name string) error {
	db, err := open(name)
	if err != nil {
		return fmt.Errorf("connecting: %w", err)
	}
	defer db.Close()
	q := pq.QuoteIdentifier(role)
	for _, s := range []string{"REASSIGN OWNED BY " + q + " TO CURRENT_USER", "DROP OWNED BY " + q} {
		if _, err := db.Exec(s); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
	}
	return nil
}

// openDatabase connects to one database on the configured server, as the agent.
func openDatabase(cfg DBConfig, database string) (*sql.DB, error) {
	if isMySQL(cfg) {
		return sql.Open("mysql", mysqlDSN(cfg, database))
	}
	kv := func(v string) string {
		return "'" + strings.ReplaceAll(strings.ReplaceAll(v, `\`, `\\`), `'`, `\'`) + "'"
	}
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		kv(cfg.Host), kv(cfg.Port), kv(cfg.User), kv(cfg.Password), kv(database))
	return sql.Open("postgres", dsn)
}
