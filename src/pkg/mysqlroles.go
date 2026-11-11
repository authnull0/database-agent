package pkg

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Managed accounts on a MySQL target: the vault's jobs (vaultjobs.go) and the scheduled rotation,
// as for PostgreSQL roles, with MySQL's statements.
//
// A MySQL account is user@host; the agent manages the user name. A password is set on every host
// row of that name (rotate_password, the sweep): ProxySQL logs in as the name, and a customer's
// account with both 'app'@'%' and 'app'@'localhost' must not keep a password the vault no longer
// has on one of them. The accounts the agent creates -- console users and policy users -- are
// 'name'@'%' only, marked with a user attribute:
//
//	console users  ATTRIBUTE '{"authnull":"console"}'   (consolerole.go's consoleRoleMark)
//	policy users   ATTRIBUTE '{"authnull":"policy"}'    (an_..., backendrole.go)
//
// and the agent alters or drops only an account that carries its mark and has no other host row.
//
// The agent's MySQL account needs CREATE USER, SELECT on mysql.* (accounts, grants, attributes),
// CONNECTION_ADMIN (to end a dropped account's sessions), and the privileges it grants WITH GRANT
// OPTION.

const (
	mysqlConsoleMark = "console"
	mysqlPolicyMark  = "policy"
)

// mysqlMaxUserName is MySQL 8.0's limit on a user name.
const mysqlMaxUserName = 32

// managedHost is the host a managed user's ProxySQL comment records: the target's host for
// PostgreSQL, as always, and host:port for MySQL, so that two MySQL servers on one IP never claim
// each other's rows. (A MySQL and a PostgreSQL target never meet: their rows are in different tables.)
func managedHost(config DBConfig) string {
	if isMySQL(config) {
		return config.Host + ":" + strconv.Itoa(targetPort(config))
	}
	return config.Host
}

// validateMySQLUserName is ValidateRoleName with MySQL's shorter limit.
func validateMySQLUserName(user string) error {
	if err := ValidateRoleName(user); err != nil {
		return err
	}
	if len(user) > mysqlMaxUserName {
		return fmt.Errorf("user name is %d bytes, longer than MySQL's %d", len(user), mysqlMaxUserName)
	}
	return nil
}

// mysqlUserHosts lists the hosts of the accounts named user.
func mysqlUserHosts(db *sql.DB, user string) ([]string, error) {
	return queryStrings(db, "SELECT Host FROM mysql.user WHERE User = ? ORDER BY Host", user)
}

// mysqlUserState: does an account named user exist, and does it carry the agent's mark -- 'user'@'%'
// alone, with the attribute {"authnull": mark}?
func mysqlUserState(db *sql.DB, user, mark string) (exists, ours bool, err error) {
	hosts, err := mysqlUserHosts(db, user)
	if err != nil || len(hosts) == 0 {
		return false, false, err
	}
	if len(hosts) != 1 || hosts[0] != "%" {
		return true, false, nil
	}
	var attr sql.NullString
	err = db.QueryRow("SELECT ATTRIBUTE FROM information_schema.USER_ATTRIBUTES WHERE USER = ? AND HOST = '%'", user).Scan(&attr)
	if errors.Is(err, sql.ErrNoRows) {
		return true, false, nil
	}
	if err != nil {
		return true, false, err
	}
	var a struct {
		AuthNull string `json:"authnull"`
	}
	if attr.String == "" || json.Unmarshal([]byte(attr.String), &a) != nil {
		return true, false, nil
	}
	return true, a.AuthNull == mark, nil
}

// mysqlSetPassword sets password on every host row of user. An account that does not exist is an
// error: rotation never creates one.
func mysqlSetPassword(db *sql.DB, user, password string) error {
	hosts, err := mysqlUserHosts(db, user)
	if err != nil {
		return fmt.Errorf("checking user: %w", err)
	}
	if len(hosts) == 0 {
		return fmt.Errorf("user %q does not exist", user)
	}
	pw, err := mysqlString(password)
	if err != nil {
		return err
	}
	for _, h := range hosts {
		acct, err := mysqlAccount(user, h)
		if err != nil {
			return err
		}
		if _, err := db.Exec("ALTER USER " + acct + " IDENTIFIED BY " + pw); err != nil {
			return fmt.Errorf("setting the password of %s: %v", acct, strings.ReplaceAll(err.Error(), password, "[redacted]"))
		}
	}
	return nil
}

// mysqlCreateMarkedUser creates 'user'@'%' with password and the agent's mark.
func mysqlCreateMarkedUser(db *sql.DB, user, password, mark string) error {
	acct, err := mysqlAccount(user, "%")
	if err != nil {
		return err
	}
	pw, err := mysqlString(password)
	if err != nil {
		return err
	}
	attr, err := json.Marshal(map[string]string{"authnull": mark})
	if err != nil {
		return err
	}
	lit, err := mysqlString(string(attr))
	if err != nil {
		return err
	}
	if _, err := db.Exec("CREATE USER " + acct + " IDENTIFIED BY " + pw + " ATTRIBUTE " + lit); err != nil {
		return fmt.Errorf("creating user: %v", strings.ReplaceAll(err.Error(), password, "[redacted]"))
	}
	return nil
}

// --- console users (apply_role / drop_role) ---

var (
	mysqlIdentRe = regexp.MustCompile(`^[A-Za-z0-9_$]{1,64}$`)

	mysqlTablePrivileges = []string{"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "REFERENCES",
		"INDEX", "ALTER", "CREATE VIEW", "SHOW VIEW", "TRIGGER"}
	mysqlRoutinePrivileges = []string{"EXECUTE", "ALTER ROUTINE"}

	// mysqlObjectPriv is what a console user can be granted on each kind of object. GRANT OPTION
	// and ALL are never among them.
	mysqlObjectPriv = map[string]map[string]bool{
		"database":  setOf(mysqlDatabasePrivileges),
		"table":     setOf(mysqlTablePrivileges),
		"function":  setOf(mysqlRoutinePrivileges),
		"procedure": setOf(mysqlRoutinePrivileges),
	}
)

func setOf(list []string) map[string]bool {
	out := map[string]bool{}
	for _, s := range list {
		out[s] = true
	}
	return out
}

// mysqlGrantableDatabase: a plain name, and not one of MySQL's own databases.
func mysqlGrantableDatabase(name string) bool {
	return mysqlIdentRe.MatchString(name) && !isSystemDatabase(strings.ToLower(name), EngineMySQL)
}

// checkMySQLRolePayload is checkRolePayload for a MySQL target. Its grants have no schema: an object
// is the database itself, or a table, function or procedure in it.
func checkMySQLRolePayload(p rolePayload) error {
	if len(p.Grants.Databases) == 0 {
		return errors.New("the job grants nothing")
	}
	for _, d := range p.Grants.Databases {
		if !mysqlGrantableDatabase(d.Database) {
			return fmt.Errorf("database %q cannot be granted on", d.Database)
		}
		for _, o := range d.Objects {
			allowed, ok := mysqlObjectPriv[o.Type]
			if !ok {
				return fmt.Errorf("unknown object type %q for MySQL", o.Type)
			}
			if o.Schema != "" {
				return fmt.Errorf("%s grant names schema %q: MySQL grants have no schema", o.Type, o.Schema)
			}
			named := o.Type != "database"
			if named != (o.Name != "") || (named && !mysqlIdentRe.MatchString(o.Name)) {
				return fmt.Errorf("%s %q: bad object name", o.Type, o.Name)
			}
			if len(o.Privileges) == 0 {
				return fmt.Errorf("%s grant with no privileges", o.Type)
			}
			for _, pr := range o.Privileges {
				if !allowed[pr] {
					return fmt.Errorf("%q is not a MySQL %s privilege", pr, o.Type)
				}
			}
		}
	}
	for _, d := range p.RevokeDatabases {
		if !mysqlGrantableDatabase(d) {
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

// mysqlGrantKey is a grantable object in one database: kind database (name: the grant's database
// pattern, as mysql.db stores it), table, function or procedure (name: the object's).
type mysqlGrantKey struct{ kind, name string }

// mysqlGrants are the privileges held, or wanted, on each object.
type mysqlGrants map[mysqlGrantKey]map[string]bool

// mysqlColumnGrant is one column privilege: the payload never grants these, so any are revoked.
type mysqlColumnGrant struct{ Table, Column, Privilege string }

// mysqlWantedGrants are a payload database's objects as grants. The database-level grant is made on
// the escaped pattern, so it covers this database alone (mysqlDBGrantTarget).
func mysqlWantedGrants(d roleDBGrants) mysqlGrants {
	out := mysqlGrants{}
	for _, o := range d.Objects {
		k := mysqlGrantKey{o.Type, o.Name}
		if o.Type == "database" {
			k.name = mysqlDBPattern(d.Database)
		}
		if out[k] == nil {
			out[k] = map[string]bool{}
		}
		for _, p := range o.Privileges {
			out[k][p] = true
		}
	}
	return out
}

// mysqlGrantObject is the object part of a GRANT or REVOKE on k in database.
func mysqlGrantObject(database string, k mysqlGrantKey) string {
	switch k.kind {
	case "database":
		return mysqlIdent(k.name) + ".*" // k.name is already the stored pattern
	case "function", "procedure":
		return strings.ToUpper(k.kind) + " " + mysqlIdent(database) + "." + mysqlIdent(k.name)
	}
	return mysqlIdent(database) + "." + mysqlIdent(k.name)
}

// mysqlPrivilegeOrder is the order privileges appear in in a statement, GRANT OPTION last.
var mysqlPrivilegeOrder = func() map[string]int {
	out := map[string]int{}
	for i, p := range mysqlDatabasePrivileges {
		out[p] = i
	}
	out["GRANT OPTION"] = len(mysqlDatabasePrivileges)
	return out
}()

func orderedPrivileges(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		oi, iok := mysqlPrivilegeOrder[out[i]]
		oj, jok := mysqlPrivilegeOrder[out[j]]
		if iok != jok {
			return iok
		}
		if oi != oj {
			return oi < oj
		}
		return out[i] < out[j]
	})
	return out
}

func sortedGrantKeys(g mysqlGrants) []mysqlGrantKey {
	out := make([]mysqlGrantKey, 0, len(g))
	for k := range g {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].kind != out[j].kind {
			return out[i].kind < out[j].kind
		}
		return out[i].name < out[j].name
	})
	return out
}

// mysqlGrantDiff are the statements that make acct's grants in database exactly wanted, given what
// it holds: the missing privileges granted first (so access the payload keeps never lapses), then
// the extra ones revoked -- GRANT OPTION and column privileges included -- and nothing said about
// what already matches.
func mysqlGrantDiff(acct, database string, held mysqlGrants, cols []mysqlColumnGrant, wanted mysqlGrants) []string {
	var out []string
	for _, k := range sortedGrantKeys(wanted) {
		missing := map[string]bool{}
		for p := range wanted[k] {
			if !held[k][p] {
				missing[p] = true
			}
		}
		if len(missing) > 0 {
			out = append(out, fmt.Sprintf("GRANT %s ON %s TO %s", strings.Join(orderedPrivileges(missing), ", "), mysqlGrantObject(database, k), acct))
		}
	}
	for _, k := range sortedGrantKeys(held) {
		extra := map[string]bool{}
		for p := range held[k] {
			if !wanted[k][p] {
				extra[p] = true
			}
		}
		if len(extra) > 0 {
			out = append(out, fmt.Sprintf("REVOKE %s ON %s FROM %s", strings.Join(orderedPrivileges(extra), ", "), mysqlGrantObject(database, k), acct))
		}
	}
	for _, c := range cols {
		out = append(out, fmt.Sprintf("REVOKE %s (%s) ON %s FROM %s", c.Privilege, mysqlIdent(c.Column),
			mysqlGrantObject(database, mysqlGrantKey{"table", c.Table}), acct))
	}
	return out
}

// mysqlHeldGrants reads what user@host holds in database: on the database itself (under its exact
// name or its escaped pattern -- a wider pattern such as `shop%` is not this database's, and is left
// alone), its tables and columns, and its routines.
func mysqlHeldGrants(db *sql.DB, user, host, database string) (mysqlGrants, []mysqlColumnGrant, error) {
	grantee := mysqlGrantee(user, host)
	held := mysqlGrants{}
	add := func(k mysqlGrantKey, p string) {
		if held[k] == nil {
			held[k] = map[string]bool{}
		}
		held[k][p] = true
	}
	read := func(query string, args []any, scan func(r *sql.Rows) error) error {
		rows, err := db.Query(query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			if err := scan(rows); err != nil {
				return err
			}
		}
		return rows.Err()
	}
	err := read(`SELECT TABLE_SCHEMA, PRIVILEGE_TYPE, IS_GRANTABLE FROM information_schema.SCHEMA_PRIVILEGES
		WHERE GRANTEE = ? AND TABLE_SCHEMA IN (?, ?)`, []any{grantee, database, mysqlDBPattern(database)},
		func(r *sql.Rows) error {
			var schema, priv, grantable string
			if err := r.Scan(&schema, &priv, &grantable); err != nil {
				return err
			}
			add(mysqlGrantKey{"database", schema}, priv)
			if grantable == "YES" {
				add(mysqlGrantKey{"database", schema}, "GRANT OPTION")
			}
			return nil
		})
	if err != nil {
		return nil, nil, fmt.Errorf("reading database grants: %w", err)
	}
	err = read(`SELECT TABLE_NAME, PRIVILEGE_TYPE, IS_GRANTABLE FROM information_schema.TABLE_PRIVILEGES
		WHERE GRANTEE = ? AND TABLE_SCHEMA = ?`, []any{grantee, database},
		func(r *sql.Rows) error {
			var table, priv, grantable string
			if err := r.Scan(&table, &priv, &grantable); err != nil {
				return err
			}
			add(mysqlGrantKey{"table", table}, priv)
			if grantable == "YES" {
				add(mysqlGrantKey{"table", table}, "GRANT OPTION")
			}
			return nil
		})
	if err != nil {
		return nil, nil, fmt.Errorf("reading table grants: %w", err)
	}
	var cols []mysqlColumnGrant
	err = read(`SELECT TABLE_NAME, COLUMN_NAME, PRIVILEGE_TYPE FROM information_schema.COLUMN_PRIVILEGES
		WHERE GRANTEE = ? AND TABLE_SCHEMA = ? ORDER BY TABLE_NAME, COLUMN_NAME, PRIVILEGE_TYPE`, []any{grantee, database},
		func(r *sql.Rows) error {
			var c mysqlColumnGrant
			if err := r.Scan(&c.Table, &c.Column, &c.Privilege); err != nil {
				return err
			}
			cols = append(cols, c)
			return nil
		})
	if err != nil {
		return nil, nil, fmt.Errorf("reading column grants: %w", err)
	}
	// Routine grants are in mysql.procs_priv only; Proc_priv is a set of "Execute", "Alter Routine", "Grant".
	err = read(`SELECT Routine_name, Routine_type, Proc_priv FROM mysql.procs_priv WHERE User = ? AND Host = ? AND Db = ?`,
		[]any{user, host, database},
		func(r *sql.Rows) error {
			var name, kind, privs string
			if err := r.Scan(&name, &kind, &privs); err != nil {
				return err
			}
			k := mysqlGrantKey{strings.ToLower(kind), name}
			for _, p := range strings.Split(privs, ",") {
				switch strings.TrimSpace(p) {
				case "Execute":
					add(k, "EXECUTE")
				case "Alter Routine":
					add(k, "ALTER ROUTINE")
				case "Grant":
					add(k, "GRANT OPTION")
				}
			}
			return nil
		})
	if err != nil {
		return nil, nil, fmt.Errorf("reading routine grants: %w", err)
	}
	return held, cols, nil
}

// syncMySQLGrants makes user@'%”s grants in one database exactly d's (nothing, for a database in
// revokeDatabases). MySQL's GRANT and REVOKE commit as they go, so there is no transaction: a
// failure part way is reported, and the next apply of the same job finishes the change.
func syncMySQLGrants(db *sql.DB, user, database string, d roleDBGrants) error {
	acct, err := mysqlAccount(user, "%")
	if err != nil {
		return err
	}
	held, cols, err := mysqlHeldGrants(db, user, "%", database)
	if err != nil {
		return err
	}
	for _, s := range mysqlGrantDiff(acct, database, held, cols, mysqlWantedGrants(d)) {
		if _, err := db.Exec(s); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
	}
	return nil
}

// applyMySQLConsoleUser is applyConsoleRole for a MySQL target: the account 'user'@'%' is created if
// missing (with password and the console mark), its grants in every named database become exactly
// p's, those in p.RevokeDatabases are revoked, and -- when it has a new password -- ProxySQL gets it.
// MySQL has no VALID UNTIL: the backend drops an expired console user, so validUntil is only logged.
func applyMySQLConsoleUser(server *sql.DB, proxy proxyUsers, host string, hostgroup int, user string,
	p rolePayload, password string, now time.Time) (warn string, err error) {
	if err := validateMySQLUserName(user); err != nil {
		return "", err
	}
	if err := checkMySQLRolePayload(p); err != nil {
		return "", err
	}
	exists, ours, err := mysqlUserState(server, user, mysqlConsoleMark)
	if err != nil {
		return "", fmt.Errorf("looking up user: %w", err)
	}
	switch {
	case exists && !ours:
		return "", fmt.Errorf("user %q exists and was not created by AuthNull; refusing to change it", user)
	case !exists && password == "":
		return "", fmt.Errorf("user %q does not exist and the job carries no password to create it with", user)
	case !exists:
		if err := mysqlCreateMarkedUser(server, user, password, mysqlConsoleMark); err != nil {
			return "", err
		}
	case password != "":
		if err := mysqlSetPassword(server, user, password); err != nil {
			return "", err
		}
	}
	if p.ValidUntil != "" {
		log.Printf("Console user %q: validUntil %s is not set in MySQL, which has no equivalent; the backend drops the user when it expires",
			user, p.ValidUntil)
	}

	for _, name := range p.RevokeDatabases {
		if err := syncMySQLGrants(server, user, name, roleDBGrants{Database: name}); err != nil {
			return "", fmt.Errorf("database %s: %w", name, err)
		}
	}
	for _, d := range p.Grants.Databases {
		if err := syncMySQLGrants(server, user, d.Database, d); err != nil {
			return "", fmt.Errorf("database %s: %w", d.Database, err)
		}
	}

	if password != "" {
		row := proxyUser{Username: user, Password: password, DefaultHostgroup: hostgroup, Comment: vaultComment(host, now)}
		if err := proxy.Put(row); err != nil {
			if err2 := proxy.Put(row); err2 != nil {
				return "the user has its password, but ProxySQL could not be updated: " + err2.Error(), nil
			}
		}
	}
	return "", nil
}

// dropMySQLUser drops user, which state says exists and is AuthNull's -- or refuses, saying why
// not: logins stop (ACCOUNT LOCK), its sessions end (DROP USER leaves them running), the account
// goes with all its grants, and so does its ProxySQL row. A user that is already gone is done.
func dropMySQLUser(server *sql.DB, proxy proxyUsers, user string, state func() (exists, ours bool, err error), notOurs string) error {
	if err := validateMySQLUserName(user); err != nil {
		return err
	}
	exists, ours, err := state()
	if err != nil {
		return fmt.Errorf("looking up user: %w", err)
	}
	if exists && !ours {
		return fmt.Errorf("user %q exists and %s; refusing to drop it", user, notOurs)
	}
	if exists {
		acct, err := mysqlAccount(user, "%")
		if err != nil {
			return err
		}
		if _, err := server.Exec("ALTER USER " + acct + " ACCOUNT LOCK"); err != nil {
			return fmt.Errorf("stopping logins: %w", err)
		}
		ids, err := queryStrings(server, "SELECT CAST(ID AS CHAR) FROM information_schema.PROCESSLIST WHERE USER = ?", user)
		if err != nil {
			return fmt.Errorf("finding sessions: %w", err)
		}
		for _, id := range ids {
			n, err := strconv.ParseUint(id, 10, 64)
			if err != nil {
				continue
			}
			// A session that ended meanwhile is "Unknown thread id": nothing to do.
			if _, err := server.Exec("KILL CONNECTION " + strconv.FormatUint(n, 10)); err != nil && !strings.Contains(err.Error(), "1094") {
				return fmt.Errorf("ending session %d: %w", n, err)
			}
		}
		if _, err := server.Exec("DROP USER " + acct); err != nil {
			return fmt.Errorf("dropping user: %w", err)
		}
	}
	if err := proxy.Delete(user); err != nil {
		return fmt.Errorf("the user is dropped, but its ProxySQL user could not be removed: %w", err)
	}
	return nil
}

// --- policy users (apply_policy_role / drop_policy_role) ---

// mysqlPolicyPrivileges are the MySQL privileges a restricted user gets for a policy's privileges:
// the table verbs (backendRoleVerbs), with TRUNCATE as DROP -- MySQL's TRUNCATE TABLE needs the DROP
// privilege on the table, and there is no narrower one. (DROP also lets the user drop the table.)
func mysqlPolicyPrivileges(privileges []string) []string {
	var out []string
	for _, v := range backendRoleVerbs(privileges) {
		if v == "TRUNCATE" {
			v = "DROP"
		}
		out = append(out, v)
	}
	return out
}

// mysqlPolicyTable is a policy table as `database`.`table`. A table is named bare, under
// PostgreSQL's default schema (public: the canonical form BackendRoleName shares with ProxySQL), or
// under the database's own name; any other qualifier is a table in another database, refused. The
// name keeps its case: MySQL table names are case-sensitive on Linux.
func mysqlPolicyTable(database, table string) (string, error) {
	t := strings.TrimSpace(table)
	if i := strings.Index(t, "."); i >= 0 {
		qualifier := strings.TrimSpace(t[:i])
		if !strings.EqualFold(qualifier, "public") && !strings.EqualFold(qualifier, database) {
			return "", fmt.Errorf("table %q is not in database %q", table, database)
		}
		t = strings.TrimSpace(t[i+1:])
	}
	if t == "" {
		return "", fmt.Errorf("table %q has no name", table)
	}
	return mysqlIdent(database) + "." + mysqlIdent(t), nil
}

// provisionMySQLPolicyUser is ProvisionBackendRole for a MySQL target: the restricted user for a
// policy, 'an_...'@'%' with the policy mark, holding the policy's privileges on its tables (or on
// the database when it names none), registered in mysql_users afterwards -- a user ProxySQL can find
// is always a complete one. Returns the name, or "" when the policy gets no restricted user.
func provisionMySQLPolicyUser(server *sql.DB, proxy proxyUsers, host, database, sourceRole string, privileges, tables []string,
	hostgroupID int, now time.Time, interval time.Duration) (string, error) {
	name, ok := BackendRoleName(database, sourceRole, privileges, tables)
	if !ok {
		return "", nil
	}
	var targets []string
	for _, t := range tables {
		if strings.TrimSpace(t) == "" {
			continue
		}
		target, err := mysqlPolicyTable(database, t)
		if err != nil {
			return "", err
		}
		targets = append(targets, target)
	}
	if len(targets) == 0 {
		// No tables named: the policy covers the database.
		targets = []string{mysqlDBGrantTarget(database)}
	}

	// One password, held by ProxySQL and set on the user: keep ProxySQL's until it is due.
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

	exists, ours, err := mysqlUserState(server, name, mysqlPolicyMark)
	if err != nil {
		return "", fmt.Errorf("look up user %s: %w", name, err)
	}
	switch {
	case exists && !ours:
		return "", fmt.Errorf("user %q exists and is not an AuthNull restricted role; refusing to change it", name)
	case !exists:
		if err := mysqlCreateMarkedUser(server, name, password, mysqlPolicyMark); err != nil {
			return "", fmt.Errorf("create user %s: %w", name, err)
		}
	default:
		// Always re-set: ProxySQL's copy and the user's must not drift apart.
		if err := mysqlSetPassword(server, name, password); err != nil {
			return "", fmt.Errorf("set password of %s: %w", name, err)
		}
	}

	acct, err := mysqlAccount(name, "%")
	if err != nil {
		return "", err
	}
	privList := strings.Join(mysqlPolicyPrivileges(privileges), ", ")
	for _, target := range targets {
		s := fmt.Sprintf("GRANT %s ON %s TO %s", privList, target, acct)
		if _, err := server.Exec(s); err != nil {
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

// mysqlPolicyUserState: does user exist, and is it one of AuthNull's restricted users (an_..., with
// the policy mark)?
func mysqlPolicyUserState(server *sql.DB, user string) (exists, ours bool, err error) {
	exists, marked, err := mysqlUserState(server, user, mysqlPolicyMark)
	return exists, marked && strings.HasPrefix(user, "an_"), err
}
