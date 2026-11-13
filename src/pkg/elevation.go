package pkg

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/lib/pq"
)

// Privilege elevation (authnull-service docs/DB-ELEVATION.md): an owner's own account is given a
// role for the length of an approved grant, and loses it when the grant ends.
//
// elevate makes the account a member of the payload's role: an existing role (grant), or a bundle
// the agent builds from the policy's privileges and tables (database, privileges, tables) -- a
// NOLOGIN role named by its shape and marked elevationBundleMark. unelevate takes the membership
// back and ends the account's open sessions, so nothing keeps running with what was granted.
//
// A role that would let its member out of the database -- a superuser, CREATEROLE, BYPASSRLS,
// REPLICATION, or reading and writing the server's files or running its programs -- is never
// granted, nor a membership the account already holds: ending the grant would take away standing
// access nobody gave through AuthNull.

const elevationBundleMark = "authnull:elevation"

type elevatePayload struct {
	Grant      string   `json:"grant,omitempty"`
	Database   string   `json:"database,omitempty"`
	Privileges []string `json:"privileges,omitempty"`
	Tables     []string `json:"tables,omitempty"`
}

// escapeRoles are predefined roles whose members reach past the database: the server's files and
// programs, and other people's sessions.
var escapeRoles = []string{"pg_execute_server_program", "pg_read_server_files", "pg_write_server_files", "pg_signal_backend"}

// ElevationBundleName is the bundle for a custom elevation's shape, false when the privileges give
// none (EXECUTE, an unknown privilege, or none).
func ElevationBundleName(database string, privileges, tables []string) (string, bool) {
	verbs, ok := verbSet(privileges)
	if !ok {
		return "", false
	}
	var privs []string
	for v := range verbs {
		privs = append(privs, v)
	}
	sort.Strings(privs)
	canonical := "elev1|" + strings.ToLower(strings.TrimSpace(database)) + "|" + strings.Join(privs, ",") +
		"|" + strings.Join(canonicalTables(tables), ",")
	sum := sha256.Sum256([]byte(canonical))
	return "authnull_elev_" + hex.EncodeToString(sum[:])[:16], true
}

// reservedRole: a role no elevation names -- AuthNull's own, or the server's predefined ones.
func reservedRole(role string) bool {
	l := strings.ToLower(role)
	return strings.HasPrefix(l, "pg_") || strings.HasPrefix(l, "authnull_") || strings.HasPrefix(l, "an_") ||
		strings.HasPrefix(l, "jit_") || l == "postgres" || role == ManagedGroup || role == BackendRolesGroup
}

// checkElevatePayload: a role to grant, or a bundle's shape -- never both, never neither.
func checkElevatePayload(p elevatePayload) error {
	custom := p.Database != "" || len(p.Privileges) > 0 || len(p.Tables) > 0
	switch {
	case p.Grant == "" && !custom:
		return errors.New("the job names no role and no privileges")
	case p.Grant != "" && custom:
		return errors.New("the job names a role and privileges; an elevation grants one or the other")
	case p.Grant != "":
		if err := ValidateRoleName(p.Grant); err != nil {
			return err
		}
		if reservedRole(p.Grant) {
			return fmt.Errorf("role %q cannot be granted: it is AuthNull's or the server's own", p.Grant)
		}
		return nil
	}
	if !grantableDatabase(p.Database) {
		return fmt.Errorf("database %q cannot be granted on", p.Database)
	}
	if _, ok := ElevationBundleName(p.Database, p.Privileges, p.Tables); !ok {
		return fmt.Errorf("privileges %v cannot be granted by an elevation (SELECT, INSERT, UPDATE, DELETE, TRUNCATE, READ, WRITE)", p.Privileges)
	}
	return nil
}

// elevationRole is the role an elevation grants: the payload's, or its bundle's name.
func elevationRole(p elevatePayload) string {
	if p.Grant != "" {
		return p.Grant
	}
	name, _ := ElevationBundleName(p.Database, p.Privileges, p.Tables)
	return name
}

// escapingRole names what, in role or anything it is a member of, reaches past the database; ""
// when nothing does.
func escapingRole(server *sql.DB, role string) (string, error) {
	var hit sql.NullString
	err := server.QueryRow(`WITH RECURSIVE g(oid) AS (
		  SELECT oid FROM pg_roles WHERE rolname = $1
		  UNION SELECT m.roleid FROM pg_auth_members m JOIN g ON m.member = g.oid)
		SELECT r.rolname FROM pg_roles r JOIN g ON g.oid = r.oid
		WHERE r.rolsuper OR r.rolcreaterole OR r.rolbypassrls OR r.rolreplication OR r.rolname = ANY($2)
		ORDER BY r.rolname LIMIT 1`, role, pq.Array(escapeRoles)).Scan(&hit)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return hit.String, err
}

// membership: is account a direct member of role, and did the agent's own login grant it?
func membership(server *sql.DB, role, account string) (member, ours bool, err error) {
	err = server.QueryRow(`SELECT g.rolname = current_user FROM pg_auth_members m
		JOIN pg_roles r ON r.oid = m.roleid JOIN pg_roles a ON a.oid = m.member
		LEFT JOIN pg_roles g ON g.oid = m.grantor
		WHERE r.rolname = $1 AND a.rolname = $2`, role, account).Scan(&ours)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false, nil
	}
	return err == nil, ours, err
}

// ensureBundle makes p's bundle: a NOLOGIN role holding the privileges on the tables, in its
// database. It is granted again on every elevation, so a bundle over a whole schema picks up tables
// created since.
func ensureBundle(server *sql.DB, open dbOpener, name string, p elevatePayload) error {
	q := pq.QuoteIdentifier(name)
	var comment sql.NullString
	err := server.QueryRow(`SELECT shobj_description(oid, 'pg_authid') FROM pg_roles WHERE rolname = $1`, name).Scan(&comment)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := server.Exec("CREATE ROLE " + q + " NOLOGIN"); err != nil {
			return fmt.Errorf("creating %s: %w", name, err)
		}
		if _, err := server.Exec(fmt.Sprintf("COMMENT ON ROLE %s IS %s", q, pq.QuoteLiteral(elevationBundleMark))); err != nil {
			return fmt.Errorf("marking %s: %w", name, err)
		}
	case err != nil:
		return fmt.Errorf("looking up %s: %w", name, err)
	case comment.String != elevationBundleMark:
		return fmt.Errorf("role %q exists and is not an AuthNull elevation bundle", name)
	}
	target, err := open(p.Database)
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", p.Database, err)
	}
	defer target.Close()
	stmts := []string{fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", pq.QuoteIdentifier(p.Database), q)}
	more, err := tableGrantStatements(target, q, backendRoleVerbs(p.Privileges), p.Tables)
	if err != nil {
		return err
	}
	for _, s := range append(stmts, more...) {
		if _, err := target.Exec(s); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
	}
	return nil
}

// elevate makes account a member of the payload's role.
func elevate(server *sql.DB, open dbOpener, config DBConfig, account string, p elevatePayload) (warn string, err error) {
	if err := checkElevatePayload(p); err != nil {
		return "", err
	}
	if reservedRole(account) {
		return "", fmt.Errorf("account %q is AuthNull's or the server's own; it is not elevated", account)
	}
	role := elevationRole(p)
	if own := strings.TrimSpace(config.User); role == own || builtinAdmin(config, role) {
		return "", fmt.Errorf("role %q cannot be granted: it is the agent's login or the server's admin", role)
	}
	exists, err := pgRoleExists(server, account)
	if err != nil {
		return "", fmt.Errorf("looking up account: %w", err)
	}
	if !exists {
		return "", fmt.Errorf("account %q does not exist", account)
	}
	if p.Grant == "" {
		if err := ensureBundle(server, open, role, p); err != nil {
			return "", err
		}
	} else if exists, err := pgRoleExists(server, role); err != nil {
		return "", fmt.Errorf("looking up role: %w", err)
	} else if !exists {
		return "", fmt.Errorf("role %q does not exist", role)
	}
	if hit, err := escapingRole(server, role); err != nil {
		return "", fmt.Errorf("checking role: %w", err)
	} else if hit != "" {
		return "", fmt.Errorf("role %q cannot be granted: through %s it reaches past the database", role, hit)
	}
	member, _, err := membership(server, role, account)
	if err != nil {
		return "", fmt.Errorf("checking membership: %w", err)
	}
	if member {
		return "", fmt.Errorf("account %q is already a member of %q; an elevation would grant nothing and its end would take standing access away", account, role)
	}
	if _, err := server.Exec(fmt.Sprintf("GRANT %s TO %s", pq.QuoteIdentifier(role), pq.QuoteIdentifier(account))); err != nil {
		return "", fmt.Errorf("granting %s: %w", role, err)
	}
	var inherit bool
	if err := server.QueryRow(`SELECT rolinherit FROM pg_roles WHERE rolname = $1`, account).Scan(&inherit); err == nil && !inherit {
		return fmt.Sprintf("account %q does not inherit; its sessions need SET ROLE %s", account, role), nil
	}
	return "", nil
}

// unelevate takes back the membership the agent granted and ends the account's sessions. A
// membership someone else granted is left in place, and said so.
func unelevate(server *sql.DB, account string, p elevatePayload) (warn string, err error) {
	if err := checkElevatePayload(p); err != nil {
		return "", err
	}
	role := elevationRole(p)
	exists, err := pgRoleExists(server, account)
	if err != nil {
		return "", fmt.Errorf("looking up account: %w", err)
	}
	if !exists {
		return "", nil
	}
	member, ours, err := membership(server, role, account)
	if err != nil {
		return "", fmt.Errorf("checking membership: %w", err)
	}
	switch {
	case member && ours:
		if _, err := server.Exec(fmt.Sprintf("REVOKE %s FROM %s", pq.QuoteIdentifier(role), pq.QuoteIdentifier(account))); err != nil {
			return "", fmt.Errorf("revoking %s: %w", role, err)
		}
	case member:
		warn = fmt.Sprintf("account %q is a member of %q through a grant AuthNull did not make; left in place", account, role)
	}
	if err := endSessions(server, account); err != nil {
		return warn, err
	}
	return warn, nil
}

// endSessions ends account's sessions. Membership lets an agent that is not a superuser signal
// them (Postgres allows members); it is taken only for that, and given back.
func endSessions(server *sql.DB, account string) error {
	q := pq.QuoteIdentifier(account)
	member, _, err := membership(server, account, currentUser(server))
	if err != nil {
		return fmt.Errorf("checking membership: %w", err)
	}
	if !member {
		if _, err := server.Exec(fmt.Sprintf("GRANT %s TO CURRENT_USER", q)); err != nil {
			return fmt.Errorf("taking membership of the account: %w", err)
		}
		defer server.Exec(fmt.Sprintf("REVOKE %s FROM CURRENT_USER", q))
	}
	if _, err := server.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = $1`, account); err != nil {
		return fmt.Errorf("ending sessions: %w", err)
	}
	return nil
}

func currentUser(server *sql.DB) string {
	var u string
	server.QueryRow("SELECT current_user").Scan(&u)
	return u
}

// runElevationJob runs elevate or unelevate for job j.
func runElevationJob(pg *sql.DB, config DBConfig, j vaultJob) (bool, string, error) {
	if isMySQL(config) {
		return false, "", errors.New("elevation is not yet supported on MySQL")
	}
	var p elevatePayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return false, "", fmt.Errorf("%s payload: %w", j.Kind, err)
	}
	var warn string
	var err error
	if j.Kind == "elevate" {
		open := func(name string) (*sql.DB, error) { return openRoleDatabase(config, name) }
		warn, err = elevate(pg, open, config, j.Role, p)
	} else {
		warn, err = unelevate(pg, j.Role, p)
	}
	if err != nil {
		return false, "", err
	}
	return true, warn, nil
}
