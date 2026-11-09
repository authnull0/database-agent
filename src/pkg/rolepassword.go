package pkg

import (
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
)

// Managed role passwords (DB-MFA-HARDENING S4).
//
// ProxySQL is the only holder of a managed role's password: users log in with a console token
// and ProxySQL logs in to the database for them. So the password can be long, random and rotated
// at will without anyone noticing. Every name and value that reaches SQL is quoted, because role
// names come from the policy API.

// rolePasswordLength is long enough that guessing is hopeless (62^32 ≈ 2^190).
const rolePasswordLength = 32

// passwordCharset is letters and digits only: nothing that needs escaping in a SQL literal, a
// connection string or a shell, so a mistake elsewhere can't turn the password into syntax.
const passwordCharset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// DefaultPasswordRotation is how long a managed role keeps its password until the console's
// interval arrives, and the console's own default.
const DefaultPasswordRotation = 24 * time.Hour

// MaxPasswordRotationHours is the longest interval the console offers: a year.
const MaxPasswordRotationHours = 8760

// The rotation interval is the tenant's choice in the AuthNull console (AUTH-233). It arrives in
// the response to every last-active report (LastActive) and is held here, for checkout and the
// scheduled sweep. It used to be PASSWORD_ROTATION_HOURS in db.env, which is no longer read.
var rotationInterval = struct {
	sync.Mutex
	d time.Duration
}{d: DefaultPasswordRotation}

// RotationInterval is the interval in use.
func RotationInterval() time.Duration {
	rotationInterval.Lock()
	defer rotationInterval.Unlock()
	return rotationInterval.d
}

// SetRotationHours takes the console's interval. A value outside 1..MaxPasswordRotationHours is
// refused and the interval in use is kept: rotation is never turned off by a bad value.
func SetRotationHours(hours int) error {
	if hours < 1 || hours > MaxPasswordRotationHours {
		return fmt.Errorf("password rotation interval of %d hours is outside 1..%d", hours, MaxPasswordRotationHours)
	}
	d := time.Duration(hours) * time.Hour
	rotationInterval.Lock()
	prev := rotationInterval.d
	rotationInterval.d = d
	rotationInterval.Unlock()
	if prev != d {
		log.Printf("Password rotation interval is now %d hours (set in the AuthNull console)", hours)
	}
	return nil
}

// rotationCommentPrefix marks a pgsql_users row the agent manages. The comment carries the host
// the role lives on and when its password was last set: "authnull:host=<host>;rotated=<unix>".
const rotationCommentPrefix = "authnull:"

// GenerateRandomPassword returns a password of length characters from crypto/rand, each drawn
// uniformly from passwordCharset.
func GenerateRandomPassword(length int) (string, error) {
	if length < 16 {
		return "", fmt.Errorf("password length %d is too short (minimum 16)", length)
	}
	max := big.NewInt(int64(len(passwordCharset)))
	b := make([]byte, length)
	for i := range b {
		n, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", fmt.Errorf("reading random bytes: %w", err)
		}
		b[i] = passwordCharset[n.Int64()]
	}
	return string(b), nil
}

// ValidateRoleName refuses names that can't be a managed role. Quoting makes any other name safe
// in SQL; these are the ones that would still break something.
func ValidateRoleName(role string) error {
	switch {
	case role == "":
		return errors.New("role name is empty")
	case len(role) > 63:
		return fmt.Errorf("role name is %d bytes, longer than PostgreSQL's 63", len(role))
	case strings.ContainsRune(role, 0):
		return errors.New("role name contains a NUL byte")
	case strings.Contains(role, ","):
		// ProxySQL splits the login at the first comma into role and token.
		return errors.New("role name contains a comma, which ProxySQL treats as the token separator")
	}
	return nil
}

// rotationComment is the pgsql_users comment for a role on host whose password was set at t.
func rotationComment(host string, t time.Time) string {
	return fmt.Sprintf("%shost=%s;rotated=%d", rotationCommentPrefix, host, t.Unix())
}

// parseRotationComment reads a comment written by rotationComment. ok is false for any other
// comment, including the empty one on rows from agents before S4.
func parseRotationComment(comment string) (host string, rotated time.Time, ok bool) {
	if !strings.HasPrefix(comment, rotationCommentPrefix) {
		return "", time.Time{}, false
	}
	rest := strings.TrimPrefix(comment, rotationCommentPrefix)
	var gotHost, gotRotated bool
	for _, field := range strings.Split(rest, ";") {
		k, v := field, ""
		if i := strings.IndexByte(field, '='); i >= 0 {
			k, v = field[:i], field[i+1:]
		}
		switch k {
		case "host":
			host, gotHost = v, true
		case "rotated":
			secs, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return "", time.Time{}, false
			}
			rotated, gotRotated = time.Unix(secs, 0), true
		}
	}
	if !gotHost || !gotRotated {
		return "", time.Time{}, false
	}
	return host, rotated, true
}

// rotationDue says whether u needs a new password at now. A row with no password, or with no
// rotation record (written before S4, so its password came from math/rand), is due at once.
func rotationDue(u proxyUser, now time.Time, interval time.Duration) bool {
	if u.Password == "" {
		return true
	}
	_, rotated, ok := parseRotationComment(u.Comment)
	if !ok {
		return true
	}
	return now.Sub(rotated) >= interval
}

// --- PostgreSQL ---

// pgSetPasswordStatement is CREATE ROLE (when the role is new) or ALTER ROLE, with the name and
// the password quoted. PostgreSQL can't take either as a bind parameter in DDL.
func pgSetPasswordStatement(role, password string, create bool) string {
	if create {
		return fmt.Sprintf("CREATE ROLE %s WITH LOGIN PASSWORD %s", pq.QuoteIdentifier(role), pq.QuoteLiteral(password))
	}
	return fmt.Sprintf("ALTER ROLE %s WITH PASSWORD %s", pq.QuoteIdentifier(role), pq.QuoteLiteral(password))
}

// pgRoleExists looks the role up with a bind parameter.
func pgRoleExists(db *sql.DB, role string) (bool, error) {
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM pg_roles WHERE rolname = $1", role).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

// pgSetPassword creates the role with password, or sets the password of the existing role.
func pgSetPassword(db *sql.DB, role, password string) error {
	exists, err := pgRoleExists(db, role)
	if err != nil {
		return fmt.Errorf("checking role: %w", err)
	}
	if _, err := db.Exec(pgSetPasswordStatement(role, password, !exists)); err != nil {
		if exists {
			return fmt.Errorf("setting role password: %w", err)
		}
		return fmt.Errorf("creating role: %w", err)
	}
	return nil
}

// --- ProxySQL ---

// proxyUser is the part of a pgsql_users row the agent reads and writes.
type proxyUser struct {
	Username         string
	Password         string
	DefaultHostgroup int
	Comment          string
}

// proxyUsers is ProxySQL's pgsql_users table. Put writes the row and makes it live.
type proxyUsers interface {
	Get(role string) (proxyUser, bool, error)
	Put(u proxyUser) error
	List() ([]proxyUser, error)
	Delete(role string) error
}

// sqliteLiteral quotes s for ProxySQL's admin interface, which is SQLite: a single quote is
// doubled and nothing else is special. The MySQL driver can't do this for us: its client-side
// interpolation escapes with backslashes, which SQLite doesn't understand, and the admin
// interface has no server-side prepared statements.
func sqliteLiteral(s string) (string, error) {
	if strings.ContainsRune(s, 0) {
		return "", errors.New("value contains a NUL byte")
	}
	return "'" + strings.ReplaceAll(s, "'", "''") + "'", nil
}

// adminProxyUsers is pgsql_users through a connection to ProxySQL's admin interface.
type adminProxyUsers struct{ db *sql.DB }

func (a adminProxyUsers) Get(role string) (proxyUser, bool, error) {
	q, err := proxyGetUserQuery(role)
	if err != nil {
		return proxyUser{}, false, err
	}
	u := proxyUser{Username: role}
	var password sql.NullString
	err = a.db.QueryRow(q).Scan(&password, &u.DefaultHostgroup, &u.Comment)
	if err == sql.ErrNoRows {
		return proxyUser{}, false, nil
	}
	if err != nil {
		return proxyUser{}, false, err
	}
	u.Password = password.String
	return u, true, nil
}

func (a adminProxyUsers) List() ([]proxyUser, error) {
	rows, err := a.db.Query("SELECT username, password, default_hostgroup, comment FROM pgsql_users")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var users []proxyUser
	for rows.Next() {
		var u proxyUser
		var password sql.NullString
		if err := rows.Scan(&u.Username, &password, &u.DefaultHostgroup, &u.Comment); err != nil {
			return nil, err
		}
		u.Password = password.String
		users = append(users, u)
	}
	return users, rows.Err()
}

func (a adminProxyUsers) Put(u proxyUser) error {
	_, found, err := a.Get(u.Username)
	if err != nil {
		return err
	}
	q, err := proxyPutUserQuery(u, found)
	if err != nil {
		return err
	}
	if _, err := a.db.Exec(q); err != nil {
		return err
	}
	if _, err := a.db.Exec("LOAD PGSQL USERS TO RUNTIME"); err != nil {
		return fmt.Errorf("loading users to runtime: %w", err)
	}
	if _, err := a.db.Exec("SAVE PGSQL USERS TO DISK"); err != nil {
		return fmt.Errorf("saving users to disk: %w", err)
	}
	return nil
}

// Delete removes role's row, if any.
func (a adminProxyUsers) Delete(role string) error {
	r, err := sqliteLiteral(role)
	if err != nil {
		return err
	}
	if _, err := a.db.Exec("DELETE FROM pgsql_users WHERE username = " + r); err != nil {
		return err
	}
	if _, err := a.db.Exec("LOAD PGSQL USERS TO RUNTIME"); err != nil {
		return fmt.Errorf("loading users to runtime: %w", err)
	}
	if _, err := a.db.Exec("SAVE PGSQL USERS TO DISK"); err != nil {
		return fmt.Errorf("saving users to disk: %w", err)
	}
	return nil
}

func proxyGetUserQuery(role string) (string, error) {
	r, err := sqliteLiteral(role)
	if err != nil {
		return "", err
	}
	return "SELECT password, default_hostgroup, comment FROM pgsql_users WHERE username = " + r + " LIMIT 1", nil
}

// proxyPutUserQuery inserts the row, or updates the existing one (found).
func proxyPutUserQuery(u proxyUser, found bool) (string, error) {
	name, err := sqliteLiteral(u.Username)
	if err != nil {
		return "", err
	}
	password, err := sqliteLiteral(u.Password)
	if err != nil {
		return "", err
	}
	comment, err := sqliteLiteral(u.Comment)
	if err != nil {
		return "", err
	}
	if found {
		return fmt.Sprintf("UPDATE pgsql_users SET password = %s, default_hostgroup = %d, comment = %s WHERE username = %s",
			password, u.DefaultHostgroup, comment, name), nil
	}
	return fmt.Sprintf("INSERT INTO pgsql_users (username, password, default_hostgroup, active, use_ssl, comment) VALUES (%s, %s, %d, 1, 0, %s)",
		name, password, u.DefaultHostgroup, comment), nil
}

// --- Rotation ---

// rotateRolePassword gives role on host a fresh password: first in the database, then in
// ProxySQL. In that order a failure leaves ProxySQL with the old password and the next login
// fails until the next rotation, rather than the database keeping a password that ProxySQL
// no longer has and nobody can rotate away from.
func rotateRolePassword(pg *sql.DB, proxy proxyUsers, role, host string, hostgroup int, now time.Time) (string, error) {
	if err := ValidateRoleName(role); err != nil {
		return "", err
	}
	password, err := GenerateRandomPassword(rolePasswordLength)
	if err != nil {
		return "", err
	}
	if err := pgSetPassword(pg, role, password); err != nil {
		return "", err
	}
	err = proxy.Put(proxyUser{
		Username:         role,
		Password:         password,
		DefaultHostgroup: hostgroup,
		Comment:          rotationComment(host, now),
	})
	if err != nil {
		return "", fmt.Errorf("storing the new password in ProxySQL: %w", err)
	}
	log.Printf("Rotated the password of managed role %q on %s", role, host)
	return password, nil
}

// EnsureRolePassword returns the password ProxySQL uses for role, rotating it first when it's
// due, when the role doesn't exist in the database yet, or when ProxySQL has none.
// hostgroup 0 keeps the row's current hostgroup.
func EnsureRolePassword(pg *sql.DB, proxy proxyUsers, role, host string, hostgroup int, now time.Time, interval time.Duration) (string, error) {
	if err := ValidateRoleName(role); err != nil {
		return "", err
	}
	u, found, err := proxy.Get(role)
	if err != nil {
		return "", fmt.Errorf("reading ProxySQL user: %w", err)
	}
	if hostgroup == 0 && found {
		hostgroup = u.DefaultHostgroup
	}
	exists, err := pgRoleExists(pg, role)
	if err != nil {
		return "", fmt.Errorf("checking role: %w", err)
	}
	// A vault-managed role keeps the vault's password: checkout never rotates it (vaultjobs.go).
	if found && isVaultManaged(u.Comment) {
		if !exists {
			return "", fmt.Errorf("role %q is managed in the vault but does not exist in the database", role)
		}
		if u.DefaultHostgroup != hostgroup {
			u.DefaultHostgroup = hostgroup
			if err := proxy.Put(u); err != nil {
				return "", fmt.Errorf("updating ProxySQL hostgroup: %w", err)
			}
		}
		return u.Password, nil
	}
	if found && exists && !rotationDue(u, now, interval) {
		if u.DefaultHostgroup != hostgroup {
			u.DefaultHostgroup = hostgroup
			if err := proxy.Put(u); err != nil {
				return "", fmt.Errorf("updating ProxySQL hostgroup: %w", err)
			}
		}
		return u.Password, nil
	}
	return rotateRolePassword(pg, proxy, role, host, hostgroup, now)
}

// RotateDuePasswords rotates every managed role on host whose password is older than interval.
// Roles nobody checks out still get new passwords, so a leaked one stops working. Rows the agent
// doesn't manage on this host are left alone: legacy rows are adopted at their next checkout.
// It returns the roles it rotated; one role failing doesn't stop the others.
func RotateDuePasswords(pg *sql.DB, proxy proxyUsers, host string, now time.Time, interval time.Duration) ([]string, error) {
	users, err := proxy.List()
	if err != nil {
		return nil, fmt.Errorf("listing ProxySQL users: %w", err)
	}
	var rotated []string
	var errs []string
	for _, u := range users {
		h, _, ok := parseRotationComment(u.Comment)
		// A vault-managed role's password is the backend's (vaultjobs.go): never replaced here.
		if !ok || h != host || isVaultManaged(u.Comment) || !rotationDue(u, now, interval) {
			continue
		}
		if _, err := rotateRolePassword(pg, proxy, u.Username, host, u.DefaultHostgroup, now); err != nil {
			errs = append(errs, fmt.Sprintf("role %q: %v", u.Username, err))
			continue
		}
		rotated = append(rotated, u.Username)
	}
	if len(errs) > 0 {
		return rotated, errors.New(strings.Join(errs, "; "))
	}
	return rotated, nil
}

// RotateDuePasswordsForHost is RotateDuePasswords with the agent's own ProxySQL connection.
func RotateDuePasswordsForHost(pg *sql.DB, config DBConfig, now time.Time) error {
	interval := RotationInterval()
	proxyDB, err := ConnectToProxysqlDB(config)
	if err != nil {
		return err
	}
	defer proxyDB.Close()
	rotated, err := RotateDuePasswords(pg, adminProxyUsers{proxyDB}, config.Host, now, interval)
	if len(rotated) > 0 {
		log.Printf("Scheduled rotation: %d managed role(s) on %s", len(rotated), config.Host)
	}
	return err
}
