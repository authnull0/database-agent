package pkg

import (
	"database/sql"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
)

// Integration tests. They run only when pointed at real servers:
//
//	DBAGENT_TEST_PG_DSN        postgres://<role with CREATEROLE>:<pw>@host:port/db?sslmode=disable
//	DBAGENT_TEST_PROXYSQL_DSN  admin:admin@tcp(127.0.0.1:6032)/   (ProxySQL's admin interface)
//
// The Postgres tests create and drop roles named authnull_s4_test_*; the ProxySQL test writes and
// removes pgsql_users rows with that prefix.

const testRolePrefix = "authnull_s4_test_"

func testPG(t *testing.T) (*sql.DB, string) {
	t.Helper()
	dsn := os.Getenv("DBAGENT_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("DBAGENT_TEST_PG_DSN not set")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, dsn
}

func dropRoles(t *testing.T, db *sql.DB, roles ...string) {
	t.Helper()
	for _, r := range roles {
		if _, err := db.Exec("DROP ROLE IF EXISTS " + pq.QuoteIdentifier(r)); err != nil {
			t.Errorf("dropping %q: %v", r, err)
		}
	}
}

// canLogin tries to log in to the test database as role with password.
func canLogin(t *testing.T, baseDSN, role, password string) bool {
	t.Helper()
	u, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatalf("DBAGENT_TEST_PG_DSN must be a URL: %v", err)
	}
	u.User = url.UserPassword(role, password)
	db, err := sql.Open("postgres", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	return db.Ping() == nil
}

func TestIntegrationHostileRoleIsCreatedRotatedAndHarmless(t *testing.T) {
	db, dsn := testPG(t)
	victim := testRolePrefix + "victim"
	hostile := testRolePrefix + `x"; drop role ` + victim + `; --`
	dropRoles(t, db, hostile, victim)
	t.Cleanup(func() { dropRoles(t, db, hostile, victim) })
	if _, err := db.Exec("CREATE ROLE " + pq.QuoteIdentifier(victim)); err != nil {
		t.Fatal(err)
	}

	proxy := newFakeProxy()
	now := time.Now()
	pw1, err := EnsureRolePassword(db, proxy, hostile, "h1", 2, now, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if exists, _ := pgRoleExists(db, victim); !exists {
		t.Fatal("the hostile name dropped another role")
	}
	if exists, _ := pgRoleExists(db, hostile); !exists {
		t.Fatal("the role was not created under its literal name")
	}
	if got := proxy.users[hostile]; got.Password != pw1 || got.DefaultHostgroup != 2 {
		t.Fatalf("ProxySQL row %+v", got)
	}
	if !canLogin(t, dsn, hostile, pw1) {
		t.Fatal("cannot log in with the password ProxySQL holds")
	}

	// Not due: the same password, nothing written.
	puts := proxy.puts
	pw, err := EnsureRolePassword(db, proxy, hostile, "h1", 2, now.Add(time.Hour), 24*time.Hour)
	if err != nil || pw != pw1 || proxy.puts != puts {
		t.Fatalf("reuse: pw changed=%v err=%v puts %d→%d", pw != pw1, err, puts, proxy.puts)
	}

	// Due: a new password that works, and the old one doesn't.
	pw2, err := EnsureRolePassword(db, proxy, hostile, "h1", 2, now.Add(25*time.Hour), 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if pw2 == pw1 {
		t.Fatal("password not rotated")
	}
	if !canLogin(t, dsn, hostile, pw2) {
		t.Fatal("cannot log in with the rotated password")
	}
	if canLogin(t, dsn, hostile, pw1) {
		t.Fatal("the old password still works")
	}
}

func TestIntegrationLegacyRowIsRotatedAtCheckout(t *testing.T) {
	db, dsn := testPG(t)
	role := testRolePrefix + "legacy"
	dropRoles(t, db, role)
	t.Cleanup(func() { dropRoles(t, db, role) })
	// What an agent before S4 left behind: a role, and a row with its weak password and no record.
	if _, err := db.Exec("CREATE ROLE " + pq.QuoteIdentifier(role) + " WITH LOGIN PASSWORD 'weak-legacy-pw-1'"); err != nil {
		t.Fatal(err)
	}
	proxy := newFakeProxy()
	proxy.users[role] = proxyUser{Username: role, Password: "weak-legacy-pw-1", DefaultHostgroup: 4}

	pw, err := EnsureRolePassword(db, proxy, role, "h1", 0, time.Now(), 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if pw == "weak-legacy-pw-1" || len(pw) != rolePasswordLength {
		t.Fatal("legacy password kept")
	}
	if got := proxy.users[role]; got.DefaultHostgroup != 4 {
		t.Fatalf("hostgroup 0 should keep the row's hostgroup, got %d", got.DefaultHostgroup)
	}
	if !canLogin(t, dsn, role, pw) || canLogin(t, dsn, role, "weak-legacy-pw-1") {
		t.Fatal("login state after adoption is wrong")
	}
}

func TestIntegrationRoleMissingInDatabaseIsRecreated(t *testing.T) {
	db, dsn := testPG(t)
	role := testRolePrefix + "dropped"
	dropRoles(t, db, role)
	t.Cleanup(func() { dropRoles(t, db, role) })
	now := time.Now()
	proxy := newFakeProxy()
	// ProxySQL has a fresh row, but someone dropped the role in the database.
	proxy.users[role] = proxyUser{Username: role, Password: "fresh-but-orphaned-pw", Comment: rotationComment("h1", now)}

	pw, err := EnsureRolePassword(db, proxy, role, "h1", 1, now, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if pw == "fresh-but-orphaned-pw" || !canLogin(t, dsn, role, pw) {
		t.Fatal("role not recreated with a new password")
	}
}

func TestIntegrationScheduledRotationTouchesOnlyDueRolesOnThisHost(t *testing.T) {
	db, dsn := testPG(t)
	due, fresh, other, legacy := testRolePrefix+"due", testRolePrefix+"fresh", testRolePrefix+"other", testRolePrefix+"unmanaged"
	all := []string{due, fresh, other, legacy}
	dropRoles(t, db, all...)
	t.Cleanup(func() { dropRoles(t, db, all...) })

	now := time.Now()
	proxy := newFakeProxy()
	set := func(role, comment string) {
		if _, err := db.Exec(pgSetPasswordStatement(role, "start-pw-"+role, true)); err != nil {
			t.Fatal(err)
		}
		proxy.users[role] = proxyUser{Username: role, Password: "start-pw-" + role, DefaultHostgroup: 1, Comment: comment}
	}
	set(due, rotationComment("h1", now.Add(-48*time.Hour)))
	set(fresh, rotationComment("h1", now.Add(-time.Hour)))
	set(other, rotationComment("h2", now.Add(-48*time.Hour)))
	set(legacy, "")

	rotated, err := RotateDuePasswords(db, proxy, "h1", now, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(rotated) != 1 || rotated[0] != due {
		t.Fatalf("rotated %v, want [%s]", rotated, due)
	}
	if !canLogin(t, dsn, due, proxy.users[due].Password) || canLogin(t, dsn, due, "start-pw-"+due) {
		t.Fatal("due role not rotated")
	}
	for _, r := range []string{fresh, other, legacy} {
		if proxy.users[r].Password != "start-pw-"+r || !canLogin(t, dsn, r, "start-pw-"+r) {
			t.Errorf("%s was touched", r)
		}
	}
	if _, at, _ := parseRotationComment(proxy.users[due].Comment); at.Unix() != now.Unix() {
		t.Errorf("rotation record not updated: %q", proxy.users[due].Comment)
	}
}

func TestIntegrationScheduledRotationCarriesOnPastAFailure(t *testing.T) {
	db, _ := testPG(t)
	a, b := testRolePrefix+"fail_a", testRolePrefix+"fail_b"
	dropRoles(t, db, a, b)
	t.Cleanup(func() { dropRoles(t, db, a, b) })
	now := time.Now()
	proxy := newFakeProxy()
	for _, r := range []string{a, b} {
		if _, err := db.Exec(pgSetPasswordStatement(r, "start", true)); err != nil {
			t.Fatal(err)
		}
		proxy.users[r] = proxyUser{Username: r, Password: "start", Comment: rotationComment("h1", now.Add(-48*time.Hour))}
	}
	proxy.failPut[a] = true

	rotated, err := RotateDuePasswords(db, proxy, "h1", now, 24*time.Hour)
	if err == nil || !strings.Contains(err.Error(), a) {
		t.Fatalf("want an error naming %s, got %v", a, err)
	}
	if len(rotated) != 1 || rotated[0] != b {
		t.Fatalf("rotated %v, want [%s]", rotated, b)
	}
}

func TestIntegrationProxySQLAdminUsersQuoteHostileValues(t *testing.T) {
	dsn := os.Getenv("DBAGENT_TEST_PROXYSQL_DSN")
	if dsn == "" {
		t.Skip("DBAGENT_TEST_PROXYSQL_DSN not set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	// Registered before cleanup, so it runs after it (cleanups run last-in, first-out). A defer
	// would close the connection before cleanup could delete the test rows.
	t.Cleanup(func() { db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	p := adminProxyUsers{db}
	hostile := testRolePrefix + `o'brien'); DELETE FROM pgsql_users; --`
	plain := testRolePrefix + "plain"
	cleanup := func() {
		for _, r := range []string{hostile, plain} {
			lit, _ := sqliteLiteral(r)
			if _, err := db.Exec("DELETE FROM pgsql_users WHERE username = " + lit); err != nil {
				t.Errorf("removing test row %q: %v", r, err)
			}
		}
		for _, q := range []string{"LOAD PGSQL USERS TO RUNTIME", "SAVE PGSQL USERS TO DISK"} {
			if _, err := db.Exec(q); err != nil {
				t.Errorf("%s: %v", q, err)
			}
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	var before int
	if err := db.QueryRow("SELECT COUNT(*) FROM pgsql_users").Scan(&before); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1793500000, 0)
	if err := p.Put(proxyUser{Username: plain, Password: "pw-plain", DefaultHostgroup: 7, Comment: rotationComment("h1", now)}); err != nil {
		t.Fatal(err)
	}
	if err := p.Put(proxyUser{Username: hostile, Password: "pw'one", DefaultHostgroup: 7, Comment: rotationComment("h'1", now)}); err != nil {
		t.Fatal(err)
	}
	var after int
	if err := db.QueryRow("SELECT COUNT(*) FROM pgsql_users").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before+2 {
		t.Fatalf("pgsql_users went from %d to %d rows, want +2", before, after)
	}

	got, found, err := p.Get(hostile)
	if err != nil || !found {
		t.Fatalf("Get: found=%v err=%v", found, err)
	}
	if got.Password != "pw'one" || got.DefaultHostgroup != 7 || got.Comment != rotationComment("h'1", now) {
		t.Fatalf("round trip: %+v", got)
	}

	// Update in place, and the change reaches runtime.
	if err := p.Put(proxyUser{Username: hostile, Password: "pw-two", DefaultHostgroup: 8, Comment: rotationComment("h'1", now)}); err != nil {
		t.Fatal(err)
	}
	lit, _ := sqliteLiteral(hostile)
	var runtimePW string
	var hg int
	if err := db.QueryRow("SELECT password, default_hostgroup FROM runtime_pgsql_users WHERE username = "+lit+" LIMIT 1").Scan(&runtimePW, &hg); err != nil {
		t.Fatal(err)
	}
	if runtimePW != "pw-two" || hg != 8 {
		t.Fatalf("runtime row: %q hostgroup %d", runtimePW, hg)
	}

	users, err := p.List()
	if err != nil {
		t.Fatal(err)
	}
	var listed int
	for _, u := range users {
		if strings.HasPrefix(u.Username, testRolePrefix) {
			listed++
		}
	}
	if listed != 2 {
		t.Fatalf("List found %d test rows, want 2", listed)
	}
}
