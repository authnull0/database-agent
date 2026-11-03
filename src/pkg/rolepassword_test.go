package pkg

import (
	"strings"
	"testing"
	"time"
)

func TestGenerateRandomPasswordLengthAndCharset(t *testing.T) {
	pw, err := GenerateRandomPassword(rolePasswordLength)
	if err != nil {
		t.Fatal(err)
	}
	if len(pw) != rolePasswordLength {
		t.Fatalf("length %d, want %d", len(pw), rolePasswordLength)
	}
	for _, c := range pw {
		if !strings.ContainsRune(passwordCharset, c) {
			t.Fatalf("character %q is outside the charset", c)
		}
	}
}

func TestGenerateRandomPasswordIsNotRepeatable(t *testing.T) {
	// The old generator seeded math/rand with the clock: two calls in the same instant matched.
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		pw, err := GenerateRandomPassword(rolePasswordLength)
		if err != nil {
			t.Fatal(err)
		}
		if seen[pw] {
			t.Fatalf("password repeated after %d calls", i)
		}
		seen[pw] = true
	}
}

func TestGenerateRandomPasswordUsesWholeCharset(t *testing.T) {
	counts := map[rune]int{}
	for i := 0; i < 200; i++ {
		pw, _ := GenerateRandomPassword(rolePasswordLength)
		for _, c := range pw {
			counts[c]++
		}
	}
	// 6400 draws over 62 characters: ~103 each. Any character never drawn means a skewed generator.
	if len(counts) != len(passwordCharset) {
		t.Fatalf("drew %d distinct characters, want all %d", len(counts), len(passwordCharset))
	}
}

func TestGenerateRandomPasswordRefusesShortLengths(t *testing.T) {
	if _, err := GenerateRandomPassword(15); err == nil {
		t.Fatal("want an error for a 15-character password")
	}
}

func TestValidateRoleName(t *testing.T) {
	ok := []string{"dbmfa_reader", `x"; drop role y; --`, "x'; drop role y; --", "Mixed Case", strings.Repeat("a", 63)}
	for _, name := range ok {
		if err := ValidateRoleName(name); err != nil {
			t.Errorf("ValidateRoleName(%q) = %v, want ok", name, err)
		}
	}
	bad := []string{"", strings.Repeat("a", 64), "a\x00b", "reader,token"}
	for _, name := range bad {
		if err := ValidateRoleName(name); err == nil {
			t.Errorf("ValidateRoleName(%q) = nil, want an error", name)
		}
	}
}

// The interval comes from the console (AUTH-233), through the last-active response. Until the
// first one arrives it is the default; a value outside 1..8760 hours is refused and the
// interval in use is kept, never turned off.
func TestRotationIntervalFromTheConsole(t *testing.T) {
	t.Cleanup(resetRotationIntervalForTest)
	resetRotationIntervalForTest()
	if got := RotationInterval(); got != DefaultPasswordRotation {
		t.Fatalf("before the console says anything: %v, want %v", got, DefaultPasswordRotation)
	}
	if err := SetRotationHours(168); err != nil || RotationInterval() != 168*time.Hour {
		t.Fatalf("168 hours: %v, %v", RotationInterval(), err)
	}
	for _, bad := range []int{0, -1, MaxPasswordRotationHours + 1} {
		if err := SetRotationHours(bad); err == nil {
			t.Errorf("SetRotationHours(%d) accepted", bad)
		}
		if got := RotationInterval(); got != 168*time.Hour {
			t.Errorf("after refusing %d the interval is %v, want 168h kept", bad, got)
		}
	}
	if err := SetRotationHours(1); err != nil || RotationInterval() != time.Hour {
		t.Fatalf("1 hour: %v, %v", RotationInterval(), err)
	}
}

func TestRotationCommentRoundTrip(t *testing.T) {
	at := time.Unix(1793500000, 0)
	host, rotated, ok := parseRotationComment(rotationComment("192.168.122.175", at))
	if !ok || host != "192.168.122.175" || !rotated.Equal(at) {
		t.Fatalf("got %q %v %v", host, rotated, ok)
	}
	for _, c := range []string{"", "some admin note", "authnull:host=h", "authnull:rotated=1", "authnull:host=h;rotated=x"} {
		if _, _, ok := parseRotationComment(c); ok {
			t.Errorf("parseRotationComment(%q) ok, want not ours", c)
		}
	}
}

func TestRotationDue(t *testing.T) {
	now := time.Unix(1793500000, 0)
	fresh := proxyUser{Password: "p", Comment: rotationComment("h", now.Add(-time.Hour))}
	old := proxyUser{Password: "p", Comment: rotationComment("h", now.Add(-25*time.Hour))}
	legacy := proxyUser{Password: "p"}
	empty := proxyUser{Comment: rotationComment("h", now)}
	if rotationDue(fresh, now, 24*time.Hour) {
		t.Error("an hour-old password is not due")
	}
	if !rotationDue(old, now, 24*time.Hour) {
		t.Error("a 25-hour-old password is due")
	}
	if !rotationDue(legacy, now, 24*time.Hour) {
		t.Error("a password from before S4 (no record) is due")
	}
	if !rotationDue(empty, now, 24*time.Hour) {
		t.Error("an empty password is due")
	}
}

func TestPgSetPasswordStatementQuotes(t *testing.T) {
	role := `x"; drop role y; --`
	pw := "pa'ss"
	got := pgSetPasswordStatement(role, pw, true)
	want := `CREATE ROLE "x""; drop role y; --" WITH LOGIN PASSWORD 'pa''ss'`
	if got != want {
		t.Fatalf("create:\n got %s\nwant %s", got, want)
	}
	got = pgSetPasswordStatement(role, pw, false)
	want = `ALTER ROLE "x""; drop role y; --" WITH PASSWORD 'pa''ss'`
	if got != want {
		t.Fatalf("alter:\n got %s\nwant %s", got, want)
	}
}

func TestSqliteLiteral(t *testing.T) {
	got, err := sqliteLiteral(`x'; DELETE FROM pgsql_users; --\`)
	if err != nil {
		t.Fatal(err)
	}
	// Only the quote is doubled; SQLite has no backslash escapes.
	if want := `'x''; DELETE FROM pgsql_users; --\'`; got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
	if _, err := sqliteLiteral("a\x00b"); err == nil {
		t.Fatal("want an error for a NUL byte")
	}
}

func TestProxyQueriesQuoteEveryValue(t *testing.T) {
	u := proxyUser{Username: "o'brien", Password: "p'w", DefaultHostgroup: 3, Comment: "c'm"}
	ins, err := proxyPutUserQuery(u, false)
	if err != nil {
		t.Fatal(err)
	}
	if want := `INSERT INTO pgsql_users (username, password, default_hostgroup, active, use_ssl, comment) VALUES ('o''brien', 'p''w', 3, 1, 0, 'c''m')`; ins != want {
		t.Fatalf("insert:\n got %s\nwant %s", ins, want)
	}
	upd, _ := proxyPutUserQuery(u, true)
	if want := `UPDATE pgsql_users SET password = 'p''w', default_hostgroup = 3, comment = 'c''m' WHERE username = 'o''brien'`; upd != want {
		t.Fatalf("update:\n got %s\nwant %s", upd, want)
	}
	get, _ := proxyGetUserQuery("o'brien")
	if want := `SELECT password, default_hostgroup, comment FROM pgsql_users WHERE username = 'o''brien' LIMIT 1`; get != want {
		t.Fatalf("get:\n got %s\nwant %s", get, want)
	}
}

func TestEnsureRolePasswordRefusesBadNamesBeforeTouchingAnything(t *testing.T) {
	proxy := newFakeProxy()
	// A nil *sql.DB would panic if used: the name check must come first.
	if _, err := EnsureRolePassword(nil, proxy, "reader,token", "h", 1, time.Now(), time.Hour); err == nil {
		t.Fatal("want an error")
	}
	if proxy.puts != 0 {
		t.Fatal("ProxySQL was written")
	}
}

// fakeProxy is pgsql_users in memory.
type fakeProxy struct {
	users   map[string]proxyUser
	puts    int
	failPut map[string]bool
}

func newFakeProxy() *fakeProxy {
	return &fakeProxy{users: map[string]proxyUser{}, failPut: map[string]bool{}}
}

func (f *fakeProxy) Get(role string) (proxyUser, bool, error) {
	u, ok := f.users[role]
	return u, ok, nil
}

func (f *fakeProxy) Put(u proxyUser) error {
	if f.failPut[u.Username] {
		return errFakePut
	}
	f.puts++
	f.users[u.Username] = u
	return nil
}

func (f *fakeProxy) List() ([]proxyUser, error) {
	var out []proxyUser
	for _, u := range f.users {
		out = append(out, u)
	}
	return out, nil
}

var errFakePut = &fakeError{"fake ProxySQL write failure"}

type fakeError struct{ s string }

func (e *fakeError) Error() string { return e.s }
