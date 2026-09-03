package pkg

import (
	mathRand "math/rand"
	"strings"
	"testing"
)

// The old implementation seeded math/rand from time.Now().UnixNano(), so two
// passwords generated inside the same second came from the same seed. These
// checks fail if anything reintroduces a seeded generator.
func TestGenerateRandomPassword(t *testing.T) {
	const n = 16

	// Same second, must differ.
	a, err := GenerateRandomPassword(n)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	b, err := GenerateRandomPassword(n)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if a == b {
		t.Errorf("two passwords in the same second are identical: %q", a)
	}
	if len(a) != n {
		t.Errorf("length = %d, want %d", len(a), n)
	}

	// Not reproducible from a seed: pinning math/rand must not pin the output.
	mathRand.Seed(1)
	c, _ := GenerateRandomPassword(n)
	mathRand.Seed(1)
	d, _ := GenerateRandomPassword(n)
	if c == d {
		t.Errorf("output is reproducible from a math/rand seed: %q", c)
	}

	// No character outside the declared charset.
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()_+[]{}|;:,.<>?"
	for _, r := range a {
		if !strings.ContainsRune(charset, r) {
			t.Errorf("character %q not in charset", r)
		}
	}

	if _, err := GenerateRandomPassword(0); err == nil {
		t.Error("length 0 should be an error")
	}
}

// A tripwire, not the control. The PostgreSQL DDL is quoted with pq and the
// ProxySQL admin statements use placeholders, so injection no longer depends on
// the charset -- but a quote or backslash appearing here would still be a
// surprise worth catching, and the check costs nothing.
func TestPasswordCharsetIsSQLSafe(t *testing.T) {
	const forbidden = `'"\`

	// Generate enough to make an accidental widening overwhelmingly likely to show.
	for i := 0; i < 200; i++ {
		p, err := GenerateRandomPassword(32)
		if err != nil {
			t.Fatalf("generate: %v", err)
		}
		if idx := strings.IndexAny(p, forbidden); idx >= 0 {
			t.Fatalf("password contains SQL-unsafe character %q -- it is interpolated "+
				"unparameterised into ALTER ROLE and an UPDATE on pgsql_users: %q",
				p[idx], p)
		}
	}
}
