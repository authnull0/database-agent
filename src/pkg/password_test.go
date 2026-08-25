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
