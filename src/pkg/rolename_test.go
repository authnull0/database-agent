package pkg

import (
	"strings"
	"testing"

	"github.com/lib/pq"
)

// dbUserName arrives as policyJson.Database.User -- an administrator-typed
// database role name -- and reaches DDL on the customer's own PostgreSQL
// (ALTER ROLE / CREATE ROLE), which cannot take bind parameters. It is quoted
// with pq.QuoteIdentifier rather than interpolated raw.
//
// These are the payloads that would have escaped the old
//
//	fmt.Sprintf("ALTER ROLE %s WITH PASSWORD '%s'", dbUserName, password)
//
// and each must come back as a single quoted identifier instead.
func TestRoleNameQuotingNeutralisesInjection(t *testing.T) {
	payloads := []string{
		`app"; DROP DATABASE salesdb; --`,
		`app" WITH SUPERUSER; --`,
		`"`,
		`app""role`,
		`app\"; SELECT 1; --`,
	}

	for _, raw := range payloads {
		got := pq.QuoteIdentifier(raw)

		// A quoted identifier is "..." with every embedded " doubled, so the
		// statement can only ever see one identifier -- no statement break.
		if !strings.HasPrefix(got, `"`) || !strings.HasSuffix(got, `"`) {
			t.Errorf("QuoteIdentifier(%q) = %q, not wrapped in double quotes", raw, got)
			continue
		}
		inner := got[1 : len(got)-1]
		if strings.Contains(strings.ReplaceAll(inner, `""`, ""), `"`) {
			t.Errorf("QuoteIdentifier(%q) = %q leaves an unescaped quote", raw, got)
		}
	}
}

// The password goes into the same DDL as a literal.
func TestPasswordLiteralQuotingNeutralisesInjection(t *testing.T) {
	for _, raw := range []string{`p'; DROP ROLE app; --`, `'`, `p\'`} {
		got := pq.QuoteLiteral(raw)
		if !strings.HasPrefix(got, `'`) && !strings.HasPrefix(got, ` E'`) {
			t.Errorf("QuoteLiteral(%q) = %q, unexpected form", raw, got)
		}
		// Must not terminate early: a bare ' inside would end the literal.
		body := strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(got, " E"), "'"), "'")
		if strings.Contains(strings.ReplaceAll(body, `''`, ""), `'`) &&
			!strings.Contains(got, `\'`) {
			t.Errorf("QuoteLiteral(%q) = %q leaves an unescaped quote", raw, got)
		}
	}
}
