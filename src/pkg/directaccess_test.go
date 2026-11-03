package pkg

import (
	"net"
	"strings"
	"testing"
)

// S1: who besides the proxy can log in as a managed role, worked out from pg_hba.conf.

var (
	testProxy = net.ParseIP("192.168.122.15").To4()
	testLocal = serverLocal("192.168.122.175")
	reader    = managedRole{Name: "dbmfa_reader", Groups: map[string]bool{ManagedGroup: true}}
	dbs       = []string{"dbmfa_test", "salesdb"}
)

func host(line int, db, user, addr, mask, method string) hbaRule {
	return hbaRule{Line: line, Type: "host", Databases: strings.Split(db, ","), Users: strings.Split(user, ","), Address: addr, Netmask: mask, Method: method}
}

// The lab's pg_hba.conf, as pg_hba_file_rules shows it.
func labRules() []hbaRule {
	return []hbaRule{
		{Line: 90, Type: "local", Databases: []string{"all"}, Users: []string{"postgres"}, Method: "peer"},
		{Line: 95, Type: "local", Databases: []string{"all"}, Users: []string{"all"}, Method: "peer"},
		host(97, "all", "all", "127.0.0.1", "255.255.255.255", "scram-sha-256"),
		host(99, "all", "all", "::1", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "scram-sha-256"),
		host(103, "replication", "all", "127.0.0.1", "255.255.255.255", "scram-sha-256"),
		host(105, "all", "all", "192.168.122.175", "255.255.255.255", "md5"),
		host(106, "dbmfa_test", "dbmfa_admin,dbmfa_writer,dbmfa_reader", "192.168.122.15", "255.255.255.255", "scram-sha-256"),
		host(108, "dbmfa_test", "+authnull_backend_roles", "192.168.122.15", "255.255.255.255", "scram-sha-256"),
	}
}

func recommended() []hbaRule {
	return []hbaRule{
		host(1, "all", "+"+ManagedGroup, "192.168.122.15", "255.255.255.255", "scram-sha-256"),
		host(2, "all", "+"+ManagedGroup, "0.0.0.0", "0.0.0.0", "reject"),
		host(3, "all", "+"+ManagedGroup, "::", "::", "reject"),
	}
}

func findings(rules []hbaRule, roles ...managedRole) (exposed, unchecked, warnings []string) {
	return directAccessFindings(rules, roles, dbs, testProxy, testLocal)
}

// The lab admits the managed roles from the proxy only; loopback and the server's own address
// are the server itself.
func TestLabIsProtected(t *testing.T) {
	exposed, unchecked, warnings := findings(labRules(), reader)
	if len(exposed)+len(unchecked)+len(warnings) != 0 {
		t.Fatalf("lab: exposed %v unchecked %v warnings %v", exposed, unchecked, warnings)
	}
}

// A catch-all line, as many installs have, lets anyone with the password in.
func TestCatchAllIsExposed(t *testing.T) {
	rules := append(labRules(), host(110, "all", "all", "0.0.0.0", "0.0.0.0", "scram-sha-256"))
	exposed, _, _ := findings(rules, reader)
	if len(exposed) == 0 || !strings.Contains(exposed[0], "line 110") || !strings.Contains(exposed[0], "dbmfa_reader") {
		t.Fatalf("catch-all not reported: %v", exposed)
	}
	// "all" as the address is the same thing.
	exposed, _, _ = findings([]hbaRule{{Line: 7, Type: "hostssl", Databases: []string{"all"}, Users: []string{"all"}, Address: "all", Method: "md5"}}, reader)
	if len(exposed) == 0 {
		t.Fatal("address all not reported")
	}
}

func TestTrustIsCalledOut(t *testing.T) {
	exposed, _, _ := findings([]hbaRule{host(5, "all", "all", "10.0.0.0", "255.0.0.0", "trust")}, reader)
	if len(exposed) == 0 || !strings.Contains(exposed[0], "WITHOUT A PASSWORD") {
		t.Fatalf("trust: %v", exposed)
	}
}

// The recommended lines, above everything else, protect whatever comes after them.
func TestRecommendedLinesProtect(t *testing.T) {
	rules := append(recommended(), host(110, "all", "all", "0.0.0.0", "0.0.0.0", "trust"), host(111, "all", "all", "::", "::", "md5"))
	exposed, unchecked, warnings := findings(rules, reader)
	if len(exposed)+len(unchecked)+len(warnings) != 0 {
		t.Fatalf("recommended: exposed %v unchecked %v warnings %v", exposed, unchecked, warnings)
	}
	// ...but not when a broader line comes first: first match wins.
	rules = append([]hbaRule{host(0, "all", "all", "10.0.0.0", "255.0.0.0", "md5")}, recommended()...)
	if exposed, _, _ := findings(rules, reader); len(exposed) == 0 {
		t.Fatal("a broad line above the recommended ones was not reported")
	}
	// And a role outside the managed group is not covered by them.
	stray := managedRole{Name: "legacy", Groups: map[string]bool{}}
	rules = append(recommended(), host(110, "all", "all", "0.0.0.0", "0.0.0.0", "md5"))
	if exposed, _, _ := findings(rules, stray); len(exposed) == 0 {
		t.Fatal("a managed role outside the group was reported protected")
	}
}

// A reject for part of a range does not hide the rest of it.
func TestNarrowRejectDoesNotHideTheRange(t *testing.T) {
	rules := []hbaRule{
		host(1, "all", "all", "10.0.0.0", "255.255.255.0", "reject"),
		host(2, "all", "all", "10.0.0.0", "255.0.0.0", "md5"),
	}
	if exposed, _, _ := findings(rules, reader); len(exposed) == 0 {
		t.Fatal("10.1.x.x exposure hidden by a reject for 10.0.0.x")
	}
}

// Matching follows Postgres: user lists, +groups, regular expressions, database keywords.
func TestMatching(t *testing.T) {
	for name, tc := range map[string]struct {
		rule hbaRule
		want bool
	}{
		"named":                {host(1, "dbmfa_test", "dbmfa_reader", "10.0.0.0", "255.0.0.0", "md5"), true},
		"another role":         {host(1, "all", "someone_else", "10.0.0.0", "255.0.0.0", "md5"), false},
		"member of the group":  {host(1, "all", "+"+ManagedGroup, "10.0.0.0", "255.0.0.0", "md5"), true},
		"not a member":         {host(1, "all", "+admins", "10.0.0.0", "255.0.0.0", "md5"), false},
		"regex":                {host(1, "all", "/^dbmfa_", "10.0.0.0", "255.0.0.0", "md5"), true},
		"regex, no match":      {host(1, "all", "/^app_", "10.0.0.0", "255.0.0.0", "md5"), false},
		"another database":     {host(1, "otherdb", "all", "10.0.0.0", "255.0.0.0", "md5"), false},
		"replication only":     {host(1, "replication", "all", "10.0.0.0", "255.0.0.0", "md5"), false},
		"only the proxy":       {host(1, "all", "all", "192.168.122.15", "255.255.255.255", "md5"), false},
		"only the server":      {host(1, "all", "all", "192.168.122.175", "255.255.255.255", "trust"), false},
		"loopback":             {host(1, "all", "all", "127.0.0.0", "255.0.0.0", "trust"), false},
		"samehost":             {hbaRule{Line: 1, Type: "host", Databases: []string{"all"}, Users: []string{"all"}, Address: "samehost", Method: "trust"}, false},
		"a local socket":       {hbaRule{Line: 1, Type: "local", Databases: []string{"all"}, Users: []string{"all"}, Method: "trust"}, false},
		"the proxy's subnet":   {host(1, "all", "all", "192.168.122.0", "255.255.255.0", "md5"), true},
		"IPv6":                 {host(1, "all", "all", "fd00::", "ffff:ff00::", "md5"), true},
		"a rule with an error": {hbaRule{Line: 1, Type: "host", Databases: []string{"all"}, Users: []string{"all"}, Address: "0.0.0.0", Netmask: "0.0.0.0", Method: "md5", Error: "bad"}, false},
	} {
		exposed, _, _ := findings([]hbaRule{tc.rule}, reader)
		if (len(exposed) > 0) != tc.want {
			t.Errorf("%s: exposed %v, want %v", name, exposed, tc.want)
		}
	}
}

// A host name or samenet can't be evaluated: reported as unchecked, not as protected.
func TestUncheckableRules(t *testing.T) {
	for _, addr := range []string{"app.example.com", "samenet"} {
		r := hbaRule{Line: 9, Type: "host", Databases: []string{"all"}, Users: []string{"all"}, Address: addr, Method: "md5"}
		_, unchecked, _ := findings([]hbaRule{r}, reader)
		if len(unchecked) == 0 {
			t.Errorf("%s: not reported as unchecked", addr)
		}
		r.Method = "reject"
		if _, unchecked, _ := findings([]hbaRule{r}, reader); len(unchecked) != 0 {
			t.Errorf("%s reject: reported %v", addr, unchecked)
		}
	}
}

// Problems that expose nothing are warnings: the proxy itself locked out, a broken line.
func TestWarnings(t *testing.T) {
	rules := []hbaRule{host(1, "all", "+"+ManagedGroup, "0.0.0.0", "0.0.0.0", "reject")}
	exposed, _, warnings := findings(rules, reader)
	if len(exposed) != 0 || len(warnings) != 1 || !strings.Contains(warnings[0], "ProxySQL cannot log in") {
		t.Fatalf("proxy locked out: exposed %v warnings %v", exposed, warnings)
	}
	rules = append(labRules(), hbaRule{Line: 200, Type: "host", Error: "invalid IP address"})
	if _, _, warnings := findings(rules, reader); len(warnings) != 1 || !strings.Contains(warnings[0], "line 200") {
		t.Fatalf("broken line: %v", warnings)
	}
}

func TestHbaLines(t *testing.T) {
	want := "host  all  +authnull_managed  192.168.122.15/32  scram-sha-256\n" +
		"host  all  +authnull_managed  0.0.0.0/0  reject\n" +
		"host  all  +authnull_managed  ::/0  reject"
	if got := HbaLines("192.168.122.15"); got != want {
		t.Errorf("HbaLines:\n%s\nwant\n%s", got, want)
	}
	if got := HbaLines("fd00::15"); !strings.HasPrefix(got, "host  all  +authnull_managed  fd00::15/128 ") {
		t.Errorf("IPv6 proxy: %s", got)
	}
}

// One finding per rule, role and database, however many addresses reach it, and never with the
// unspecified or broadcast address as the example.
func TestOneFindingPerRuleRoleAndDatabase(t *testing.T) {
	rules := append(labRules(), host(109, "all", "all", "0.0.0.0", "0.0.0.0", "scram-sha-256"))
	writer := managedRole{Name: "dbmfa_writer", Groups: map[string]bool{ManagedGroup: true}}
	exposed, _, _ := findings(rules, reader, writer)
	if len(exposed) != 4 { // 2 roles x 2 databases
		t.Fatalf("want 4 findings, got %d: %v", len(exposed), exposed)
	}
	for _, e := range exposed {
		if strings.Contains(e, "from 0.0.0.0 ") || strings.Contains(e, "255.255.255.255") {
			t.Errorf("odd example address: %s", e)
		}
	}
}
