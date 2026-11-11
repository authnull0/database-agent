package pkg

import (
	"database/sql"
	"fmt"
	"log"
	"math/big"
	"net"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/lib/pq"
)

// Direct access to the database (DB-MFA-HARDENING S1).
//
// MFA happens at ProxySQL. A client that reaches Postgres directly as a managed role, with its
// password, never meets it. S4 made that password long, random, rotated and known only to
// ProxySQL, but Postgres itself must also refuse the managed roles from anywhere but the proxy --
// and only pg_hba.conf on the database server can say that.
//
// Two parts:
//
//   - Every managed role (the policy roles ProxySQL logs in as, and the restricted an_ roles) is a
//     member of ManagedGroup, so three pg_hba.conf lines cover all of them, now and later:
//
//     host  all  +authnull_managed  <proxy-ip>/32  scram-sha-256
//     host  all  +authnull_managed  0.0.0.0/0      reject
//     host  all  +authnull_managed  ::/0           reject
//
//     placed above every other host line (pg_hba.conf is first match).
//
//   - Every sync, the agent reads the server's rules (pg_hba_file_rules) and works out, for each
//     managed role and database, whether any address other than the proxy's gets a rule that does
//     not reject it. The verdict goes to the console with the agent's last-active report.
//
// Out of scope: the database server itself. Unix sockets, loopback and the server's own address
// reach Postgres only from that machine, whose owner can read the data files anyway.

// ManagedGroup holds every role the agent manages. It cannot log in and holds no privileges.
const ManagedGroup = "authnull_managed"

// Direct-access verdicts.
const (
	DirectAccessProtected = "protected" // only the proxy can reach the managed roles
	DirectAccessExposed   = "exposed"   // some other address can try to log in as one
	DirectAccessUnknown   = "unknown"   // the agent could not tell
)

// DirectAccessReport is what the agent says about direct access, sent with its last-active call.
type DirectAccessReport struct {
	Status    string    `json:"status"`
	Detail    string    `json:"detail"`
	CheckedAt time.Time `json:"checkedAt"`
}

// hbaRule is one row of pg_hba_file_rules.
type hbaRule struct {
	Line      int
	Type      string
	Databases []string
	Users     []string
	Address   string
	Netmask   string
	Method    string
	Error     string
}

// hostTypes are the rule types a TCP connection can match. A client picks SSL or not, and GSS
// encryption or not, so it can reach any of them.
var hostTypes = map[string]bool{"host": true, "hostssl": true, "hostnossl": true, "hostgssenc": true, "hostnogssenc": true}

// localNet says whether an address is the database server itself: loopback, or one of its own.
type localNet func(net.IP) bool

// ruleNet is a rule's address range, or nil for "all", or ok=false for one the agent cannot
// evaluate (a host name, samehost, samenet).
func ruleNet(r hbaRule) (n *net.IPNet, all bool, ok bool) {
	switch strings.ToLower(r.Address) {
	case "all":
		return nil, true, true
	case "samehost":
		// The server's own addresses: the database server itself, out of scope.
		return nil, false, true
	case "", "samenet":
		return nil, false, false
	}
	ip := net.ParseIP(r.Address)
	if ip == nil {
		return nil, false, false // a host name
	}
	mask := net.ParseIP(r.Netmask)
	if mask == nil {
		return nil, false, false
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
		mask = mask.To4()
	}
	if mask == nil {
		return nil, false, false
	}
	m := net.IPMask(mask)
	return &net.IPNet{IP: ip.Mask(m), Mask: m}, false, true
}

// probes are a few addresses in n that are neither the proxy nor the database server: the start,
// the end and the middle of the range, so a narrower rule above it (a reject for part of it) does
// not hide the rest. None when n holds no such address (a /32 for the proxy, loopback).
func probes(n *net.IPNet, proxy net.IP, local localNet) []net.IP {
	ones, bits := n.Mask.Size()
	size := new(big.Int).Lsh(big.NewInt(1), uint(bits-ones))
	base := new(big.Int).SetBytes(n.IP)
	last := new(big.Int).Sub(size, big.NewInt(1))
	mid := new(big.Int).Rsh(size, 1)
	offsets := []*big.Int{big.NewInt(1), new(big.Int).Sub(last, big.NewInt(1)), new(big.Int).Add(mid, big.NewInt(1)), big.NewInt(0), last}
	var out []net.IP
	seen := map[string]bool{}
	for _, off := range offsets {
		if off.Sign() < 0 || off.Cmp(size) >= 0 {
			continue
		}
		v := new(big.Int).Add(base, off).Bytes()
		ip := make(net.IP, len(n.IP))
		copy(ip[len(ip)-len(v):], v)
		// Not the proxy, not the server, and a host address: never the unspecified or the
		// broadcast address.
		if ip.Equal(proxy) || local(ip) || seen[ip.String()] || ip.IsUnspecified() || ip.Equal(net.IPv4bcast.To4()) {
			continue
		}
		seen[ip.String()] = true
		out = append(out, ip)
	}
	return out
}

// publicProbe stands for "any address" in a rule whose address is all.
var publicProbe = net.ParseIP("203.0.113.7").To4()

// userMatches says whether a rule's user list covers role, given the groups role is a member of.
func userMatches(users []string, role string, groups map[string]bool) bool {
	for _, u := range users {
		switch {
		case u == "all" || u == role:
			return true
		case strings.HasPrefix(u, "+") && groups[u[1:]]:
			return true
		case strings.HasPrefix(u, "/"):
			if re, err := regexp.Compile(u[1:]); err == nil && re.MatchString(role) {
				return true
			}
		}
	}
	return false
}

// databaseMatches says whether a rule's database list covers db for role. "replication" is not a
// database a client can query through.
func databaseMatches(dbs []string, db, role string, groups map[string]bool) bool {
	for _, d := range dbs {
		switch {
		case d == "all" || d == db:
			return true
		case d == "sameuser" && db == role:
			return true
		case d == "samerole" && (db == role || groups[db]):
			return true
		case strings.HasPrefix(d, "/"):
			if re, err := regexp.Compile(d[1:]); err == nil && re.MatchString(db) {
				return true
			}
		}
	}
	return false
}

// addressMatches says whether rule r covers the client address ip.
func addressMatches(r hbaRule, ip net.IP, local localNet) bool {
	n, all, ok := ruleNet(r)
	if !ok {
		return false
	}
	if all {
		return true
	}
	if n == nil { // samehost
		return local(ip)
	}
	return n.Contains(ip)
}

// managedRole is a role the agent manages and the groups it is a member of (directly or not).
type managedRole struct {
	Name   string
	Groups map[string]bool
}

// directAccessFindings works out who besides the proxy can try to log in as a managed role.
// exposed names each rule that lets someone; unchecked, rules the agent cannot evaluate that
// might; warnings, problems that do not expose anything (a rule with an error, a role the proxy
// itself cannot reach). Empty exposed and unchecked mean only the proxy can.
func directAccessFindings(rules []hbaRule, roles []managedRole, dbs []string, proxy net.IP, local localNet) (exposed, unchecked, warnings []string) {
	// The addresses to try: for every rule, one address in its range that is neither the proxy nor
	// the server. First-match decides each, so one per range is enough to find every rule that is
	// reached before a reject.
	var candidates []net.IP
	seen := map[string]bool{}
	add := func(ips ...net.IP) {
		for _, ip := range ips {
			if ip != nil && !seen[ip.String()] {
				seen[ip.String()] = true
				candidates = append(candidates, ip)
			}
		}
	}
	for _, r := range rules {
		if !hostTypes[r.Type] || r.Error != "" {
			continue
		}
		n, all, ok := ruleNet(r)
		switch {
		case all:
			add(publicProbe)
		case ok && n != nil:
			add(probes(n, proxy, local)...)
		case !ok:
			if strings.EqualFold(r.Method, "reject") {
				continue
			}
			for _, m := range roles {
				if userMatches(r.Users, m.Name, m.Groups) {
					unchecked = append(unchecked, fmt.Sprintf("pg_hba.conf line %d (%s %s) cannot be checked by the agent and admits %s",
						r.Line, r.Type, r.Address, m.Name))
					break
				}
			}
		}
	}

	found := map[string]bool{}
	for _, m := range roles {
		for _, db := range dbs {
			for _, ip := range candidates {
				for _, r := range rules {
					if !hostTypes[r.Type] || r.Error != "" {
						continue
					}
					if !userMatches(r.Users, m.Name, m.Groups) || !databaseMatches(r.Databases, db, m.Name, m.Groups) ||
						!addressMatches(r, ip, local) {
						continue
					}
					// The first rule that matches decides.
					// One finding per rule, role and database, with the first address that reaches it.
					key := fmt.Sprintf("%d|%s|%s", r.Line, m.Name, db)
					if !strings.EqualFold(r.Method, "reject") && !found[key] {
						found[key] = true
						how := "with its password"
						if strings.EqualFold(r.Method, "trust") {
							how = "WITHOUT A PASSWORD (trust)"
						}
						exposed = append(exposed, fmt.Sprintf("pg_hba.conf line %d (%s %v %v %s/%s %s) lets %s log in to %s from %s (for example) %s, without ProxySQL",
							r.Line, r.Type, r.Databases, r.Users, r.Address, r.Netmask, r.Method, m.Name, db, ip, how))
					}
					break
				}
			}
		}
	}
	// The proxy itself must still get in, to at least one database. Not to every one: a role is
	// usually meant for one database, and pg_hba.conf may say so.
	for _, m := range roles {
		admitted := false
		for _, db := range dbs {
			for _, r := range rules {
				if hostTypes[r.Type] && r.Error == "" && userMatches(r.Users, m.Name, m.Groups) &&
					databaseMatches(r.Databases, db, m.Name, m.Groups) && addressMatches(r, proxy, local) {
					admitted = admitted || !strings.EqualFold(r.Method, "reject")
					break
				}
			}
		}
		if !admitted && len(dbs) > 0 {
			warnings = append(warnings, fmt.Sprintf("no pg_hba.conf rule admits %s from the proxy (%s) to any database: ProxySQL cannot log in as it", m.Name, proxy))
		}
	}
	for _, r := range rules {
		if r.Error != "" {
			warnings = append(warnings, fmt.Sprintf("pg_hba.conf line %d has an error: %s", r.Line, r.Error))
		}
	}
	sort.Strings(exposed)
	return exposed, unchecked, warnings
}

// HbaLines is what pg_hba.conf needs, at the top, for the proxy at proxyIP.
func HbaLines(proxyIP string) string {
	mask := "/32"
	if ip := net.ParseIP(proxyIP); ip != nil && ip.To4() == nil {
		mask = "/128"
	}
	return fmt.Sprintf("host  all  +%s  %s%s  scram-sha-256\nhost  all  +%s  0.0.0.0/0  reject\nhost  all  +%s  ::/0  reject",
		ManagedGroup, proxyIP, mask, ManagedGroup, ManagedGroup)
}

// --- reading the server ---

// ensureManagedGroup makes ManagedGroup if it is missing and puts role in it. Idempotent.
func ensureManagedGroup(db *sql.DB, role string) error {
	var exists bool
	if err := db.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", ManagedGroup).Scan(&exists); err != nil {
		return fmt.Errorf("look up role %s: %w", ManagedGroup, err)
	}
	if !exists {
		if _, err := db.Exec("CREATE ROLE " + pq.QuoteIdentifier(ManagedGroup) + " NOLOGIN"); err != nil {
			return fmt.Errorf("create role %s: %w", ManagedGroup, err)
		}
	}
	if role == "" {
		return nil
	}
	var member bool
	if err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_auth_members m
		JOIN pg_roles g ON g.oid = m.roleid JOIN pg_roles r ON r.oid = m.member
		WHERE g.rolname = $1 AND r.rolname = $2)`, ManagedGroup, role).Scan(&member); err != nil {
		return fmt.Errorf("membership of %s: %w", role, err)
	}
	if member {
		return nil
	}
	if _, err := db.Exec("GRANT " + pq.QuoteIdentifier(ManagedGroup) + " TO " + pq.QuoteIdentifier(role)); err != nil {
		return fmt.Errorf("add %s to %s: %w", role, ManagedGroup, err)
	}
	return nil
}

// readHbaRules reads pg_hba_file_rules: what pg_hba.conf says now (the server applies it on reload).
func readHbaRules(db *sql.DB) ([]hbaRule, error) {
	rows, err := db.Query(`SELECT line_number, coalesce(type, ''), coalesce(database, '{}'), coalesce(user_name, '{}'),
		coalesce(address, ''), coalesce(netmask, ''), coalesce(auth_method, ''), coalesce(error, '')
		FROM pg_hba_file_rules ORDER BY line_number`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []hbaRule
	for rows.Next() {
		var r hbaRule
		var dbs, users pq.StringArray
		if err := rows.Scan(&r.Line, &r.Type, &dbs, &users, &r.Address, &r.Netmask, &r.Method, &r.Error); err != nil {
			return nil, err
		}
		r.Databases, r.Users = dbs, users
		out = append(out, r)
	}
	return out, rows.Err()
}

// roleGroups is every role name is a member of, directly or not.
func roleGroups(db *sql.DB, name string) (map[string]bool, error) {
	rows, err := db.Query(`WITH RECURSIVE g(oid) AS (
		  SELECT m.roleid FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.member WHERE r.rolname = $1
		  UNION SELECT m.roleid FROM pg_auth_members m JOIN g ON m.member = g.oid)
		SELECT rolname FROM pg_roles WHERE oid IN (SELECT oid FROM g)`, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out[n] = true
	}
	return out, rows.Err()
}

// serverLocal is a localNet for the database server: loopback, and the server's own address as
// the agent reaches it.
func serverLocal(serverAddr string) localNet {
	own := net.ParseIP(serverAddr)
	return func(ip net.IP) bool { return ip.IsLoopback() || (own != nil && ip.Equal(own)) }
}

// proxyAddress is the address the database server sees the proxy at: the agent runs on the proxy's
// host and reaches the same server, so its own connection's client address.
func proxyAddress(db *sql.DB) (net.IP, error) {
	var addr sql.NullString
	if err := db.QueryRow("SELECT host(inet_client_addr())").Scan(&addr); err != nil {
		return nil, err
	}
	if !addr.Valid || addr.String == "" {
		return net.ParseIP("127.0.0.1"), nil // a Unix socket: the proxy is on the server itself
	}
	ip := net.ParseIP(addr.String)
	if ip == nil {
		return nil, fmt.Errorf("unreadable client address %q", addr.String)
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	return ip, nil
}

// CheckDirectAccess puts every managed role on this host into ManagedGroup and says whether the
// server lets any of them in from anywhere but the proxy. databases are the ones the agent syncs.
func CheckDirectAccess(pg *sql.DB, proxy proxyUsers, config DBConfig, databases []string, now time.Time) DirectAccessReport {
	rep := DirectAccessReport{Status: DirectAccessUnknown, CheckedAt: now.UTC()}
	users, err := proxy.List()
	if err != nil {
		rep.Detail = "could not read ProxySQL's users: " + err.Error()
		return rep
	}
	var roles []managedRole
	for _, u := range users {
		h, _, ok := parseRotationComment(u.Comment)
		if !ok || h != config.Host {
			continue
		}
		if err := ensureManagedGroup(pg, u.Username); err != nil {
			log.Printf("Direct access: %v", err)
		}
		groups, err := roleGroups(pg, u.Username)
		if err != nil {
			rep.Detail = fmt.Sprintf("could not read the groups of %s: %v", u.Username, err)
			return rep
		}
		roles = append(roles, managedRole{Name: u.Username, Groups: groups})
	}
	if len(roles) == 0 {
		rep.Detail = "no managed roles on this server yet"
		return rep
	}
	proxyIP, err := proxyAddress(pg)
	if err != nil {
		rep.Detail = "could not tell the proxy's address: " + err.Error()
		return rep
	}
	rules, err := readHbaRules(pg)
	if err != nil {
		rep.Detail = fmt.Sprintf("the agent's database account cannot read pg_hba_file_rules (%v). Grant it with: "+
			"GRANT SELECT ON pg_hba_file_rules TO %s; GRANT EXECUTE ON FUNCTION pg_hba_file_rules() TO %s;",
			err, pq.QuoteIdentifier(config.User), pq.QuoteIdentifier(config.User))
		return rep
	}
	exposed, unchecked, warnings := directAccessFindings(rules, roles, databases, proxyIP, serverLocal(config.Host))
	fix := "Put these lines at the top of pg_hba.conf on the database server, then reload Postgres:\n" + HbaLines(proxyIP.String())
	var lines []string
	switch {
	case len(exposed) > 0:
		rep.Status = DirectAccessExposed
		lines = append(append(append(lines, exposed...), unchecked...), warnings...)
		lines = append(lines, fix)
	case len(unchecked) > 0:
		rep.Status = DirectAccessUnknown
		lines = append(append(append(lines, unchecked...), warnings...), fix)
	default:
		rep.Status = DirectAccessProtected
		lines = append(append(lines, fmt.Sprintf("only the proxy (%s) can log in as the %d managed role(s)", proxyIP, len(roles))), warnings...)
	}
	rep.Detail = strings.Join(lines, "\n")
	return rep
}

// CheckDirectAccessForHost is CheckDirectAccess with the agent's own ProxySQL connection, logged.
func CheckDirectAccessForHost(pg *sql.DB, config DBConfig, databases []string) *DirectAccessReport {
	proxyDB, err := ConnectToProxysqlDB(config)
	if err != nil {
		rep := DirectAccessReport{Status: DirectAccessUnknown, Detail: "could not reach ProxySQL: " + err.Error(), CheckedAt: time.Now().UTC()}
		return &rep
	}
	defer proxyDB.Close()
	rep := CheckDirectAccess(pg, newAdminProxy(proxyDB, config), config, databases, time.Now())
	// Whoever can read ProxySQL's admin interface has every managed role's password.
	rep = withAdminExposure(rep, adminExposure(readAdminIfaces(proxyDB), usingDefaultAdminPassword(config)))
	switch rep.Status {
	case DirectAccessProtected:
		logDebugf("Direct access to %s: %s", config.Host, rep.Detail)
	default:
		log.Printf("WARNING direct access to %s is %s:\n%s", config.Host, rep.Status, rep.Detail)
	}
	return &rep
}
