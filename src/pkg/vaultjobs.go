package pkg

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

// Role passwords held in the vault (authnull-service docs/DB-PASSWORD-VAULT.md).
//
// The backend generates a managed role's password and keeps it in its vault; this agent applies it.
// On every sync it claims the jobs for its database server, fetches each password, sets it on the
// database and then in ProxySQL, and reports. Only an agent with its install's credential asks:
// the backend gives passwords to nothing else.
//
// A role applied this way is marked in pgsql_users ("authnull:vault;host=…;rotated=…"), and the
// agent's own rotation (S4) leaves it alone from then on: the sweep skips it, and checkout returns
// its password without rotating. The vault's password is the one the backend can reveal.

// vaultComment marks a pgsql_users row whose password the vault holds.
func vaultComment(host string, t time.Time) string {
	return fmt.Sprintf("%svault;host=%s;rotated=%d", rotationCommentPrefix, host, t.Unix())
}

// isVaultManaged reports a row marked by vaultComment.
func isVaultManaged(comment string) bool {
	if !strings.HasPrefix(comment, rotationCommentPrefix) {
		return false
	}
	for _, field := range strings.Split(strings.TrimPrefix(comment, rotationCommentPrefix), ";") {
		if field == "vault" {
			return true
		}
	}
	return false
}

// hostgroups finds the ProxySQL hostgroup a database server is in, for a role that has no
// pgsql_users row yet (a customer's own role, taken over).
type hostgroups interface {
	HostgroupFor(host string) (int, bool, error)
}

// HostgroupFor reads the engine's servers through the admin interface: pgsql_servers by hostname,
// as always; mysql_servers by hostname and the target's port, so two MySQL servers on one IP are
// told apart.
func (a adminProxyUsers) HostgroupFor(host string) (int, bool, error) {
	lit, err := sqliteLiteral(host)
	if err != nil {
		return 0, false, err
	}
	where := "hostname = " + lit
	if a.engine == EngineMySQL && a.port > 0 {
		where += fmt.Sprintf(" AND port = %d", a.port)
	}
	var hg int
	err = a.db.QueryRow("SELECT hostgroup_id FROM " + a.tables().servers + " WHERE " + where + " ORDER BY hostgroup_id LIMIT 1").Scan(&hg)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return hg, true, nil
}

type vaultJob struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"`
	Host   string `json:"host"`
	Port   int    `json:"port"`
	DBType string `json:"dbType"`
	Role   string `json:"role"`
	// Payload: apply_role's rolePayload, drop_role's dropPayload (consolerole.go).
	Payload json.RawMessage `json:"payload,omitempty"`
}

// openRoleDatabase connects to one database for a console role's grants; swappable in tests.
var openRoleDatabase = openDatabase

func credentialConfigured() bool {
	installToken.Lock()
	defer installToken.Unlock()
	return installToken.clientID != ""
}

// postJSON sends one call to the control plane with the install's token and decodes the answer.
func postJSON(api, path string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", api+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := doAPI(&http.Client{Timeout: 30 * time.Second}, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", path, resp.Status)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// ProcessVaultJobs claims and applies this database server's password jobs, returning how many it
// applied. A job that fails is reported as failed; the backend keeps the current password.
func ProcessVaultJobs(pg *sql.DB, proxy proxyUsers, hg hostgroups, config DBConfig, now time.Time) (int, error) {
	if !credentialConfigured() {
		return 0, nil
	}
	if e := Engine(config.DBType); e != EnginePostgres && e != EngineMySQL {
		return 0, nil // the engines with job support (mysqlroles.go for MySQL)
	}
	api := strings.TrimRight(config.API, "/")
	host := config.Host
	if config.Port != "" {
		host += ":" + config.Port
	}
	var claimed struct {
		Jobs []vaultJob `json:"jobs"`
	}
	if err := postJSON(api, "/api/v1/databaseService/agent/jobs/claim", map[string]any{"hosts": []string{host}}, &claimed); err != nil {
		return 0, fmt.Errorf("claiming password jobs: %w", err)
	}
	applied := 0
	for _, j := range claimed.Jobs {
		ok, warn, err := runVaultJob(api, pg, proxy, hg, config, j, now)
		result := map[string]any{"jobId": j.ID, "ok": ok}
		switch {
		case err != nil:
			result["error"] = err.Error()
			log.Printf("Password job %s for role %q failed: %v", j.ID, j.Role, err)
		case warn != "":
			result["error"] = warn
			log.Printf("Password job %s for role %q: %s", j.ID, j.Role, warn)
		default:
			log.Printf("Set the vault's password for role %q on %s", j.Role, config.Host)
		}
		if ok {
			applied++
		}
		if err := postJSON(api, "/api/v1/databaseService/agent/jobs/result", result, nil); err != nil {
			log.Printf("Reporting password job %s: %v", j.ID, err)
		}
	}
	return applied, nil
}

// runVaultJob applies one job: the database first, then ProxySQL, so a failure can't leave
// ProxySQL with a password the database doesn't have. If the database changed but ProxySQL could
// not be written even on a retry, the job is still reported done with a warning: the database now
// has the vault's password, and the vault must keep it -- the next rotation repairs ProxySQL.
func runVaultJob(api string, pg *sql.DB, proxy proxyUsers, hg hostgroups, config DBConfig, j vaultJob, now time.Time) (ok bool, warn string, err error) {
	if j.Host != config.Host {
		return false, "", fmt.Errorf("host %s is not served by this agent (%s)", j.Host, config.Host)
	}
	// Never the account the agent itself logs in as: a password set on it here is one the agent
	// does not have, and it would lose the database. A policy can name it; the job is refused.
	if own := strings.TrimSpace(config.User); own != "" && j.Role == own {
		return false, "", fmt.Errorf("role %q is the account this agent logs in as; it is not changed by jobs", j.Role)
	}
	// Nor the engine's built-in admin (postgres, root): it is never brought into AuthNull, so a job
	// naming it comes from a stale or hand-made entry.
	if builtinAdmin(config, j.Role) {
		return false, "", fmt.Errorf("role %q is the database's built-in admin; AuthNull does not manage it", j.Role)
	}
	switch j.Kind {
	case "rotate_password":
		return runRotateJob(api, pg, proxy, hg, config, j, now)
	case "apply_role":
		return runApplyRoleJob(api, pg, proxy, hg, config, j, now)
	case "apply_policy_role":
		return runApplyPolicyRoleJob(pg, proxy, hg, config, j, now)
	case "drop_policy_role":
		var p dropPayload
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return false, "", fmt.Errorf("drop_policy_role payload: %w", err)
		}
		if isMySQL(config) {
			// DROP USER takes every grant with it, and a MySQL user owns nothing: no per-database step.
			state := func() (bool, bool, error) { return mysqlPolicyUserState(pg, j.Role) }
			if err := dropMySQLUser(pg, proxy, j.Role, state, "is not an AuthNull restricted role"); err != nil {
				return false, "", err
			}
			return true, "", nil
		}
		open := func(name string) (*sql.DB, error) { return openRoleDatabase(config, name) }
		state := func() (bool, bool, error) { return policyRoleState(pg, j.Role) }
		if err := dropOwnRole(pg, open, proxy, j.Role, p, state, "is not an AuthNull restricted role"); err != nil {
			return false, "", err
		}
		return true, "", nil
	case "drop_role":
		var p dropPayload
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return false, "", fmt.Errorf("drop_role payload: %w", err)
		}
		if isMySQL(config) {
			state := func() (bool, bool, error) { return mysqlUserState(pg, j.Role, mysqlConsoleMark) }
			if err := dropMySQLUser(pg, proxy, j.Role, state, "was not created by AuthNull"); err != nil {
				return false, "", err
			}
			return true, "", nil
		}
		open := func(name string) (*sql.DB, error) { return openRoleDatabase(config, name) }
		if err := dropConsoleRole(pg, open, proxy, j.Role, p); err != nil {
			return false, "", err
		}
		return true, "", nil
	}
	return false, "", fmt.Errorf("unknown job kind %q", j.Kind)
}

// runApplyRoleJob creates or updates a console role: its password from the job when it is new.
func runApplyRoleJob(api string, pg *sql.DB, proxy proxyUsers, hg hostgroups, config DBConfig, j vaultJob, now time.Time) (bool, string, error) {
	var p rolePayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return false, "", fmt.Errorf("apply_role payload: %w", err)
	}
	check, validName := checkRolePayload, ValidateRoleName
	if isMySQL(config) {
		check, validName = checkMySQLRolePayload, validateMySQLUserName
	}
	if err := check(p); err != nil {
		return false, "", err
	}
	if err := validName(j.Role); err != nil {
		return false, "", err
	}
	hostgroup, err := roleHostgroup(proxy, hg, config, j.Role)
	if err != nil {
		return false, "", err
	}
	password := ""
	if p.Create {
		var secret struct {
			Password string `json:"password"`
		}
		if err := postJSON(api, "/api/v1/databaseService/agent/jobs/secret", map[string]string{"jobId": j.ID}, &secret); err != nil {
			return false, "", fmt.Errorf("fetching the password: %w", err)
		}
		if secret.Password == "" {
			return false, "", errors.New("the control plane sent no password")
		}
		password = secret.Password
	}
	var warn string
	if isMySQL(config) {
		// MySQL's grants are the server's: no per-database connection.
		warn, err = applyMySQLConsoleUser(pg, proxy, managedHost(config), hostgroup, j.Role, p, password, now)
	} else {
		open := func(name string) (*sql.DB, error) { return openRoleDatabase(config, name) }
		warn, err = applyConsoleRole(pg, open, proxy, config.Host, hostgroup, j.Role, p, password, now)
	}
	if err != nil {
		if password != "" {
			err = errors.New(strings.ReplaceAll(err.Error(), password, "[redacted]"))
		}
		return false, "", err
	}
	return true, warn, nil
}

// policyRolePayload is an apply_policy_role job's: the policy's shape (authnull-service
// docs/DB-POLICY-ROLES.md).
type policyRolePayload struct {
	Database   string   `json:"database"`
	SourceRole string   `json:"sourceRole"`
	Privileges []string `json:"privileges"`
	Tables     []string `json:"tables"`
}

// runApplyPolicyRoleJob builds a policy's restricted role -- only under the name its shape gives,
// so a job cannot have the agent grant a policy's privileges to some other role.
func runApplyPolicyRoleJob(pg *sql.DB, proxy proxyUsers, hg hostgroups, config DBConfig, j vaultJob, now time.Time) (bool, string, error) {
	var p policyRolePayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return false, "", fmt.Errorf("apply_policy_role payload: %w", err)
	}
	grantable := grantableDatabase
	if isMySQL(config) {
		grantable = mysqlGrantableDatabase
	}
	if !grantable(p.Database) {
		return false, "", fmt.Errorf("database %q cannot be granted on", p.Database)
	}
	name, ok := BackendRoleName(p.Database, p.SourceRole, p.Privileges, p.Tables)
	if !ok {
		return false, "", fmt.Errorf("privileges %v get no restricted role", p.Privileges)
	}
	if name != j.Role {
		return false, "", fmt.Errorf("role %q does not match the policy's shape (%s)", j.Role, name)
	}
	if isMySQL(config) {
		// The source role's hostgroup: the restricted user stands in for it, on the same server.
		hostgroup, err := policyHostgroup(proxy, hg, config, j.Role, p.SourceRole)
		if err != nil {
			return false, "", err
		}
		if _, err := provisionMySQLPolicyUser(pg, proxy, managedHost(config), p.Database, p.SourceRole, p.Privileges, p.Tables,
			hostgroup, now, RotationInterval()); err != nil {
			return false, "", err
		}
		return true, "", nil
	}
	hostgroup, err := roleHostgroup(proxy, hg, config, j.Role)
	if err != nil {
		return false, "", err
	}
	target, err := openRoleDatabase(config, p.Database)
	if err != nil {
		return false, "", fmt.Errorf("connecting to %s: %w", p.Database, err)
	}
	defer target.Close()
	if _, err := ProvisionBackendRole(target, proxy, config.Host, p.Database, p.SourceRole, p.Privileges, p.Tables,
		hostgroup, now, RotationInterval()); err != nil {
		return false, "", err
	}
	return true, "", nil
}

// policyRoleState: does role exist, and is it one of AuthNull's restricted roles (an_..., in the
// restricted roles' group)?
func policyRoleState(server *sql.DB, role string) (exists, ours bool, err error) {
	if exists, err = pgRoleExists(server, role); err != nil || !exists {
		return exists, false, err
	}
	groups, err := roleGroups(server, role)
	if err != nil {
		return true, false, err
	}
	return true, strings.HasPrefix(role, "an_") && groups[BackendRolesGroup], nil
}

// policyHostgroup is a MySQL restricted user's hostgroup: its source role's row's, else its own
// row's, else the server's.
func policyHostgroup(proxy proxyUsers, hg hostgroups, config DBConfig, role, sourceRole string) (int, error) {
	if sourceRole != "" {
		src, found, err := proxy.Get(sourceRole)
		if err != nil {
			return 0, fmt.Errorf("reading ProxySQL user: %w", err)
		}
		if found {
			return src.DefaultHostgroup, nil
		}
	}
	return roleHostgroup(proxy, hg, config, role)
}

// roleHostgroup is the ProxySQL hostgroup a role's row should have: its own, or the server's.
func roleHostgroup(proxy proxyUsers, hg hostgroups, config DBConfig, role string) (int, error) {
	existing, found, err := proxy.Get(role)
	if err != nil {
		return 0, fmt.Errorf("reading ProxySQL user: %w", err)
	}
	if found {
		return existing.DefaultHostgroup, nil
	}
	g, ok, err := hg.HostgroupFor(config.Host)
	if err != nil {
		return 0, fmt.Errorf("finding the ProxySQL hostgroup: %w", err)
	}
	if !ok {
		return 0, fmt.Errorf("no ProxySQL hostgroup holds %s", config.Host)
	}
	return g, nil
}

// runRotateJob sets a managed role's password to the vault's.
// On a MySQL target the password is set on every host row of the user name (mysqlroles.go).
func runRotateJob(api string, pg *sql.DB, proxy proxyUsers, hg hostgroups, config DBConfig, j vaultJob, now time.Time) (ok bool, warn string, err error) {
	validName, roleExists := ValidateRoleName, pgRoleExists
	if isMySQL(config) {
		validName = validateMySQLUserName
		roleExists = func(db *sql.DB, user string) (bool, error) {
			hosts, err := mysqlUserHosts(db, user)
			return len(hosts) > 0, err
		}
	}
	if err := validName(j.Role); err != nil {
		return false, "", err
	}
	exists, err := roleExists(pg, j.Role)
	if err != nil {
		return false, "", fmt.Errorf("checking role: %w", err)
	}
	if !exists {
		return false, "", fmt.Errorf("role %q does not exist on %s", j.Role, config.Host)
	}
	existing, found, err := proxy.Get(j.Role)
	if err != nil {
		return false, "", fmt.Errorf("reading ProxySQL user: %w", err)
	}
	hostgroup := existing.DefaultHostgroup
	if !found {
		g, ok, err := hg.HostgroupFor(config.Host)
		if err != nil {
			return false, "", fmt.Errorf("finding the ProxySQL hostgroup: %w", err)
		}
		if !ok {
			return false, "", fmt.Errorf("no ProxySQL hostgroup holds %s", config.Host)
		}
		hostgroup = g
	}

	var secret struct {
		Password string `json:"password"`
	}
	if err := postJSON(api, "/api/v1/databaseService/agent/jobs/secret", map[string]string{"jobId": j.ID}, &secret); err != nil {
		return false, "", fmt.Errorf("fetching the password: %w", err)
	}
	if secret.Password == "" {
		return false, "", errors.New("the control plane sent no password")
	}

	var managedWarn string
	if isMySQL(config) {
		if err := mysqlSetPassword(pg, j.Role, secret.Password); err != nil {
			return false, "", fmt.Errorf("setting the user's password: %v", strings.ReplaceAll(err.Error(), secret.Password, "[redacted]"))
		}
	} else {
		if _, err := pg.Exec(pgSetPasswordStatement(j.Role, secret.Password, false)); err != nil {
			return false, "", fmt.Errorf("setting the role's password: %v", strings.ReplaceAll(err.Error(), secret.Password, "[redacted]"))
		}
		// A managed role logs in through the proxy only: pg_hba.conf admits the managed group from it
		// alone (directaccess.go). Not fatal -- the password is set; the direct-access report says so.
		if err := ensureManagedGroup(pg, j.Role); err != nil {
			managedWarn = fmt.Sprintf("the role is not in %s: %v", ManagedGroup, err)
		}
	}
	row := proxyUser{Username: j.Role, Password: secret.Password, DefaultHostgroup: hostgroup, Comment: vaultComment(managedHost(config), now)}
	if err := proxy.Put(row); err != nil {
		if err2 := proxy.Put(row); err2 != nil {
			return true, "the database has the new password, but ProxySQL could not be updated: " + err2.Error(), nil
		}
	}
	return true, managedWarn, nil
}

// ProcessVaultJobsForHost is ProcessVaultJobs with the agent's own ProxySQL connection.
func ProcessVaultJobsForHost(pg *sql.DB, config DBConfig, now time.Time) error {
	if !credentialConfigured() {
		return nil
	}
	proxyDB, err := ConnectToProxysqlDB(config)
	if err != nil {
		return err
	}
	defer proxyDB.Close()
	admin := newAdminProxy(proxyDB, config)
	_, err = ProcessVaultJobs(pg, admin, admin, config, now)
	return err
}
