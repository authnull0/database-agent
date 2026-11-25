package pkg

import "strings"

// Accounts the agent never brings into AuthNull: a sync does not report them, so no policy can
// name them and the vault can neither rotate nor reveal them. Every other account, admins and
// superusers included, is reported.

// builtinAdmin is the engine's own admin account: postgres for PostgreSQL, root (any host part)
// for MySQL.
func builtinAdmin(config DBConfig, user string) bool {
	if isMySQL(config) {
		return user == "root"
	}
	return user == "postgres"
}

// healthCheckNames are the health check's database and login names: HEALTH_CHECK_NAMES,
// comma-separated, or "monitor" (the ProxySQL monitor's login and database) when unset.
func healthCheckNames(config DBConfig) []string {
	var out []string
	for _, n := range strings.Split(config.HealthCheckNames, ",") {
		if n = strings.TrimSpace(n); n != "" {
			out = append(out, n)
		}
	}
	if len(out) == 0 {
		return []string{"monitor"}
	}
	return out
}

// healthCheckName is whether name (a database or a login) is the health check's.
func healthCheckName(config DBConfig, name string) bool {
	for _, n := range healthCheckNames(config) {
		if strings.EqualFold(n, name) {
			return true
		}
	}
	return false
}

// excludedAccount is whether user stays out of AuthNull on this target: the engine's built-in
// admin, the account the agent itself logs in as (config.User), MySQL's own system accounts, and
// the health check's login (monitor; any host part on MySQL).
func excludedAccount(config DBConfig, user string) bool {
	if builtinAdmin(config, user) || healthCheckName(config, user) {
		return true
	}
	if own := strings.TrimSpace(config.User); own != "" && user == own {
		return true
	}
	return isMySQL(config) && mysqlSystemAccount(user)
}

// filterExcludedAccounts is users without the excluded ones, in order.
func filterExcludedAccounts(config DBConfig, users []databaseUser) []databaseUser {
	out := users[:0:0]
	for _, u := range users {
		if excludedAccount(config, u.Username) {
			logDebugf("Not reporting %s: excluded from AuthNull", u.Username)
			continue
		}
		out = append(out, u)
	}
	return out
}
