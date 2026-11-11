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

// excludedAccount is whether user stays out of AuthNull on this target: the engine's built-in
// admin, the account the agent itself logs in as (config.User), and MySQL's own system accounts.
func excludedAccount(config DBConfig, user string) bool {
	if builtinAdmin(config, user) {
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
