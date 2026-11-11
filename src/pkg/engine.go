package pkg

import (
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Database engines a target can be (DBConfig.DBType, from DB_TYPE or a target's type: in
// data-source.yaml). Everything engine-specific branches on Engine, never on the raw string.
const (
	EnginePostgres = "postgres"
	EngineMySQL    = "mysql"
)

// Engine normalises a configured database type: empty, postgres, postgresql and pg are PostgreSQL
// (the agent's original and default engine); mysql and mariadb are MySQL. Any other value comes
// back lower case, so the paths that serve only these two can refuse it.
func Engine(dbType string) string {
	switch t := strings.ToLower(strings.TrimSpace(dbType)); t {
	case "", "postgres", "postgresql", "pg":
		return EnginePostgres
	case "mysql", "mariadb":
		return EngineMySQL
	default:
		return t
	}
}

func isMySQL(config DBConfig) bool { return Engine(config.DBType) == EngineMySQL }

// defaultPort is the engine's usual port, for a target configured without one.
func defaultPort(engine string) int {
	if engine == EngineMySQL {
		return 3306
	}
	return 5432
}

// targetPort is the configured port, or the engine's default when it is missing or unreadable.
func targetPort(config DBConfig) int {
	if p, err := strconv.Atoi(strings.TrimSpace(config.Port)); err == nil && p > 0 {
		return p
	}
	return defaultPort(Engine(config.DBType))
}

// mysqlConnectTimeout bounds a connection attempt to a MySQL target.
const mysqlConnectTimeout = 10 * time.Second

// mysqlDSN is the go-sql-driver DSN for the target, as the agent, in database ("" for none). The
// driver's own formatter builds it, so nothing in a password can turn into DSN syntax.
func mysqlDSN(config DBConfig, database string) string {
	c := mysql.NewConfig()
	c.Net = "tcp"
	c.Addr = net.JoinHostPort(config.Host, strconv.Itoa(targetPort(config)))
	c.User = config.User
	c.Passwd = config.Password
	c.DBName = database
	c.ParseTime = true
	c.Timeout = mysqlConnectTimeout
	return c.FormatDSN()
}

// --- quoting for MySQL statements ---
//
// Account management and GRANT take no bind parameters, so names and passwords are quoted here.
// String literals double the quote and escape the backslash: correct with the default sql_mode,
// and still unable to end the literal under NO_BACKSLASH_ESCAPES.

// mysqlIdent quotes an identifier with backticks, doubling any backtick in it.
func mysqlIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// mysqlString quotes a string literal.
func mysqlString(s string) (string, error) {
	if strings.ContainsRune(s, 0) {
		return "", errors.New("value contains a NUL byte")
	}
	return "'" + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), "'", "''") + "'", nil
}

// mysqlAccount is the account 'user'@'host'.
func mysqlAccount(user, host string) (string, error) {
	u, err := mysqlString(user)
	if err != nil {
		return "", err
	}
	h, err := mysqlString(host)
	if err != nil {
		return "", err
	}
	return u + "@" + h, nil
}

// mysqlDBGrantTarget is `db`.* for a database-level GRANT or REVOKE. There _ and % are wildcards
// (GRANT ... ON `my_db`.* also covers myXdb), so they are escaped: the grant names this database
// alone. Table and routine grants name the database literally and do not need this.
func mysqlDBGrantTarget(database string) string {
	return mysqlIdent(mysqlDBPattern(database)) + ".*"
}

// mysqlDBPattern is database with the grant wildcards escaped, as mysql.db stores the grant.
func mysqlDBPattern(database string) string {
	return strings.NewReplacer(`_`, `\_`, `%`, `\%`).Replace(database)
}
