# database-agent

Agent binary Synchronize the Mysql Databases

It fetches Db status, Users roles and their privileges

command to build the agent


go build -o authnull-db-agent main.go


File Path for db.env 


/etc/authnull-db-agent/db.env


File Path for authnull-db-agent.log file


/var/log/authnull-db-agent.log


Command to run agent 

./authnull-db-agent -host "localhost" -username "root" -password "password"




## Identity

The agent authenticates to the control plane with its install's credential:
`PROXY_CLIENT_ID` and `PROXY_CLIENT_SECRET` in `db.env`, written by the console when it builds the
file. The agent exchanges them for a short-lived token at
`/api/v1/databaseService/proxy/token`, sends it on every call, and exchanges again before it
expires or after a 401. The secret and the token are never logged.

An agent without them still calls, with no credential, while the control plane's
`DB_AGENT_ALLOW_UNAUTHENTICATED` is on, which is the default during the move. Download a fresh
`db.env` for each install before that switch is turned off.

## Managed role passwords

ProxySQL is the only holder of a managed role's password: users log in with a console token, and
ProxySQL logs in to the database for them. The agent gives each role a 32-character password from
`crypto/rand` and rotates it:

- at checkout, when the password is due, the role is missing, or the row predates rotation;
- on every sync, for each managed role on that host whose password is due.

The interval is set per tenant in the AuthNull console (Databases settings: password rotation,
1 to 8760 hours, default 24). The agent learns it from the response to its last-active report on
every sync, and keeps the interval it has when a report fails or the answer is missing or out of
range. `PASSWORD_ROTATION_HOURS` in `db.env` is no longer read; the agent logs a warning if it is
still there. The last rotation is recorded in the role's `pgsql_users.comment`
(`authnull:host=<host>;rotated=<unix time>`). Rows without that record are left alone until their
next checkout, which adopts them.

The restricted `an_…` roles the agent makes for READ/WRITE policies (see `src/pkg/backendrole.go`)
are managed roles too: same generator, same rotation record, rotated on the same schedule.

## Tests

    go test ./...

The integration tests run only when pointed at real servers. Use a disposable database owned by
a role with CREATEROLE:

    export DBAGENT_TEST_PG_DSN='postgres://<role>:<pw>@host:port/<db>?sslmode=disable'
    export AGENT_TEST_DSN="$DBAGENT_TEST_PG_DSN"
    export DBAGENT_TEST_PROXYSQL_DSN='admin,test:admin@tcp(127.0.0.1:6032)/'
    go test ./src/pkg/ -run 'Integration|Postgres' -v

They create and drop roles, and write and remove `pgsql_users` rows, named `authnull_s4_test_*`
(and `an_…` restricted roles plus a few `br_*` tables for the `Postgres` tests).
