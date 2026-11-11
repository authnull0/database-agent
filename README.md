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




## Logging

The agent writes `/var/log/authnull-db-agent.log` itself, mode 0640 (an older 0644 file is
tightened at start).

**Level.** By default the log carries failures (with the control plane's status code for a
refused call), what the agent changed (rotations, vault jobs, kills, ProxySQL server moves), and
one line per database per sync:

    Synced appdb (postgres, 192.168.122.175:5432): 42 users, 0 failed

`LOG_LEVEL=debug`, in `db.env` or the service's environment, adds the per-user, per-table and
per-payload detail and every successful connection (payloads with their secrets redacted). That
is tens of thousands of lines an hour on a busy install: turn it on to chase a problem, then off.

**Rotation.** Install `packaging/logrotate/authnull-db-agent`:

    sudo install -m 0644 packaging/logrotate/authnull-db-agent /etc/logrotate.d/authnull-db-agent
    sudo logrotate -d /etc/logrotate.d/authnull-db-agent   # dry run

Daily, 14 days kept, compressed from the second day. It uses `copytruncate` because the agent
holds the file open and is not told to reopen it (a line written during the copy can be lost).

## Identity

The agent authenticates to the control plane with its install's credential:
`PROXY_CLIENT_ID` and `PROXY_CLIENT_SECRET` in `db.env`, written by the console when it builds the
file. The agent exchanges them for a short-lived token at
`/api/v1/databaseService/proxy/token`, sends it on every call, and exchanges again before it
expires or after a 401. The secret and the token are never logged.

An agent without them still calls, with no credential, while the control plane's
`DB_AGENT_ALLOW_UNAUTHENTICATED` is on, which is the default during the move. Download a fresh
`db.env` for each install before that switch is turned off.

## ProxySQL's admin interface

ProxySQL holds every managed role's password in clear in `pgsql_users`, because it logs in to the
database with them. Whoever can read its admin interface has them all, and can then log in to the
database directly. The agent's login to it comes from `db.env`:

| Setting | Default |
|---|---|
| `PROXYSQL_ADMIN_USER` | `admin` |
| `PROXYSQL_ADMIN_PASSWORD` | `admin` |
| `PROXYSQL_ADMIN_ADDR` | `127.0.0.1:6032` |

The agent logs in as `<user>,agent`, because the AuthNull ProxySQL build splits every login at its
first comma. On every sync it reports the admin interface as **direct access** (exposed) when:
- it listens on anything but loopback (`admin-mysql_ifaces` / `admin-pgsql_ifaces`), or
- it still has the default password.

To lock it down:
- set `admin-mysql_ifaces` and `admin-pgsql_ifaces` to `127.0.0.1:…`;
- give `admin-admin_credentials` a generated password, and put the same password in
  `PROXYSQL_ADMIN_PASSWORD`;
- make `db.env` and `/var/lib/proxysql/proxysql.db` mode 600.

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

## Passwords held in the vault

For roles an administrator manages from the console (Database → Users → Manage password),
AuthNull's backend generates the password and keeps it in its vault; the agent applies it. On
every sync the agent:

1. claims its database server's password jobs (`/api/v1/databaseService/agent/jobs/claim`);
2. fetches each password (`…/jobs/secret`, released only to this install, while it holds the job);
3. sets it with `ALTER ROLE`, then in ProxySQL's `pgsql_users`;
4. reports the result (`…/jobs/result`).

It never creates a role this way, and refuses a job for a server it does not serve.

**Requirements:** only an agent with its install's credential takes part (see Identity). Postgres
and MySQL targets (see MySQL targets).

**Afterwards:** such a role is marked `authnull:vault;host=…;rotated=…` in `pgsql_users`. The
agent's own rotation leaves it alone: the sweep skips it, and checkout returns its password
unchanged.

**If ProxySQL fails:** when the database took the password but ProxySQL could not be written, even
on a retry, the job is reported done with a warning, so the vault keeps the password the database
really has. The next rotation repairs ProxySQL.

## MySQL targets

A target in `data-source.yaml` is MySQL when its `type` is `mysql` (or `mariadb`); `postgres`,
`postgresql`, `pg` or no type is Postgres. Without a `port` the engine's default is used (3306 /
5432). One agent can serve both kinds:

```yaml
databases:
  - host: 10.0.0.10
    type: postgres
    port: 5432
    username: authnull_agent
    password: ENC(...)
  - host: 10.0.0.11
    type: mysql
    port: 3306
    username: authnull_agent
    password: ENC(...)
```

The MySQL account the agent logs in as needs:

    CREATE USER 'authnull_agent'@'<agent-ip>' IDENTIFIED BY '...';
    GRANT CREATE USER, PROCESS, CONNECTION_ADMIN ON *.* TO 'authnull_agent'@'<agent-ip>';
    GRANT SELECT ON mysql.* TO 'authnull_agent'@'<agent-ip>';
    -- for each database whose access the console manages:
    GRANT ALL ON `shop`.* TO 'authnull_agent'@'<agent-ip>' WITH GRANT OPTION;

What differs from Postgres:

- **Sync:** databases from `information_schema.SCHEMATA` (system databases skipped), accounts from
  `mysql.user` with their global and per-database grants, tables from `information_schema`. An
  account's privilege level: `SUPER`, `CREATE USER` or any global privilege `WITH GRANT OPTION` is
  admin, `FILE` too; `CREATE`, `CREATE ROLE`/`ROLE_ADMIN`, `REPLICATION SLAVE` or a global data
  privilege is elevated; anything else standard. The pg_hba direct-access check does not apply.
- **ProxySQL:** the server goes to `mysql_servers` and its managed users to `mysql_users`
  (`LOAD/SAVE MYSQL …`). Their comment records `host:port`, so two servers on one IP stay apart.
- **Passwords:** `ALTER USER … IDENTIFIED BY` on every host row of the user name.
- **Console users** are `'name'@'%'` with `ATTRIBUTE '{"authnull":"console"}'`; **policy users**
  (`an_…`) carry `{"authnull":"policy"}`. The agent changes or drops only an account with its mark
  and no other host row. Console grants are made exactly the job's (database, table, function and
  procedure objects; anything else, column grants and `GRANT OPTION` included, is revoked); a
  database-level grant escapes `_` and `%`, so it names that database only. MySQL has no
  `VALID UNTIL`: the backend drops an expired console user. A policy's `TRUNCATE` is granted as
  `DROP`, which MySQL's `TRUNCATE TABLE` requires. A drop locks the account, ends its sessions and
  drops it.
- **Live sessions** come from `stats_mysql_processlist` and are ended with ProxySQL's admin
  `KILL CONNECTION`.

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

The MySQL tests run against a disposable MySQL 8.0 database whose account has the privileges in
MySQL targets (plus `ROLE_ADMIN` and `SYSTEM_USER` if the server has accounts with them):

    export DBAGENT_TEST_MYSQL_DSN='agent_it:<pw>@tcp(host:3306)/agent_it_db'
    go test ./src/pkg/ -run MySQL -v

They create and drop users and tables named `agent_it_*` and `an_…`.
