package pkg

// How much a database role can do, for policies' privilege condition ("MFA for superuser roles
// only"). The levels are the policy engine's: admin, elevated, standard (authnull-service
// internal/policy/conditions).
//
//	admin     superuser, or a predefined role that reaches the server itself: running programs
//	          or reading and writing its files is a short step to superuser
//	elevated  can create roles or databases, replicate, bypass row-level security, or read or
//	          write every table (pg_read_all_data / pg_write_all_data)
//	standard  anything else
const (
	PrivilegeAdmin    = "admin"
	PrivilegeElevated = "elevated"
	PrivilegeStandard = "standard"
)

// roleAttributes are what the server says about a role (pg_roles and predefined-role membership).
type roleAttributes struct {
	Super, CreateRole, CreateDB, Replication, BypassRLS bool
	ServerAccess                                        bool // pg_execute_server_program, pg_read/write_server_files
	AllData                                             bool // pg_read_all_data, pg_write_all_data
}

func privilegeLevel(a roleAttributes) string {
	switch {
	case a.Super || a.ServerAccess:
		return PrivilegeAdmin
	case a.CreateRole || a.CreateDB || a.Replication || a.BypassRLS || a.AllData:
		return PrivilegeElevated
	}
	return PrivilegeStandard
}

// roleAttributesQuery adds the attributes to the agent's role query. Membership is checked with
// pg_has_role, so it counts indirect membership, and a predefined role this server version does
// not have simply matches nothing.
const roleAttributesColumns = `,
		r.rolsuper, r.rolcreaterole, r.rolcreatedb, r.rolreplication, r.rolbypassrls,
		EXISTS (SELECT 1 FROM pg_roles g WHERE g.rolname IN
		        ('pg_execute_server_program', 'pg_read_server_files', 'pg_write_server_files')
		        AND pg_has_role(r.oid, g.oid, 'MEMBER')) AS server_access,
		EXISTS (SELECT 1 FROM pg_roles g WHERE g.rolname IN ('pg_read_all_data', 'pg_write_all_data')
		        AND pg_has_role(r.oid, g.oid, 'MEMBER')) AS all_data`
