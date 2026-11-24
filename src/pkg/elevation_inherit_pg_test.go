package pkg

import "testing"

// The agent's accounts are NOINHERIT. An elevation must still work without the session switching
// role: on PostgreSQL 16 and later the grant itself is inherited, so the account has its own
// privileges and the elevation's together (an UPDATE ... WHERE needs both), and the end takes the
// elevation back. The account stays NOINHERIT. Alpha, 24 Nov: the plain grant gave nothing.
func TestElevationIsInheritedByANoInheritAccount(t *testing.T) {
	db, _ := agentTestDB(t)
	const acct, rw, tbl = "el_inh_acct_it", "el_inh_rw_it", "el_inh_t_it"
	cleanup := func() {
		db.Exec("DROP TABLE IF EXISTS " + tbl)
		db.Exec("DROP ROLE IF EXISTS " + acct)
		db.Exec("DROP ROLE IF EXISTS " + rw)
	}
	cleanup()
	t.Cleanup(cleanup)
	for _, s := range []string{
		"CREATE TABLE " + tbl + " (id int)",
		"CREATE ROLE " + acct + " LOGIN NOINHERIT",
		"GRANT SELECT ON " + tbl + " TO " + acct,
		"CREATE ROLE " + rw + " NOLOGIN",
		"GRANT INSERT, UPDATE ON " + tbl + " TO " + rw,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	can := func(priv string) (ok bool) {
		db.QueryRow(`SELECT has_table_privilege($1, $2, $3)`, acct, tbl, priv).Scan(&ok)
		return ok
	}

	inherits := func() (ok bool) {
		db.QueryRow(`SELECT rolinherit FROM pg_roles WHERE rolname = $1`, acct).Scan(&ok)
		return ok
	}

	warn, err := elevate(db, nil, DBConfig{User: "agent_it"}, acct, elevatePayload{Grant: rw})
	if err != nil {
		t.Fatal(err)
	}
	if warn != "" {
		t.Errorf("the elevation still needs SET ROLE: %q", warn)
	}
	if !can("UPDATE") || !can("SELECT") {
		t.Errorf("elevated, without SET ROLE: UPDATE %v, SELECT %v; want both", can("UPDATE"), can("SELECT"))
	}
	// On 16 only the grant inherits; before 16 the account inherits for the elevation's length.
	if perGrantInheritance(db) == inherits() {
		t.Errorf("PostgreSQL 16+: %v, account inherits: %v", perGrantInheritance(db), inherits())
	}

	if _, err := unelevate(db, acct, elevatePayload{Grant: rw}); err != nil {
		t.Fatal(err)
	}
	if can("UPDATE") || !can("SELECT") {
		t.Errorf("after the end: UPDATE %v (want false), SELECT %v (want true)", can("UPDATE"), can("SELECT"))
	}
	if inherits() {
		t.Error("after the end the account still inherits")
	}
	var marks int
	db.QueryRow(`SELECT count(*) FROM pg_db_role_setting s JOIN pg_roles r ON r.oid = s.setrole WHERE r.rolname = $1`, acct).Scan(&marks)
	if marks != 0 {
		t.Error("the elevation's marker was left on the account")
	}
}
