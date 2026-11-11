package pkg

import (
	"reflect"
	"testing"
)

// A table name is a value in the column query, not SQL: a quote in it neither breaks nor injects.
func TestPostgresTablesWithAQuoteInTheName(t *testing.T) {
	db, _ := agentTestDB(t)
	cleanup := func() { db.Exec(`DROP TABLE IF EXISTS "br_o'quote"`) }
	cleanup()
	t.Cleanup(cleanup)
	if _, err := db.Exec(`CREATE TABLE "br_o'quote" (a int, b text)`); err != nil {
		t.Fatal(err)
	}
	tables, err := pgTables(db)
	if err != nil {
		t.Fatal(err)
	}
	if got := tables["br_o'quote"]; !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("columns %v", got)
	}
	if got := tables["br_customers"]; !reflect.DeepEqual(got, []string{"id", "name"}) {
		t.Fatalf("br_customers columns %v", got)
	}
}
