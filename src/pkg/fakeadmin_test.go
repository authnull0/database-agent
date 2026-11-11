package pkg

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
)

// fakeAdmin is ProxySQL's admin interface as a database/sql driver: it records every statement and
// answers queries from a function. It takes no bind parameters, as the real one (SQLite behind the
// MySQL protocol, no server-side prepared statements) cannot.
type fakeAdmin struct {
	mu     sync.Mutex
	stmts  []string
	answer func(q string) (cols []string, rows [][]driver.Value, err error)
}

func (f *fakeAdmin) statements() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string{}, f.stmts...)
}

var fakeAdmins = struct {
	sync.Mutex
	n  int
	by map[string]*fakeAdmin
}{by: map[string]*fakeAdmin{}}

func init() { sql.Register("fakeadmin", fakeAdminDriver{}) }

// newFakeAdmin opens a connection to a fresh fake admin interface. answer may be nil: every query
// then has no rows.
func newFakeAdmin(t *testing.T, answer func(q string) ([]string, [][]driver.Value, error)) (*sql.DB, *fakeAdmin) {
	t.Helper()
	f := &fakeAdmin{answer: answer}
	fakeAdmins.Lock()
	fakeAdmins.n++
	name := fmt.Sprintf("admin-%d", fakeAdmins.n)
	fakeAdmins.by[name] = f
	fakeAdmins.Unlock()
	db, err := sql.Open("fakeadmin", name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, f
}

// countAnswer answers every SELECT COUNT(*) with n.
func countAnswer(n int64) func(string) ([]string, [][]driver.Value, error) {
	return func(q string) ([]string, [][]driver.Value, error) {
		if strings.HasPrefix(q, "SELECT COUNT(*)") {
			return []string{"n"}, [][]driver.Value{{n}}, nil
		}
		return nil, nil, nil
	}
}

type fakeAdminDriver struct{}

func (fakeAdminDriver) Open(name string) (driver.Conn, error) {
	fakeAdmins.Lock()
	defer fakeAdmins.Unlock()
	f, ok := fakeAdmins.by[name]
	if !ok {
		return nil, errors.New("no such fake admin")
	}
	return &fakeAdminConn{f}, nil
}

type fakeAdminConn struct{ f *fakeAdmin }

func (c *fakeAdminConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("the admin interface has no prepared statements")
}
func (c *fakeAdminConn) Close() error              { return nil }
func (c *fakeAdminConn) Begin() (driver.Tx, error) { return nil, errors.New("no transactions") }

func (c *fakeAdminConn) record(q string, args []driver.NamedValue) error {
	if len(args) > 0 {
		return errors.New("the admin interface takes no bind parameters")
	}
	c.f.mu.Lock()
	c.f.stmts = append(c.f.stmts, q)
	c.f.mu.Unlock()
	return nil
}

func (c *fakeAdminConn) ExecContext(_ context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	if err := c.record(q, args); err != nil {
		return nil, err
	}
	if c.f.answer != nil {
		if _, _, err := c.f.answer(q); err != nil {
			return nil, err
		}
	}
	return driver.RowsAffected(1), nil
}

func (c *fakeAdminConn) QueryContext(_ context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.record(q, args); err != nil {
		return nil, err
	}
	var cols []string
	var rows [][]driver.Value
	if c.f.answer != nil {
		var err error
		if cols, rows, err = c.f.answer(q); err != nil {
			return nil, err
		}
	}
	if cols == nil {
		cols = []string{"x"}
	}
	return &fakeAdminRows{cols: cols, rows: rows}, nil
}

type fakeAdminRows struct {
	cols []string
	rows [][]driver.Value
}

func (r *fakeAdminRows) Columns() []string { return r.cols }
func (r *fakeAdminRows) Close() error      { return nil }
func (r *fakeAdminRows) Next(dest []driver.Value) error {
	if len(r.rows) == 0 {
		return io.EOF
	}
	copy(dest, r.rows[0])
	r.rows = r.rows[1:]
	return nil
}
