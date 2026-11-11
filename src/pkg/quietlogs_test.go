package pkg

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Lines a sync, a ProxySQL connection or a per-table read used to log at the default level, every
// minute (or every few seconds) for every user, table and database: debug only now (logDebugf).
var debugOnlyPhrases = []string{
	"Successfully connected",
	"Skipping system database",
	"Fetching database details",
	"Fetching table names",
	"Fetched tables for",
	"Payload created for",
	"Payload Sent",
	"Sending table names",
	"Sending User Payload",
	"API Response",
	"Response from",
	"Executing query",
	"Processing database",
	"Registered agent for database",
	"call completed",
	" completed for",
	"FetchTablePrivileges completed",
	"FetchTables completed",
	"UpdateHostInProxySQL completed",
	"All operations completed",
	"Database Status from API",
	"Database Synchronized with host group",
	"DbSync Response",
	"Printing Data Obj",
	"Register Agent",
	"Agent VM IP",
	"Agent Private IP",
	"Private IP Address:",
	"Database Host IP",
	"Org Id",
	"Tenant Id",
	"is Active",
	"Active: %d seconds",
	"Database Register Agent Payload",
	"=====",
	"Machine registered",
	"Instance Id after last active",
	"No Need to update ProxySQL server",
	"ProxySQL init query success",
}

// No default-level log call in the package (tests aside) carries one of those lines.
func TestNoPerIterationSuccessLinesAtDefaultLevel(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isDefaultLogCall(call) {
				return true
			}
			for _, arg := range call.Args {
				ast.Inspect(arg, func(m ast.Node) bool {
					lit, ok := m.(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						return true
					}
					s, _ := strconv.Unquote(lit.Value)
					for _, p := range debugOnlyPhrases {
						if strings.Contains(s, p) {
							t.Errorf("%s: %q logged at the default level; use logDebugf", fset.Position(lit.Pos()), s)
						}
					}
					return true
				})
			}
			return true
		})
	}
}

// isDefaultLogCall is log.Print*/log.Default().Print*, or fmt.Print* (stdout, around the log).
func isDefaultLogCall(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !strings.HasPrefix(sel.Sel.Name, "Print") {
		return false
	}
	switch x := sel.X.(type) {
	case *ast.Ident:
		return x.Name == "log" || x.Name == "fmt"
	case *ast.CallExpr:
		inner, ok := x.Fun.(*ast.SelectorExpr)
		return ok && inner.Sel.Name == "Default"
	}
	return false
}

// A control plane that cannot be reached: registration reports nothing, and does not crash the
// agent on a nil response.
func TestRegisterAgentUnreachable(t *testing.T) {
	t.Cleanup(resetInstallCredentialForTest)
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	captureLog(t, false)
	id, status := RegisterAgent(nil, "appdb", DBConfig{API: srv.URL, DBType: "postgres"})
	if id != "" || status != "" {
		t.Fatalf("got %q %q", id, status)
	}
}

func TestFetchDatabaseStatusUnreachable(t *testing.T) {
	t.Cleanup(resetInstallCredentialForTest)
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	captureLog(t, false)
	db, _ := newFakeAdmin(t, countAnswer(100))
	if _, _, err := FetchDatabaseStatus(db, "appdb", DBConfig{API: srv.URL, DBType: "postgres"}, "inst-1"); err == nil {
		t.Fatal("no error for an unreachable control plane")
	}
}
