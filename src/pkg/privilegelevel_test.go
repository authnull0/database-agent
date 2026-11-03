package pkg

import "testing"

func TestPrivilegeLevel(t *testing.T) {
	for name, tc := range map[string]struct {
		a    roleAttributes
		want string
	}{
		"plain login role":      {roleAttributes{}, PrivilegeStandard},
		"superuser":             {roleAttributes{Super: true}, PrivilegeAdmin},
		"runs server programs":  {roleAttributes{ServerAccess: true}, PrivilegeAdmin},
		"superuser wins":        {roleAttributes{Super: true, CreateDB: true}, PrivilegeAdmin},
		"creates roles":         {roleAttributes{CreateRole: true}, PrivilegeElevated},
		"creates databases":     {roleAttributes{CreateDB: true}, PrivilegeElevated},
		"replication":           {roleAttributes{Replication: true}, PrivilegeElevated},
		"bypasses RLS":          {roleAttributes{BypassRLS: true}, PrivilegeElevated},
		"reads/writes all data": {roleAttributes{AllData: true}, PrivilegeElevated},
	} {
		if got := privilegeLevel(tc.a); got != tc.want {
			t.Errorf("%s: %q, want %q", name, got, tc.want)
		}
	}
}
