package pkg

import (
	"reflect"
	"testing"
)

// The servers whose recordings the agent uploads: MySQL targets as well as PostgreSQL ones (the
// ProxySQL fork spools both, with the hostgroup), each placed by its own engine's hostgroup.
func TestRecordingTargetsIncludeMySQL(t *testing.T) {
	var asked []string
	lookup := func(cfg DBConfig) (int, bool, error) {
		asked = append(asked, Engine(cfg.DBType)+" "+cfg.Host)
		return map[string]int{"pg": 6, "my": 4}[cfg.Host], true, nil
	}
	got := recordingTargets([]DBConfig{{Host: "pg", DBType: "postgres"}, {Host: "my", DBType: "mysql"}, {Host: "ora", DBType: "oracle"}}, lookup)
	var hgs []int
	for _, r := range got {
		hgs = append(hgs, r.hostgroup)
	}
	if !reflect.DeepEqual(hgs, []int{6, 4}) || !reflect.DeepEqual(asked, []string{"postgres pg", "mysql my"}) {
		t.Fatalf("hostgroups %v, asked %v", hgs, asked)
	}

	// One server: every line is its, no lookup.
	asked = nil
	got = recordingTargets([]DBConfig{{Host: "my", DBType: "mysql"}}, lookup)
	if len(got) != 1 || got[0].hostgroup != -1 || len(asked) != 0 {
		t.Fatalf("single: %+v, asked %v", got, asked)
	}

	// A server whose hostgroup is not known is left out of this round.
	got = recordingTargets([]DBConfig{{Host: "pg"}, {Host: "my", DBType: "mysql"}}, func(cfg DBConfig) (int, bool, error) {
		return 6, cfg.Host == "pg", nil
	})
	if len(got) != 1 || got[0].cfg.Host != "pg" {
		t.Fatalf("unknown hostgroup: %+v", got)
	}
}
