package pkg

import (
	"encoding/json"
	"fmt"
)

// Secrets never reach the agent's log. The agent printed its configuration -- database password,
// tenant key, machine key -- at every start and every sync, and the registration payload with the
// tenant key in it. DBConfig formats itself without them, so every %v, %+v, %#v or %s of a config
// is safe, including ones not written yet.

const redacted = "[redacted]"

func mask(s string) string {
	if s == "" {
		return ""
	}
	return redacted
}

// String is what %v, %+v and %s print.
func (c DBConfig) String() string {
	return fmt.Sprintf("{OrgID:%s TenantID:%s DBType:%s Port:%s TimeInterval:%s API:%s Host:%s User:%s Password:%s Key:%s MachineKey:%s AgentVMIP:%s ProxyClientID:%s ProxyClientSecret:%s}",
		c.OrgID, c.TenantID, c.DBType, c.Port, c.TimeInterval, c.API, c.Host, c.User,
		mask(c.Password), mask(c.Key), mask(c.MachineKey), c.AgentVMIP, c.ProxyClientID, mask(c.ProxyClientSecret))
}

// GoString is what %#v prints.
func (c DBConfig) GoString() string { return "pkg.DBConfig" + c.String() }

// secretFields are the payload keys that carry credentials: the tenant key travels as "uuid".
var secretFields = map[string]bool{"uuid": true, "key": true, "machineKey": true, "password": true, "token": true,
	"accessToken": true, "clientSecret": true}

// payloadForLog is a copy of an API payload with its secrets replaced, for logging only.
func payloadForLog(p map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(p))
	for k, v := range p {
		if secretFields[k] {
			out[k] = redacted
		} else {
			out[k] = redactValue(v)
		}
	}
	return out
}

func redactValue(v interface{}) interface{} {
	switch x := v.(type) {
	case map[string]interface{}:
		return payloadForLog(x)
	case []interface{}:
		out := make([]interface{}, len(x))
		for i := range x {
			out[i] = redactValue(x[i])
		}
		return out
	default:
		return v
	}
}

// jsonForLog is a JSON body as it may appear in a log: secret fields replaced at any depth. A body
// that is not a JSON object is not printed at all.
func jsonForLog(body []byte) string {
	var v interface{}
	if err := json.Unmarshal(body, &v); err != nil {
		return fmt.Sprintf("(%d bytes, not JSON; not shown)", len(body))
	}
	out, err := json.Marshal(redactValue(v))
	if err != nil {
		return "(not shown)"
	}
	return string(out)
}
