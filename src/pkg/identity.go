package pkg

import (
	"log"

	"github.com/authnull0/database-agent/utils"
)

// AgentIdentity returns the stable identity of this agent VM, used for the
// agent_vm_ip field in every payload sent to the API.
//
// AGENT_VM_IP from db.env wins when set. Otherwise the VM's private IP is used:
// it survives reboots and identifies this agent inside the VNet. The public IP
// is deliberately NOT used here -- on a VM with no public IP of its own, an
// outbound lookup returns the subnet's egress SNAT address, which is ephemeral,
// shared with every other VM in the subnet, and accepts no inbound traffic.
func AgentIdentity(config DBConfig) string {
	if config.AgentVMIP != "" {
		return config.AgentVMIP
	}

	privateIP, err := utils.GetPrivateIP()
	if err != nil {
		log.Printf("Failed to resolve private IP for agent identity: %v", err)
		return ""
	}
	return privateIP
}
