package pkg

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
)

type GetAllJobQueueRequest struct {
	OrgID     int    `json:"org_id"`
	TenantID  int    `json:"tenant_id"`
	Host      string `json:"host"`
	DbName    string `json:"db_name"`
	AgentVMIP string `json:"agent_vm_ip"` // Multi-host: Agent VM IP
}
type GetAllJobQueueResponse struct {
	Code        string     `json:"code"`
	Status      string     `json:"status"`
	Message     string     `json:"message"`
	DbUserName  string     `json:"db_user_name"`
	HostGroupId int        `json:"hostgroup_id"`
	Data        []JobQueue `json:"data"`
}
type JobQueue struct {
	PolicyID      uuid.UUID `gorm:"primaryKey;column:id"`
	ID            int       `gorm:"primaryKey;column:id"`
	JobName       string    `gorm:"column:job_name"`
	Status        string    `gorm:"column:status"`
	DbUserID      int       `gorm:"column:db_user_id"`
	DbID          int       `gorm:"column:db_id"`
	WalletUserID  int       `gorm:"column:wallet_user_id"`
	Host          string    `gorm:"column:host"`
	DomainID      int       `gorm:"column:domain_id;default:0"`
	IssuerID      int       `gorm:"column:issuer_id;default:0"`
	Port          *int      `gorm:"column:port"`
	CredentialID  *int      `gorm:"column:credential_id"`
	Table_Name    string    `gorm:"column:table_name"`
	Fields        string    `gorm:"column:fields"`
	Privileges    string    `gorm:"column:privileges"`
	UpdatedAt     time.Time `gorm:"column:updated_at;default:CURRENT_TIMESTAMP"`
	CreatedAt     time.Time `gorm:"column:created_at;default:CURRENT_TIMESTAMP"`
	AgentVMIP     string    `gorm:"column:agent_vm_ip" json:"agent_vm_ip"`       // Multi-host: Agent VM IP
	HostVMIP      string    `gorm:"column:host_vm_ip" json:"host_vm_ip"`         // Multi-host: Database host VM IP
	HostgroupID   int       `gorm:"column:hostgroup_id" json:"hostgroup_id"`     // Multi-host: ProxySQL hostgroup ID
	DefaultSchema string    `gorm:"column:default_schema" json:"default_schema"` // Multi-host: ProxySQL default schema (database name)
}
type GetPolicyDetails struct {
	OrgId    int       `json:"orgId"`
	TenantId int       `json:"tenantId"`
	PolicyID uuid.UUID `json:"policyId"`
}
type GetPolicyDetailsResponse struct {
	Code    int        `json:"code"`
	Status  string     `json:"status"`
	Message string     `json:"message"`
	Data    PolicyJSON `json:"data"`
}

type PolicyJSON struct {
	PolicyName     string         `json:"policyName" binding:"required"`
	PolicyType     string         `json:"policyType" binding:"required"`
	Endpoints      Endpoints      `json:"endpoints,omitempty"`
	Domain         Domain         `json:"domain,omitempty"`
	AD             AD             `json:"ad,omitempty"`
	ADGroupInfra   AdGroupPolicy  `json:"infra,omitempty"`
	Networks       RadiusNetwork  `json:"networks,omitempty"`
	Dit            Dit            `json:"dit,omitempty"`
	ServiceAccount ServiceAccount `json:"serviceaccount,omitempty"`
	Database       Database       `json:"database,omitempty"`
	Permissions    Permission     `json:"permissions"`
}

type Database struct {
	DatabaseName string              `json:"database_name"`
	Tables       []string            `json:"tables"`
	FieldMasking map[string][]string `json:"field_masking"`
	User         string              `json:"user"`
	Privilege    []string            `json:"privilege"`
}

type Endpoints struct{}      // Placeholder for missing struct
type Domain struct{}         // Placeholder for missing struct
type AD struct{}             // Placeholder for missing struct
type AdGroupPolicy struct{}  // Placeholder for missing struct
type RadiusNetwork struct{}  // Placeholder for missing struct
type Dit struct{}            // Placeholder for missing struct
type ServiceAccount struct{} // Placeholder for missing struct
type Permission struct{}     // Placeholder for missing struct

func PollCheckoutJob(db *sql.DB, dbName string, Config DBConfig) error {

	//API call to get all jobs from the queue
	url := Config.API + "/api/v1/databaseService/getJobQueue"

	orgID, _ := strconv.Atoi(Config.OrgID)
	tenantID, _ := strconv.Atoi(Config.TenantID)

	ipAddr := AgentIdentity(Config)
	log.Default().Println("Agent VM IP:", ipAddr)
	payload := GetAllJobQueueRequest{
		OrgID:     orgID,
		TenantID:  tenantID,
		Host:      Config.Host,
		DbName:    dbName,
		AgentVMIP: ipAddr, // Multi-host: Agent VM IP

	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payloadBytes))

	if err != nil {
		log.Printf("Error while creating request: %v", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	//Unmarshal the response
	var response GetAllJobQueueResponse
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error while reading response body: %v", err)
		return err
	}
	err = json.Unmarshal(bodyBytes, &response)
	if err != nil {
		log.Printf("Error while unmarshalling response: %v", err)
	}
	if response.Code != "200" {
		log.Printf("Error in response: %s", response.Message)
		return fmt.Errorf("error in response: %s", response.Message)
	}
	log.Printf("Response: %s", response.Message)
	if len(response.Data) == 0 {
		log.Printf("No jobs found in the queue")
		return nil
	}
	//Iterate through the jobs and process them
	for _, job := range response.Data {
		//Call Other Function to rotate the Password for the DB User in the Database
		policyDetails, err := FetchPolicyDetails(Config.API, orgID, tenantID, job.PolicyID)
		if err != nil {
			log.Printf("Error fetching policy details for job %s: %v", job.JobName, err)
			continue
		}
		log.Printf("Policy details retrieved - Tables: %v, FieldMasking: %v, Privileges: %v",
			policyDetails.Data.Database.Tables, policyDetails.Data.Database.FieldMasking, policyDetails.Data.Database.Privilege)

		fmt.Println("Policy details:", policyDetails)

		fmt.Println("Job Details :", job)

		// Use hostgroup and default_schema from job for multi-host routing
		// DefaultSchema defaults to dbName if not provided in job
		defaultSchema := job.DefaultSchema
		if defaultSchema == "" {
			defaultSchema = dbName
		}
		success, err := GenerateCredentials(db, Config, dbName, response.DbUserName, job.Host,
			job.WalletUserID, job.IssuerID, job.Table_Name, job.Fields, job.Privileges, job.DbUserID,
			job.PolicyID, policyDetails, response.HostGroupId)
		if err != nil {
			log.Printf("Error while generating credentials: %v", err)
			continue
		}
		if success {
			log.Printf("Credentials generated successfully for job: %s", job.JobName)
			//Call Update Job API to update the job status to completed
			updateJobURL := Config.API + "/api/v1/databaseService/updateQueue"
			updateJobPayload := map[string]interface{}{
				"org_id":    orgID,
				"tenant_id": tenantID,
				"job_id":    job.ID,
			}
			updateJobPayloadBytes, err := json.Marshal(updateJobPayload)
			if err != nil {
				log.Printf("Error while marshalling update job payload: %v", err)
				continue
			}
			updateJobReq, err := http.NewRequest("POST", updateJobURL, bytes.NewBuffer(updateJobPayloadBytes))
			if err != nil {
				log.Printf("Error while creating update job request: %v", err)
				continue
			}
			updateJobReq.Header.Set("Content-Type", "application/json")
			updateJobClient := &http.Client{}
			updateJobResp, err := updateJobClient.Do(updateJobReq)
			if err != nil {
				return err
			}

			defer updateJobResp.Body.Close()
			if updateJobResp.StatusCode != http.StatusOK {
				log.Printf("Error while updating job status: %s", updateJobResp.Status)
			}
			log.Printf("Job status updated successfully for job: %s", job.JobName)

		} else {
			log.Printf("Error while generating credentials for job: %s", job.JobName)
		}

	}

	return nil

}

func FetchPolicyDetails(api string, orgID int, tenantID int, policyID uuid.UUID) (*GetPolicyDetailsResponse, error) {
	url := api + "/api/v1/policyService/getPolicyDetails"

	payload := GetPolicyDetails{
		OrgId:    orgID,
		TenantId: tenantID,
		PolicyID: policyID,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal policy request: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute HTTP request: %w", err)
	}
	defer resp.Body.Close()

	// Check HTTP status code
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API returned non-OK status: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// Parse the response
	var apiResponse GetPolicyDetailsResponse
	if err := json.Unmarshal(body, &apiResponse); err != nil {
		return nil, fmt.Errorf("failed to parse policy response: %w", err)
	}

	if apiResponse.Code != 200 {
		return nil, fmt.Errorf("invalid policy response: %s (code: %d)", apiResponse.Message, apiResponse.Code)
	}

	// Tables field is optional for PostgreSQL
	// if apiResponse.Data.Database.Tables == nil {
	// 	return nil, errors.New("policy response contains no tables data")
	// }

	return &apiResponse, nil
}

func GenerateCredentials(db *sql.DB, Config DBConfig, dbName string, dbUserName string, host string,
	WalletUserID int, IssuerId int, TableName string, Fields string, Privlege string, DbUserID int,
	policyID uuid.UUID, policyDetails *GetPolicyDetailsResponse, hostgroupID int) (bool, error) {

	// Validate policy details
	if policyDetails == nil {
		return false, errors.New("policy details cannot be nil")
	}

	// Tables field is optional for PostgreSQL
	// if len(policyDetails.Data.Database.Tables) == 0 {
	// 	return false, errors.New("no tables found in policy details")
	// }

	if len(policyDetails.Data.Database.Privilege) == 0 {
		return false, errors.New("no privileges found in policy details")
	}

	interval, err := PasswordRotationInterval(Config.PasswordRotationHours)
	if err != nil {
		return false, err
	}
	proxySQLDB, err := ConnectToProxysqlDB(Config)
	if err != nil {
		log.Printf("Error while connecting to ProxySQL database: %v", err)
		return false, err
	}
	defer proxySQLDB.Close()
	proxyUsers := adminProxyUsers{proxySQLDB}
	now := time.Now()

	// Creates the role, or rotates its password when it's due; otherwise keeps the one ProxySQL
	// already holds. The password itself goes nowhere else (see below).
	if _, err := EnsureRolePassword(db, proxyUsers, dbUserName, Config.Host, hostgroupID, now, interval); err != nil {
		log.Printf("Error while preparing managed role %q: %v", dbUserName, err)
		return false, err
	}

	// The restricted role for this policy (READ and/or WRITE): the database itself refuses what the
	// policy did not grant. See backendrole.go. Not fatal: without it ProxySQL uses the policy's own
	// role (backend_roles = prefer) or refuses the session (enforce).
	provisionPolicyBackendRole(Config, proxyUsers, dbName, dbUserName, policyDetails, hostgroupID, now, interval)

	// DID-REMOVAL: the database role IS the credential. Provisioning is complete here.
	//
	// This is where the agent used to POST /api/v1/credential/createDatabaseCredential and then
	// /api/v1/policyService/updatePolicyCredentialMapping. Neither path exists any more. The
	// issuer module (SSI / DID / VC) is unregistered in authnull-service
	// cmd/authnull-service/routes.go, and the policyService spelling of
	// updatePolicyCredentialMapping was never aliased -- only /api/v1/policy/json/... is
	// mounted, and that sits behind AuthnzMiddleware, which the agent cannot satisfy. Both
	// answer 404 "404 page not found", verified against onprem.dev.authnull.com.
	//
	// DO NOT RESTORE THE ROUTE. createDatabaseCredential minted a W3C verifiable credential into
	// the user's mobile wallet with the AES-encrypted password inside credentialSubject. It
	// required the subject's DID and the SSI gRPC agent, so there is nothing left to mount it
	// on -- and nothing to mount it FOR. listConnections now hands the user
	//
	//	psql postgresql://<dbuser>,<redis-uuid>:'<uuid>'@<agent>:6133/<db>
	//
	// where the client's password is an ephemeral Redis UUID, not this one. ProxySQL holds the
	// real role password -- the one written into pgsql_users above -- and uses it backend-side.
	// The user never sees it, so there is no credential left to hand anyone. authnull-service
	// reached the same conclusion for its own copy of this call, which is now a logging no-op in
	// internal/policy/repo/policyjson_repository.go.
	//
	// WHAT THE 404 ACTUALLY COST, WHICH IS WHY THIS IS NOT JUST DEAD-CODE REMOVAL
	//
	// The role and the pgsql_users row are both written ABOVE this point, so provisioning had
	// already succeeded every time. The 404 came back as text/plain "404 page not found", which
	// the response decoder reported as a json unmarshal error -- so the log blamed serialisation
	// rather than a missing route. GenerateCredentials then returned (false, err) and
	// PollCheckoutJob hit `continue` BEFORE updateQueue, so the job was never marked completed:
	// it stayed QUEUED and was re-provisioned every TIME_INTERVAL minute, forever. Returning
	// true here is what lets updateQueue run and the job actually finish.
	//
	// WalletUserID, IssuerId and policyID are now unused. They are kept on the signature because
	// getJobQueue still sends wallet_user_id, issuer_id and the policy id on every job row (both
	// columns are written as 0 since the issuer lookup was removed from ApproveDatabasePolicy),
	// and because the deferred gateway-auth work needs the policy id back.
	return true, nil
}
