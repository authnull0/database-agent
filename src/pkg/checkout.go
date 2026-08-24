package pkg

import (
	"bytes"
	cryptoRand "crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"strconv"
	"time"

	"github.com/authnull0/database-agent/utils"
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

	ipAddr, err := utils.GetPublicIP()
	if err != nil {
		fmt.Println("Failed to get PublicIp Address", err)
		//return ""
	}
	log.Default().Println("IP Address:", ipAddr)
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

	var password string
	proxySQLDB, err := ConnectToProxysqlDB(Config)
	if err != nil {
		log.Printf("Error while connecting to ProxySQL database: %v", err)
		return false, err
	}

	// Before checking the password, first verify the user exists
	var userExists int
	passwordReused := false
	checkUserExistsQuery := fmt.Sprintf("SELECT COUNT(*) FROM pgsql_users WHERE username = '%s'", dbUserName)
	err = proxySQLDB.QueryRow(checkUserExistsQuery).Scan(&userExists)
	if err != nil {
		log.Printf("Error checking if user exists in ProxySQL: %v", err)
		return false, err
	}

	// If the user and password exist, we can skip password generation and just update the hostgroup if needed
	if userExists > 0 {
		// User exists, let's get the password
		var existingPassword string
		checkExistingPasswordQuery := fmt.Sprintf("SELECT password FROM pgsql_users WHERE username = '%s'", dbUserName)
		err = proxySQLDB.QueryRow(checkExistingPasswordQuery).Scan(&existingPassword)
		if err != nil {
			log.Printf("Error retrieving password for user %s: %v", dbUserName, err)
			return false, err
		}

		if existingPassword != "" {
			log.Printf("Existing password found for user %s, skipping password rotation", dbUserName)
			password = existingPassword
			passwordReused = true
		} else {
			log.Printf("User exists but has empty password, generating new one")
			password, err = GenerateRandomPassword(16)
			if err != nil {
				return false, err
			}
		}
	} else {
		// User doesn't exist, generate new password
		password, err = GenerateRandomPassword(16)
		if err != nil {
			return false, err
		}
	}

	// PostgreSQL conversion: Check if user exists
	var dbhost string
	// In PostgreSQL, we don't need host info for the user, just check if role exists
	err = db.QueryRow("SELECT rolname FROM pg_roles WHERE rolname = $1 LIMIT 1", dbUserName).Scan(&dbhost)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Printf("User %s doesn't exist in PostgreSQL", dbUserName)
			dbhost = "" // Not used in PostgreSQL but keeping the variable
		} else {
			log.Printf("Error fetching user info for %s: %v", dbUserName, err)
			return false, err
		}
	}

	// Check if the user exists
	checkUserQuery1 := fmt.Sprintf("SELECT COUNT(*) FROM pg_roles WHERE rolname = '%s'", dbUserName)
	alterPasswdQuery := fmt.Sprintf("ALTER ROLE %s WITH PASSWORD '%s'", dbUserName, password)
	updatePasswordQuery := fmt.Sprintf("UPDATE pgsql_users SET password = '%s' WHERE username = '%s'", password, dbUserName)

	var userCount1 int
	err = db.QueryRow(checkUserQuery1).Scan(&userCount1)
	if err != nil {
		log.Printf("Error checking user: %v", err)
		return false, err
	}

	if userCount1 == 0 {
		// Create role in PostgreSQL
		createUserQuery := fmt.Sprintf("CREATE ROLE %s WITH LOGIN PASSWORD '%s'", dbUserName, password)
		_, err = db.Exec(createUserQuery)
		if err != nil {
			log.Printf("Error creating user: %v", err)
			return false, err
		}
	} else if !passwordReused {
		// Only ALTER ROLE when password is newly generated, not when reusing existing ProxySQL password
		_, err = db.Exec(alterPasswdQuery)
		if err != nil {
			log.Printf("Error updating password: %v", err)
			return false, err
		}
		log.Printf("Password updated successfully for user %s", dbUserName)
	} else {
		log.Printf("User %s already exists with correct password, skipping ALTER ROLE", dbUserName)
	}

	// Check if the user already exists in ProxySQL
	checkUserQuery := fmt.Sprintf("SELECT COUNT(*) FROM pgsql_users WHERE username = '%s'", dbUserName)
	var userCount2 int
	err = proxySQLDB.QueryRow(checkUserQuery).Scan(&userCount2)
	if err != nil {
		log.Printf("Error while checking existence of user %s in ProxySQL: %v", dbUserName, err)
		return false, err
	}

	if userCount2 == 0 {
		// Create the user if it does not exist
		// Include default_hostgroup for multi-host routing
		var createUserQuery string
		createUserQuery = fmt.Sprintf("INSERT INTO pgsql_users (username, password, default_hostgroup, active, use_ssl) VALUES ('%s', '%s', %d, 1, 0)", dbUserName, password, hostgroupID)

		_, err = proxySQLDB.Exec(createUserQuery)

		if err != nil {
			log.Printf("Error while creating user %s in ProxySQL: %v", dbUserName, err)
			return false, err
		}
		log.Printf("User %s created successfully in ProxySQL with hostgroup=%d", dbUserName, hostgroupID)
	} else {
		// User exists, update hostgroup if provided
		if hostgroupID > 0 {
			updateHostgroupQuery := fmt.Sprintf("UPDATE pgsql_users SET default_hostgroup = %d WHERE username = '%s'", hostgroupID, dbUserName)
			_, err = proxySQLDB.Exec(updateHostgroupQuery)
			if err != nil {
				log.Printf("Error updating hostgroup for user %s: %v", dbUserName, err)
			}
		}
	}

	// Update the password for the user in ProxySQL
	_, err = proxySQLDB.Exec(updatePasswordQuery)
	if err != nil {
		log.Printf("Error while updating password for user %s in ProxySQL: %v", dbUserName, err)
		return false, err
	}
	log.Printf("Password for user %s updated successfully in ProxySQL", dbUserName)

	_, err = proxySQLDB.Exec("LOAD PGSQL USERS TO RUNTIME;")
	if err != nil {
		log.Printf("Error loading users to runtime in ProxySQL: %v", err)
		return false, err
	}

	_, err = proxySQLDB.Exec("SAVE PGSQL USERS TO DISK;")
	if err != nil {
		log.Printf("Error saving users to disk in ProxySQL: %v", err)
		return false, err
	}

	// The credential pipeline that used to run here is gone. It encrypted the
	// password with a hardcoded AES key and posted it to
	// /api/v1/credential/createDatabaseCredential and
	// /api/v1/policyService/updatePolicyCredentialMapping -- both of which
	// return 404, which is why provisioning jobs never reached completion.
	//
	// The role and the pgsql_users row written above are the whole job now. The
	// policy fields the credential used to carry (privilege, tables,
	// fieldMasking) are read straight from the policy at login time by the MFA
	// decision endpoint, so nothing needs to be minted here.
	log.Printf("Provisioning complete for user %s on database %s", dbUserName, dbName)

	return true, nil
}

// GenerateRandomPassword returns a cryptographically random password of the
// given length.
//
// This used to use math/rand seeded from time.Now().UnixNano() -- a predictable
// seed feeding a non-cryptographic generator, for values that are written into
// pgsql_users and grant real database access.
func GenerateRandomPassword(length int) (string, error) {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()_+[]{}|;:,.<>?"
	if length <= 0 {
		return "", errors.New("password length must be positive")
	}
	b := make([]byte, length)
	max := big.NewInt(int64(len(charset)))
	for i := range b {
		n, err := cryptoRand.Int(cryptoRand.Reader, max)
		if err != nil {
			return "", fmt.Errorf("failed to read cryptographic randomness: %w", err)
		}
		b[i] = charset[n.Int64()]
	}
	return string(b), nil
}
