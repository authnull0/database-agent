package pkg

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	cryptoRand "crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"

	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/authnull0/database-agent/utils"
	"github.com/google/uuid"
)

type CreateDatabaseCredentialResponseDto struct {
	Status       string `json:"status"`
	Message      string `json:"message"`
	Code         int    `json:"code"`
	CredentialId int    `json:"credentialId"`
}

type GetAllJobQueueRequest struct {
	OrgID    int    `json:"org_id"`
	TenantID int    `json:"tenant_id"`
	Host     string `json:"host"`
	DbName   string `json:"db_name"`
}
type GetAllJobQueueResponse struct {
	Code       string     `json:"code"`
	Status     string     `json:"status"`
	Message    string     `json:"message"`
	DbUserName string     `json:"db_user_name"`
	Data       []JobQueue `json:"data"`
}
type JobQueue struct {
	PolicyID     uuid.UUID `gorm:"primaryKey;column:id"`
	ID           int       `gorm:"primaryKey;column:id"`
	JobName      string    `gorm:"column:job_name"`
	Status       string    `gorm:"column:status"`
	DbUserID     int       `gorm:"column:db_user_id"`
	DbID         int       `gorm:"column:db_id"`
	WalletUserID int       `gorm:"column:wallet_user_id"`
	Host         string    `gorm:"column:host"`
	DomainID     int       `gorm:"column:domain_id;default:0"`
	IssuerID     int       `gorm:"column:issuer_id;default:0"`
	Port         *int      `gorm:"column:port"`
	CredentialID *int      `gorm:"column:credential_id"`
	Table_Name   string    `gorm:"column:table_name"`
	Fields       string    `gorm:"column:fields"`
	Privileges   string    `gorm:"column:privileges"`
	UpdatedAt    time.Time `gorm:"column:updated_at;default:CURRENT_TIMESTAMP"`
	CreatedAt    time.Time `gorm:"column:created_at;default:CURRENT_TIMESTAMP"`
}
type CreateDatabaseCredentialRequestDto struct {
	OrgId          int                 `json:"orgId"`
	TenantId       int                 `json:"tenantId"`
	WalletUserId   int                 `json:"userId"`
	IssuerId       int                 `json:"issuerId"`
	Host           string              `json:"host"`
	CredentialType string              `json:"credentialType"`
	DatabaseName   string              `json:"database_name"`
	Tables         []string            `json:"tables"`
	FieldMasking   map[string][]string `json:"field_masking"`
	DBUser         string              `json:"user"`
	Privilege      []string            `json:"privilege"`
	Password       string              `json:"password"`
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
	url := "https:/dev.api.authnull.com/api/v1/databaseService/getJobQueue"
	orgID, _ := strconv.Atoi(Config.OrgID)
	tenantID, _ := strconv.Atoi(Config.TenantID)

	ipAddr, err := utils.GetPublicIP()
	if err != nil {
		fmt.Println("Failed to get PublicIp Address", err)
		//return ""
	}
	log.Default().Println("IP Address:", ipAddr)
	payload := GetAllJobQueueRequest{
		OrgID:    orgID,
		TenantID: tenantID,
		Host:     ipAddr,
		DbName:   dbName,
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
		policyDetails, err := FetchPolicyDetails(orgID, tenantID, job.PolicyID)
		if err != nil {
			log.Printf("Error fetching policy details for job %s: %v", job.JobName, err)
			continue
		}
		log.Printf("Policy details retrieved - Tables: %v, FieldMasking: %v, Privileges: %v",
			policyDetails.Data.Database.Tables, policyDetails.Data.Database.FieldMasking, policyDetails.Data.Database.Privilege)

		fmt.Println("Policy details:", policyDetails)

		fmt.Println("Job Details :", job)

		success, err := GenerateCredentials(db, Config, dbName, response.DbUserName, job.Host,
			job.WalletUserID, job.IssuerID, job.Table_Name, job.Fields, job.Privileges, job.DbUserID,
			job.PolicyID, policyDetails)
		if err != nil {
			log.Printf("Error while generating credentials: %v", err)
			continue
		}
		if success {
			log.Printf("Credentials generated successfully for job: %s", job.JobName)
			//Call Update Job API to update the job status to completed
			updateJobURL := "https://dev.api.authnull.com/api/v1/databaseService/updateQueue"
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

func FetchPolicyDetails(orgID int, tenantID int, policyID uuid.UUID) (*GetPolicyDetailsResponse, error) {
	url := "https://dev.api.authnull.com/api/v1/policyService/getPolicyDetails"

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

	if apiResponse.Data.Database.Tables == nil {
		return nil, errors.New("policy response contains no tables data")
	}

	return &apiResponse, nil
}

// New function to encrypt a string using AES
func EncryptAES(plaintext string, key []byte) (string, error) {
	// Create a new AES cipher block
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	// Create a byte array with the plaintext
	plaintextBytes := []byte(plaintext)

	// The IV needs to be unique, but not secure
	ciphertext := make([]byte, aes.BlockSize+len(plaintextBytes))
	iv := ciphertext[:aes.BlockSize]
	if _, err := io.ReadFull(cryptoRand.Reader, iv); err != nil {
		return "", err
	}

	// Use CFB mode for encryption
	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(ciphertext[aes.BlockSize:], plaintextBytes)

	// Return the encrypted bytes as a hex string
	return hex.EncodeToString(ciphertext), nil
}

func GenerateCredentials(db *sql.DB, Config DBConfig, dbName string, dbUserName string, host string,
	WalletUserID int, IssuerId int, TableName string, Fields string, Privlege string, DbUserID int,
	policyID uuid.UUID, policyDetails *GetPolicyDetailsResponse) (bool, error) {

	// Validate policy details
	if policyDetails == nil {
		return false, errors.New("policy details cannot be nil")
	}

	if len(policyDetails.Data.Database.Tables) == 0 {
		return false, errors.New("no tables found in policy details")
	}

	if len(policyDetails.Data.Database.Privilege) == 0 {
		return false, errors.New("no privileges found in policy details")
	}
	//Rotate the Credentials for the DB User in the Database
	//Step1 : Generate a Random Password for the DB User
	//password, err := GenerateRandomPassword(16)
	//if err != nil {
	//		log.Printf("Error while generating random password: %v", err)
	//		return false, err
	//	}
	var password string
	proxySQLDB, err := ConnectToProxysqlDB(Config)
	if err != nil {
		log.Printf("Error while connecting to ProxySQL database: %v", err)
		return false, err
	}
	// Before checking the password, first verify the user exists
	var userExists int
	checkUserExistsQuery := fmt.Sprintf("SELECT COUNT(*) FROM mysql_users WHERE username = '%s'", dbUserName)
	err = proxySQLDB.QueryRow(checkUserExistsQuery).Scan(&userExists)
	if err != nil {
		log.Printf("Error checking if user exists in ProxySQL: %v", err)
		return false, err
	}

	if userExists > 0 {
		// User exists, let's get the password
		var existingPassword string
		checkExistingPasswordQuery := fmt.Sprintf("SELECT password FROM mysql_users WHERE username = '%s'", dbUserName)
		err = proxySQLDB.QueryRow(checkExistingPasswordQuery).Scan(&existingPassword)
		if err != nil {
			log.Printf("Error retrieving password for user %s: %v", dbUserName, err)
			return false, err
		}

		if existingPassword != "" {
			log.Printf("Existing password found for user %s, skipping password rotation", dbUserName)
			password = existingPassword
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
	var dbhost string
	err = db.QueryRow("SELECT host FROM mysql.user WHERE user = ? LIMIT 1", dbUserName).Scan(&dbhost)
	if err != nil {
		log.Printf("Error fetching host for user %s: %v", dbUserName, err)
		return false, err
	}
	// Check if the user exists with the correct host
	checkUserQuery1 := fmt.Sprintf("SELECT COUNT(*) FROM mysql.user WHERE user = '%s' AND host = '%s'", dbUserName, dbhost)
	alterPasswdQuery := fmt.Sprintf("ALTER USER '%s'@'%s' IDENTIFIED BY '%s'", dbUserName, dbhost, password)
	updatePasswordQuery := fmt.Sprintf("UPDATE mysql_users SET password = '%s' WHERE username = '%s'", password, dbUserName)
	var userCount1 int
	err = db.QueryRow(checkUserQuery1).Scan(&userCount1)
	if err != nil {
		log.Printf("Error checking user: %v", err)
		return false, err
	}

	if userCount1 == 0 {
		// Create user with the correct host
		createUserQuery := fmt.Sprintf("CREATE USER '%s'@'%s' IDENTIFIED BY '%s'", dbUserName, dbhost, password)
		_, err = db.Exec(createUserQuery)
		if err != nil {
			log.Printf("Error creating user: %v", err)
			return false, err
		}
	} else {
		// Update password for the correct host
		_, err = db.Exec(alterPasswdQuery)
		if err != nil {
			log.Printf("Error updating password: %v", err)
			return false, err
		}
		log.Printf("Password updated successfully for user %s@%s", dbUserName, dbhost)
	}

	//COnnect to ProxysqlDB
	//proxySQLDB, err := ConnectToProxysqlDB(Config)
	//if err != nil {
	//	log.Printf("Error while connecting to ProxySQL database: %v", err)
	//}
	//Create the user in ProxySQL
	// Check if the user already exists in ProxySQL
	checkUserQuery := fmt.Sprintf("SELECT COUNT(*) FROM mysql_users WHERE username = '%s'", dbUserName)
	var userCount2 int
	err = proxySQLDB.QueryRow(checkUserQuery).Scan(&userCount2)
	if err != nil {
		log.Printf("Error while checking existence of user %s in ProxySQL: %v", dbUserName, err)
		return false, err
	}

	if userCount2 == 0 {
		// Create the user if it does not exist
		createUserQuery := fmt.Sprintf("INSERT INTO mysql_users (username, password, active, use_ssl) VALUES ('%s', '%s', 1, 0)", dbUserName, password)
		_, err = proxySQLDB.Exec(createUserQuery)

		if err != nil {
			log.Printf("Error while creating user %s in ProxySQL: %v", dbUserName, err)
			return false, err
		}
		log.Printf("User %s created successfully in ProxySQL", dbUserName)
	} else {
		log.Printf("User %s already exists in ProxySQL", dbUserName)
	}

	// Update the password for the user in ProxySQL
	_, err = proxySQLDB.Exec(updatePasswordQuery)
	if err != nil {
		log.Printf("Error while updating password for user %s in ProxySQL: %v", dbUserName, err)
		return false, err
	}
	log.Printf("Password for user %s updated successfully in ProxySQL", dbUserName)

	_, err = proxySQLDB.Exec("LOAD MYSQL USERS TO RUNTIME;")
	if err != nil {
		log.Printf("Error loading users to runtime in ProxySQL: %v", err)
		return false, err
	}

	_, err = proxySQLDB.Exec("SAVE MYSQL USERS TO DISK;")
	if err != nil {
		log.Printf("Error saving users to disk in ProxySQL: %v", err)
		return false, err
	}

	orgId, _ := strconv.Atoi(Config.OrgID)
	tenantId, _ := strconv.Atoi(Config.TenantID)

	tables := policyDetails.Data.Database.Tables
	fieldMasking := policyDetails.Data.Database.FieldMasking
	privilege := policyDetails.Data.Database.Privilege

	// Step 3: Encrypt the password before sending it to the API
	// You need to define this AES key somewhere secure in your application
	encryptionKey := []byte("84sF#v7Fpt!L#PesYb^AezXrUn2kE%5v") // This should be 16, 24, or 32 bytes for AES-128, AES-192, or AES-256

	encryptedPassword, err := EncryptAES(password, encryptionKey)
	if err != nil {
		log.Printf("Error encrypting password: %v", err)
		return false, err
	}

	//Create the request body with encrypted password
	databaseCredentialRequest := CreateDatabaseCredentialRequestDto{
		OrgId:          orgId,
		TenantId:       tenantId,
		WalletUserId:   WalletUserID,
		IssuerId:       IssuerId,
		Host:           host,
		CredentialType: "DATABASE",
		DatabaseName:   dbName,
		Password:       encryptedPassword,
		Tables:         tables,
		FieldMasking:   fieldMasking,
		DBUser:         dbUserName,
		Privilege:      privilege,
	}

	//Call the API
	credentialID, err := CallCreateDatabaseCredentialAPI(databaseCredentialRequest)
	if err != nil {
		log.Printf("Error while calling Create Database Credential API: %v", err)
		return false, err
	}
	log.Default().Println("The cred id is:", credentialID)
	//call policy credential mapping
	err = CallPolicyCredentialMapping(orgId, policyID, tenantId, credentialID)
	if err != nil {
		log.Printf("Error while calling Update Policy Credential Mapping API: %v", err)
		return false, err
	}
	//Step 4 : Return True if the password is updated successfully
	return true, nil
}

func GenerateRandomPassword(length int) (string, error) {
	// Generate a random password of the given length
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()_+[]{}|;:,.<>?"
	b := make([]byte, length)
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano())) // Seed the random number generator
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))] // Select a random character from the charset
	}
	return string(b), nil
}
func CallCreateDatabaseCredentialAPI(databaseCredentialRequest CreateDatabaseCredentialRequestDto) (int, error) {
	log.Default().Println("Entered CallCreateDatabaseCredentialAPI")
	client := &http.Client{}
	//Marshal the request body
	databaseCredentialRequestBytes, err := json.Marshal(databaseCredentialRequest)
	if err != nil {
		return 0, errors.New("failed to marshal request body: " + err.Error())
	}
	log.Default().Println("Successfully Marshalled Request Body")
	url := "https://dev.api.authnull.com/api/v1/credential/createDatabaseCredential"
	log.Default().Println("URL", url)
	//Create the request
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(databaseCredentialRequestBytes))
	if err != nil {
		return 0, errors.New("failed to create request: " + err.Error())
	}
	log.Default().Println("Successfully Created Request")
	req.Header.Set("Content-Type", "application/json")
	log.Default().Println("Successfully Set Header", req)
	//Execute the request
	resp, err := client.Do(req)
	if err != nil {
		return 0, errors.New("failed to execute request: " + err.Error())
	}
	defer resp.Body.Close()
	log.Default().Println("response Status:", resp.Status)
	log.Default().Println("response :", resp)
	var response CreateDatabaseCredentialResponseDto
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return 0, errors.New("failed to decode response: " + err.Error())
	}

	log.Default().Println("Successfully received credential ID:", response.CredentialId)

	return response.CredentialId, nil
}

//func call to call policy credential mapping from policy-service
//payload will be the orgid,tenantid,policyid and credential id

func CallPolicyCredentialMapping(orgId int, policyId uuid.UUID, tenantId int, credentialId int) error {
	payload := struct {
		OrgId        int       `json:"org_id"`
		TenantId     int       `json:"tenant_id"`
		PolicyId     uuid.UUID `json:"policy_id"`
		CredentialId int       `json:"credential_id"`
	}{
		OrgId:        orgId,
		TenantId:     tenantId,
		PolicyId:     policyId,
		CredentialId: credentialId,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return errors.New("failed to marshal: " + err.Error())
	}
	log.Default().Println(string(jsonData))
	client := &http.Client{}
	url := "https://dev.api.authnull.com/api/v1/policyService/updatePolicyCredentialMapping"
	log.Default().Println("URL", url)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return errors.New("failed to create request: " + err.Error())
	}
	log.Default().Println("Successfully Created Request")
	req.Header.Set("Content-Type", "application/json")
	log.Default().Println("Successfully Set Header", req)
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("failed to execute request: " + err.Error())
	}
	defer resp.Body.Close()
	log.Default().Println("response Status:", resp.Status)
	log.Default().Println("response :", resp)
	return nil
}
