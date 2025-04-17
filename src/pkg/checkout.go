package pkg

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/authnull0/database-agent/utils"
)

type GetAllJobQueueRequest struct {
	OrgID    int    `json:"org_id"`
	TenantID int    `json:"tenant_id"`
	Host     string `json:"host"`
}
type GetAllJobQueueResponse struct {
	Code       string     `json:"code"`
	Status     string     `json:"status"`
	Message    string     `json:"message"`
	DbUserName string     `json:"db_user_name"`
	Data       []JobQueue `json:"data"`
}
type JobQueue struct {
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
	Privilege    string    `gorm:"column:privilege"`
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

func PollCheckoutJob(dbName string, Config DBConfig) error {
	//API call to get all jobs from the queue
	url := "https://dev.api.authnull.com/api/v1/databaseService/getJobQueue"
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
	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		log.Printf("Error while decoding response: %v", err)
		return err
	}
	if response.Code != "200" {
		log.Printf("Error in response: %s", response.Message)
		return fmt.Errorf("error in response: %s", response.Message)
	}
	log.Printf("Response: %s", response.Message)

	//Iterate through the jobs and process them
	for _, job := range response.Data {
		//Call Other Function to rotate the Password for the DB User in the Database

		success, err := GenerateCredentials(Config, dbName, response.DbUserName, job.Host, job.WalletUserID, job.IssuerID, job.Table_Name, job.Fields, job.Privilege)
		if err != nil {
			log.Printf("Error while generating credentials: %v", err)
			continue
		}
		if success {
			log.Printf("Credentials generated successfully for job: %s", job.JobName)
			//Call Update Job API to update the job status to completed
			updateJobURL := "https://dev.api.authnull.com/api/v1/databaseService/updateQueue"
			updateJobPayload := map[string]interface{}{
				"orgId":    orgID,
				"tenantId": tenantID,
				"jobId":    job.ID,
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
func GenerateCredentials(Config DBConfig, dbName string, dbUserName string, host string, WalletUserID int, IssuerId int, TableName string, Fields string, Privlege string) (bool, error) {
	//Rotate the Credentials for the DB User in the Database
	//Step1 : Generate a Random Password for the DB User
	password, err := GenerateRandomPassword(16)
	if err != nil {
		log.Printf("Error while generating random password: %v", err)
		return false, err
	}
	//Connect to the database
	db, err := ConnectToDB(Config)
	if err != nil {
		log.Printf("Error while connecting to database: %v", err)
		return false, err
	}

	//Step2 : Update the Password for the DB User in the Database
	alterPasswdQuery := fmt.Sprintf("ALTER USER '%s'@'%s' IDENTIFIED BY '%s'", dbUserName, host, password)
	_, err = db.Exec(alterPasswdQuery)
	if err != nil {
		log.Printf("Error while updating password for user %s: %v", dbUserName, err)
		return false, err
	}
	log.Printf("Password for user %s updated successfully", dbUserName)

	//COnnect to ProxysqlDB
	proxySQLDB, err := ConnectToProxysqlDB(Config)
	if err != nil {
		log.Printf("Error while connecting to ProxySQL database: %v", err)
	}

	_, err = proxySQLDB.Exec(alterPasswdQuery)
	if err != nil {
		log.Printf("Error while updating password for user %s in ProxySQL: %v", dbUserName, err)
		return false, err
	}
	orgId, _ := strconv.Atoi(Config.OrgID)
	tenantId, _ := strconv.Atoi(Config.TenantID)
	//Step3 : Call Create Database Credential API
	//Create the request body
	databaseCredentialRequest := CreateDatabaseCredentialRequestDto{
		OrgId:          orgId,
		TenantId:       tenantId,
		WalletUserId:   WalletUserID,
		IssuerId:       IssuerId,
		Host:           host,
		CredentialType: "DATABASE",
		DatabaseName:   dbName,
		Password:       password,
		Tables:         []string{TableName},
		FieldMasking:   map[string][]string{TableName: {Fields}},
		DBUser:         dbUserName,
		Privilege:      []string{Privlege},
	}
	//Call the API
	err = CallCreateDatabaseCredentialAPI(databaseCredentialRequest)
	if err != nil {
		log.Printf("Error while calling Create Database Credential API: %v", err)
		return false, err
	}

	//Step 4 : Return True if the password is updated successfully
	return true, nil

}
func GenerateRandomPassword(length int) (string, error) {
	// Generate a random password of the given length
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()_+[]{}|;:,.<>?"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[i%len(charset)]
	}
	return string(b), nil
}
func CallCreateDatabaseCredentialAPI(databaseCredentialRequest CreateDatabaseCredentialRequestDto) error {
	log.Default().Println("Entered CallCreateDatabaseCredentialAPI")
	client := &http.Client{}
	//Marshal the request body
	databaseCredentialRequestBytes, err := json.Marshal(databaseCredentialRequest)
	if err != nil {
		return errors.New("failed to marshal request body: " + err.Error())
	}
	log.Default().Println("Successfully Marshalled Request Body")
	url := "https://dev.api.authnull.com/api/v1/credential/createDatabaseCredential"
	log.Default().Println("URL", url)
	//Create the request
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(databaseCredentialRequestBytes))
	if err != nil {
		return errors.New("failed to create request: " + err.Error())
	}
	log.Default().Println("Successfully Created Request")
	req.Header.Set("Content-Type", "application/json")
	log.Default().Println("Successfully Set Header", req)
	//Execute the request
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("failed to execute request: " + err.Error())
	}
	defer resp.Body.Close()
	log.Default().Println("response Status:", resp.Status)
	log.Default().Println("response :", resp)

	return nil
}
