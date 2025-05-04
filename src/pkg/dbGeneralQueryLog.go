package pkg

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
)

const logAPIEndpoint = "https://monitoring.authnull.com/logs"

var (
	logFilePath    = "/var/log/mysql/general.log"
	syslogFilePath = "/var/log/syslog"
	orgID          int
	tenantID       int
	sessionIDRegex = regexp.MustCompile(`User\s+(\S+)\s+connected\s+to\s+DB\s+\S+\s+with\s+session\s+ID\s+(\d+)`)
)

// LogEntry represents the structure of the log that will be sent to the monitoring API.
type LogEntry struct {
	Timestamp string `json:"timestamp"`
	ThreadID  string `json:"threadId"`
	Action    string `json:"action"`
	Query     string `json:"query"`
	OrgID     int    `json:"orgId"`
	TenantID  int    `json:"tenantId"`
	SessionID string `json:"sessionId,omitempty"`
}

// StartDBLogMonitor starts the log monitoring goroutine using the given OrgID and TenantID.
func StartDBLogMonitor(cfgOrgID int, cfgTenantID int) {
	orgID = cfgOrgID
	tenantID = cfgTenantID

	go func() {
		for {
			err := streamLogs()
			if err != nil {
				log.Printf("Log stream error: %v\nRetrying in 10s...", err)
				time.Sleep(10 * time.Second)
			}
		}
	}()
}

// streamLogs tails the MySQL general log file and sends new entries.
func streamLogs() error {
	file, err := os.Open(logFilePath)
	if err != nil {
		return fmt.Errorf("failed to open MySQL log file: %w", err)
	}
	defer file.Close()

	// Seek to the end so we only read new log lines
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek to end of log file: %w", err)
	}

	reader := bufio.NewReader(file)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		entry := parseLogLine(line)
		if entry == nil {
			continue
		}

		// Try to add SessionID
		if sessionID, err := extractSessionIDFromSyslog(); err == nil {
			entry.SessionID = sessionID
		}

		go func(entry LogEntry) {
			if err := sendLog(entry); err != nil {
				log.Printf("Failed to send log: %v", err)
			} else {
				log.Printf("Log sent: %s - %s", entry.Timestamp, entry.Query)
			}
		}(*entry)
	}
}

// parseLogLine parses a single log line into a LogEntry struct.
func parseLogLine(line string) *LogEntry {
	parts := strings.Fields(line)
	if len(parts) < 4 {
		return nil
	}

	timestamp := parts[0]
	threadID := parts[1]
	action := parts[2]
	query := strings.Join(parts[3:], " ")

	return &LogEntry{
		Timestamp: timestamp,
		ThreadID:  threadID,
		Action:    action,
		Query:     query,
		OrgID:     orgID,
		TenantID:  tenantID,
	}
}

// extractSessionIDFromSyslog attempts to find the most recent session ID from the syslog.
func extractSessionIDFromSyslog() (string, error) {
	file, err := os.Open(syslogFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to open syslog: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lastSessionID string
	for scanner.Scan() {
		line := scanner.Text()
		matches := sessionIDRegex.FindStringSubmatch(line)
		if len(matches) >= 3 {
			lastSessionID = matches[2] // Store latest session ID found
		}
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("error reading syslog: %v", err)
	}

	return lastSessionID, nil
}

// sendLog sends a LogEntry to the monitoring API endpoint.
func sendLog(logEntry LogEntry) error {
	jsonData, err := json.Marshal(logEntry)
	if err != nil {
		return fmt.Errorf("error marshaling log entry: %w", err)
	}

	req, err := http.NewRequest("POST", logAPIEndpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error creating HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending log to API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
