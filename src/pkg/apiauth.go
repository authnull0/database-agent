package pkg

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// The agent's identity to authnull-service: the DB install's credential.
//
// The console writes PROXY_CLIENT_ID / PROXY_CLIENT_SECRET into db.env for each install -- the
// agent and ProxySQL on one host. The agent exchanges them for a short-lived token at
// /api/v1/databaseService/proxy/token and sends it as "Authorization: Bearer" on every call
// (doAPI). The control plane binds the token to the install's org and tenant.
//
// Without a credential in db.env the agent calls with no header, as it always did; the control
// plane admits that only while DB_AGENT_ALLOW_UNAUTHENTICATED is on. A credential it refuses is
// logged and the call goes without a token, so the agent never stops on its own account: the
// control plane decides.

// tokenRefreshMargin: a token closer than this to expiring is exchanged again before use.
const tokenRefreshMargin = 2 * time.Minute

var installToken = struct {
	sync.Mutex
	api, clientID, clientSecret string
	token                       string
	expires                     time.Time
}{}

// SetInstallCredential records the install's credential, once, at start.
func SetInstallCredential(api, clientID, clientSecret string) {
	installToken.Lock()
	defer installToken.Unlock()
	installToken.api = strings.TrimRight(strings.TrimSpace(api), "/")
	installToken.clientID = strings.TrimSpace(clientID)
	installToken.clientSecret = strings.TrimSpace(clientSecret)
	installToken.token, installToken.expires = "", time.Time{}
	if installToken.clientID == "" || installToken.clientSecret == "" {
		log.Printf("No PROXY_CLIENT_ID / PROXY_CLIENT_SECRET in db.env: calling the control plane without a credential. " +
			"Download a fresh db.env for this install from the console.")
		installToken.clientID, installToken.clientSecret = "", ""
	}
}

// currentToken is a usable token, exchanging the credential when there is none or it is about to
// expire. "" with no error means no credential is configured.
func currentToken() (string, error) {
	installToken.Lock()
	defer installToken.Unlock()
	if installToken.clientID == "" {
		return "", nil
	}
	if installToken.token != "" && time.Until(installToken.expires) > tokenRefreshMargin {
		return installToken.token, nil
	}
	tok, exp, err := exchangeToken(installToken.api, installToken.clientID, installToken.clientSecret)
	if err != nil {
		installToken.token = ""
		return "", err
	}
	installToken.token, installToken.expires = tok, exp
	return tok, nil
}

// dropToken forgets a token the control plane refused, so the next call exchanges again.
func dropToken(sent string) {
	installToken.Lock()
	defer installToken.Unlock()
	if installToken.token == sent {
		installToken.token = ""
	}
}

// exchangeToken trades the credential for a token. Neither the secret nor the token is logged.
func exchangeToken(api, clientID, clientSecret string) (string, time.Time, error) {
	body, _ := json.Marshal(map[string]string{"clientId": clientID, "clientSecret": clientSecret})
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Post(api+"/api/v1/databaseService/proxy/token",
		"application/json", bytes.NewReader(body))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("token exchange: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", time.Time{}, fmt.Errorf("token exchange: the control plane answered %s for client %s", resp.Status, clientID)
	}
	var out struct {
		AccessToken string `json:"accessToken"`
		ExpiresIn   int    `json:"expiresIn"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || out.AccessToken == "" {
		return "", time.Time{}, errors.New("token exchange: the answer has no token")
	}
	ttl := time.Duration(out.ExpiresIn) * time.Second
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return out.AccessToken, time.Now().Add(ttl), nil
}

// doAPI sends one request to the control plane with the install's token.
func doAPI(client *http.Client, req *http.Request) (*http.Response, error) {
	tok, err := currentToken()
	if err != nil {
		log.Printf("Calling %s without a credential: %v", req.URL.Path, err)
	}
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := client.Do(req)
	if err == nil && resp.StatusCode == http.StatusUnauthorized && tok != "" {
		dropToken(tok)
	}
	return resp, err
}
