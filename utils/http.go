package utils

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
)

func GetPublicIP() (string, error) {
	resp, err := http.Get("https://api.ipify.org")
	if err != nil {
		return "localhost", err
	}
	defer resp.Body.Close()

	ip, err := io.ReadAll(resp.Body)
	if err != nil {
		return "localhost", err
	}

	return string(ip), nil
}

func GetPrivateIP() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "", err
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String(), nil
}

func DecryptPassword(enc, key string) (string, error) {
	if !strings.HasPrefix(enc, "ENC(") {
		return enc, nil
	}
	enc = strings.TrimSuffix(strings.TrimPrefix(enc, "ENC("), ")")
	// Neither the ciphertext nor the key is logged: together they are the password.
	log.Printf("Decrypting a stored database password (%d characters)", len(enc))

	cmd := decryptCommand(enc, key)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("openssl decrypt failed: %v: %s", err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

// decryptCommand runs openssl with the key in its environment, not on its command line, where any
// local user could read it in the process list while openssl runs.
func decryptCommand(enc, key string) *exec.Cmd {
	cmd := exec.Command(
		"openssl", "enc",
		"-aes-256-cbc",
		"-a", "-A", "-d",
		"-pbkdf2", "-iter", "100000",
		"-pass", "env:AUTHNULL_DECRYPT_KEY",
	)
	cmd.Env = append(os.Environ(), "AUTHNULL_DECRYPT_KEY="+key)
	cmd.Stdin = strings.NewReader(enc)
	return cmd
}
