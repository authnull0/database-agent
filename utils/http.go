package utils

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
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
	log.Printf("Decrypting password %s using key %s", enc, key)

	if !strings.HasPrefix(enc, "ENC(") {
		return enc, nil
	}

	enc = strings.TrimSuffix(strings.TrimPrefix(enc, "ENC("), ")")
	log.Printf("Stripped encrypted password: %s", enc)

	cmd := exec.Command(
		"openssl", "enc",
		"-aes-256-cbc",
		"-a", "-A", "-d",
		"-pbkdf2", "-iter", "100000",
		"-pass", "pass:"+key,
	)

	cmd.Stdin = strings.NewReader(enc)

	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("openssl decrypt failed: %v: %s", err, out)
	}
	//log.Printf("Decrypted password: %s", out)
	return strings.TrimSpace(string(out)), nil
}
