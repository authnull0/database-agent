package utils

import (
	"bytes"
	"fmt"
	"io"
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

func DecryptPassword(enc, key string) (string, error) {
	if !strings.HasPrefix(enc, "ENC(") {
		return enc, nil
	}

	enc = strings.TrimSuffix(strings.TrimPrefix(enc, "ENC("), ")")

	cmd := exec.Command(
		"openssl", "enc", "-aes-256-cbc", "-a", "-d",
		"-pbkdf2", "-iter", "100000",
		"-pass", fmt.Sprintf("pass:%s", key),
	)

	cmd.Stdin = bytes.NewBufferString(enc)

	out, err := cmd.Output()
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(out)), nil
}
