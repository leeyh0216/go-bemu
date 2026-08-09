package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// LoadMutationID is stable for one job identity and immutable configuration.
// It is stored with the engine-side physical receipt so retries cannot apply
// the same load twice.
func LoadMutationID(reference JobReference, configurationDigest string) (string, error) {
	if err := validateReference(reference); err != nil {
		return "", err
	}
	if !ValidLoadMutationID(configurationDigest) {
		return "", fmt.Errorf("%w: invalid load configuration digest", ErrInvalid)
	}
	payload, err := json.Marshal(struct {
		ProjectID           string `json:"projectId"`
		Location            string `json:"location"`
		JobID               string `json:"jobId"`
		ConfigurationDigest string `json:"configurationDigest"`
	}{
		ProjectID: reference.ProjectID, Location: reference.Location, JobID: reference.JobID,
		ConfigurationDigest: configurationDigest,
	})
	if err != nil {
		return "", fmt.Errorf("%w: encode load mutation identity", ErrInvalid)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func ValidLoadMutationID(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}
