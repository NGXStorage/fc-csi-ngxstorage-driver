package helper

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type FCVolumeState struct {
	SCSIID    string   `json:"scsi_id"`
	MapName   string   `json:"map_name,omitempty"`
	PathNames []string `json:"path_names,omitempty"`
}

func FCStatePath(dir, volumeID string) string {
	digest := sha256.Sum256([]byte(volumeID))
	return filepath.Join(dir, hex.EncodeToString(digest[:])+".json")
}

func SaveFCState(dir, volumeID string, state FCVolumeState) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create FC state directory: %w", err)
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal FC state: %w", err)
	}
	path := FCStatePath(dir, volumeID)
	temporary, err := os.CreateTemp(dir, ".fc-state-")
	if err != nil {
		return fmt.Errorf("create FC state temporary file: %w", err)
	}
	defer os.Remove(temporary.Name())
	if _, err := temporary.Write(raw); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write FC state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync FC state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close FC state: %w", err)
	}
	if err := os.Rename(temporary.Name(), path); err != nil {
		return fmt.Errorf("commit FC state: %w", err)
	}
	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open FC state directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync FC state directory: %w", err)
	}
	return nil
}

func LoadFCState(dir, volumeID string) (FCVolumeState, error) {
	raw, err := os.ReadFile(FCStatePath(dir, volumeID))
	if err != nil {
		return FCVolumeState{}, err
	}
	var state FCVolumeState
	if err := json.Unmarshal(raw, &state); err != nil {
		return FCVolumeState{}, fmt.Errorf("decode FC state: %w", err)
	}
	return state, nil
}

func DeleteFCState(dir, volumeID string) error {
	err := os.Remove(FCStatePath(dir, volumeID))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open FC state directory after deletion: %w", err)
	}
	defer directory.Close()
	return directory.Sync()
}
