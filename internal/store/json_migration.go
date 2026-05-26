package store

import (
	"encoding/json"
	"errors"
	"os"
)

func (s *Store) loadLegacyJSONState() (bool, error) {
	data, err := os.ReadFile(s.legacyJSONPath)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	legacy := newState()
	if err := json.Unmarshal(data, &legacy); err != nil {
		return false, err
	}
	s.state = legacy
	s.ensureStateMapsLocked()
	return true, nil
}
