package secrets

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/chacha20poly1305"

	"github.com/agent-parley/parley/internal/manager/store"
)

const (
	defaultKEKVersion = 1
	keyMetaID         = 1
	turnkeyKeyDir     = "keys"
	turnkeyKeyFile    = "kek"
)

var (
	ErrUnavailable           = errors.New("secrets facility unavailable")
	ErrNoKEK                 = errors.New("secrets KEK unavailable")
	ErrKeyMismatch           = errors.New("secrets KEK mismatch")
	ErrInvalidKEK            = errors.New("invalid secrets KEK")
	ErrInvalidAssociatedData = errors.New("invalid secrets associated data")
	ErrAuthenticationFailed  = errors.New("secret authentication failed")
	ErrInvalidKeyMetadata    = errors.New("invalid secrets key metadata")
)

type State string

const (
	StateAvailable      State = "available"
	StateMissingKEK     State = "missing_kek"
	StateKeyMismatch    State = "key_mismatch"
	StateInvalidKeyMeta State = "invalid_key_metadata"
	StateProviderError  State = "provider_error"
)

type Config struct {
	KEKBase64 string
	KEKFile   string

	KeyProvider KeyProvider
}

type AssociatedData struct {
	Table  string
	Column string
	RowID  string
}

type KeyRequest struct {
	DataDir     string
	ExistingDEK bool
}

type KeyMaterial struct {
	Key     [chacha20poly1305.KeySize]byte
	Version int
	Source  string
}

type KeyProvider interface {
	ResolveKey(ctx context.Context, req KeyRequest) (KeyMaterial, error)
}

type Service struct {
	db      *sql.DB
	dataDir string

	mu        sync.RWMutex
	dek       [chacha20poly1305.KeySize]byte
	available bool
	state     State
	err       error
}

func New(ctx context.Context, st *store.Store, cfg Config) (*Service, error) {
	if st == nil {
		return nil, errors.New("initialize secrets: nil store")
	}
	svc := &Service{db: st.DB(), dataDir: st.DataDir(), state: StateMissingKEK, err: ErrNoKEK}

	meta, err := loadKeyMeta(ctx, svc.db)
	if err != nil {
		if errors.Is(err, ErrInvalidKeyMetadata) {
			svc.setUnavailable(StateInvalidKeyMeta, ErrInvalidKeyMetadata)
			return svc, nil
		}
		return nil, err
	}

	provider := cfg.KeyProvider
	if provider == nil {
		provider = defaultKeyProvider{cfg: cfg}
	}
	material, err := provider.ResolveKey(ctx, KeyRequest{DataDir: svc.dataDir, ExistingDEK: meta.exists})
	if err != nil {
		svc.setUnavailable(stateForProviderError(err), err)
		return svc, nil
	}
	if material.Version <= 0 {
		svc.setUnavailable(StateProviderError, ErrInvalidKEK)
		return svc, nil
	}

	if !meta.exists {
		var dek [chacha20poly1305.KeySize]byte
		if _, err := rand.Read(dek[:]); err != nil {
			return nil, fmt.Errorf("generate secrets DEK: %w", err)
		}
		wrapped, err := sealWithKey(material.Key, dek[:], dekWrapAAD(material.Version))
		if err != nil {
			return nil, fmt.Errorf("wrap secrets DEK: %w", err)
		}
		if err := insertKeyMeta(ctx, svc.db, material.Version, wrapped); err != nil {
			return nil, err
		}
		svc.setAvailable(dek)
		return svc, nil
	}

	dek, err := openDEK(material.Key, meta.wrappedDEK, meta.kekVersion)
	if err != nil {
		if errors.Is(err, ErrInvalidKeyMetadata) {
			svc.setUnavailable(StateInvalidKeyMeta, ErrInvalidKeyMetadata)
			return svc, nil
		}
		svc.setUnavailable(StateKeyMismatch, ErrKeyMismatch)
		return svc, nil
	}
	svc.setAvailable(dek)
	return svc, nil
}

func (s *Service) Available() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.available
}

func (s *Service) State() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

func (s *Service) Err() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.err
}

func (s *Service) Seal(ctx context.Context, plaintext []byte, ad AssociatedData) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	aad, err := ad.bytes()
	if err != nil {
		return nil, err
	}
	dek, err := s.currentDEK()
	if err != nil {
		return nil, err
	}
	return sealWithKey(dek, plaintext, aad)
}

func (s *Service) Open(ctx context.Context, ciphertext []byte, ad AssociatedData) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	aad, err := ad.bytes()
	if err != nil {
		return nil, err
	}
	dek, err := s.currentDEK()
	if err != nil {
		return nil, err
	}
	return openWithKey(dek, ciphertext, aad)
}

func (s *Service) currentDEK() ([chacha20poly1305.KeySize]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.available {
		if s.err != nil {
			return [chacha20poly1305.KeySize]byte{}, fmt.Errorf("%w: %w", ErrUnavailable, s.err)
		}
		return [chacha20poly1305.KeySize]byte{}, ErrUnavailable
	}
	return s.dek, nil
}

func (s *Service) setAvailable(dek [chacha20poly1305.KeySize]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dek = dek
	s.available = true
	s.state = StateAvailable
	s.err = nil
}

func (s *Service) setUnavailable(state State, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.available = false
	s.state = state
	s.err = err
}

func (s *Service) rewrapDEK(ctx context.Context, newKEK [chacha20poly1305.KeySize]byte, newVersion int) error {
	if newVersion <= 0 {
		return ErrInvalidKEK
	}
	dek, err := s.currentDEK()
	if err != nil {
		return err
	}
	wrapped, err := sealWithKey(newKEK, dek[:], dekWrapAAD(newVersion))
	if err != nil {
		return fmt.Errorf("wrap secrets DEK: %w", err)
	}
	now := nowRFC3339()
	res, err := s.db.ExecContext(ctx, `UPDATE secrets_keymeta SET kek_version = ?, wrapped_dek = ?, updated_at = ? WHERE id = ?`, newVersion, wrapped, now, keyMetaID)
	if err != nil {
		return fmt.Errorf("rewrap secrets DEK: %w", err)
	}
	if rows, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("rewrap secrets DEK rows affected: %w", err)
	} else if rows != 1 {
		return errors.New("rewrap secrets DEK: key metadata row missing")
	}
	return nil
}

func (ad AssociatedData) bytes() ([]byte, error) {
	if err := validateADElement("table", ad.Table); err != nil {
		return nil, err
	}
	if err := validateADElement("column", ad.Column); err != nil {
		return nil, err
	}
	if err := validateADElement("row id", ad.RowID); err != nil {
		return nil, err
	}
	return []byte("parley/v1/" + ad.Table + "/" + ad.Column + "/" + ad.RowID), nil
}

func validateADElement(_ string, value string) error {
	if value == "" || strings.TrimSpace(value) != value || strings.Contains(value, "/") || strings.ContainsRune(value, '\x00') {
		return ErrInvalidAssociatedData
	}
	return nil
}

type keyMeta struct {
	exists     bool
	kekVersion int
	wrappedDEK []byte
}

func loadKeyMeta(ctx context.Context, db *sql.DB) (keyMeta, error) {
	var meta keyMeta
	err := db.QueryRowContext(ctx, `SELECT kek_version, wrapped_dek FROM secrets_keymeta WHERE id = ?`, keyMetaID).Scan(&meta.kekVersion, &meta.wrappedDEK)
	if errors.Is(err, sql.ErrNoRows) {
		return keyMeta{}, nil
	}
	if err != nil {
		return keyMeta{}, fmt.Errorf("load secrets key metadata: %w", err)
	}
	if meta.kekVersion <= 0 || len(meta.wrappedDEK) == 0 {
		return keyMeta{}, fmt.Errorf("load secrets key metadata: %w", ErrInvalidKeyMetadata)
	}
	meta.exists = true
	return meta, nil
}

func insertKeyMeta(ctx context.Context, db *sql.DB, kekVersion int, wrappedDEK []byte) error {
	now := nowRFC3339()
	_, err := db.ExecContext(ctx, `INSERT INTO secrets_keymeta(id, kek_version, wrapped_dek, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`, keyMetaID, kekVersion, wrappedDEK, now, now)
	if err != nil {
		return fmt.Errorf("store secrets key metadata: %w", err)
	}
	return nil
}

func openDEK(kek [chacha20poly1305.KeySize]byte, wrapped []byte, version int) ([chacha20poly1305.KeySize]byte, error) {
	if len(wrapped) < chacha20poly1305.NonceSizeX+chacha20poly1305.Overhead {
		return [chacha20poly1305.KeySize]byte{}, ErrInvalidKeyMetadata
	}
	plaintext, err := openWithKey(kek, wrapped, dekWrapAAD(version))
	if err != nil {
		return [chacha20poly1305.KeySize]byte{}, err
	}
	if len(plaintext) != chacha20poly1305.KeySize {
		return [chacha20poly1305.KeySize]byte{}, ErrInvalidKeyMetadata
	}
	var dek [chacha20poly1305.KeySize]byte
	copy(dek[:], plaintext)
	return dek, nil
}

func sealWithKey(key [chacha20poly1305.KeySize]byte, plaintext, aad []byte) ([]byte, error) {
	aead, err := chacha20poly1305.NewX(key[:])
	if err != nil {
		return nil, fmt.Errorf("initialize secret cipher: %w", err)
	}
	nonce := make([]byte, chacha20poly1305.NonceSizeX)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generate secret nonce: %w", err)
	}
	out := make([]byte, len(nonce), len(nonce)+len(plaintext)+aead.Overhead())
	copy(out, nonce)
	out = aead.Seal(out, nonce, plaintext, aad)
	return out, nil
}

func openWithKey(key [chacha20poly1305.KeySize]byte, ciphertext, aad []byte) ([]byte, error) {
	aead, err := chacha20poly1305.NewX(key[:])
	if err != nil {
		return nil, fmt.Errorf("initialize secret cipher: %w", err)
	}
	if len(ciphertext) < chacha20poly1305.NonceSizeX+aead.Overhead() {
		return nil, ErrAuthenticationFailed
	}
	nonce := ciphertext[:chacha20poly1305.NonceSizeX]
	sealed := ciphertext[chacha20poly1305.NonceSizeX:]
	plaintext, err := aead.Open(nil, nonce, sealed, aad)
	if err != nil {
		return nil, ErrAuthenticationFailed
	}
	return plaintext, nil
}

func dekWrapAAD(version int) []byte {
	return []byte(fmt.Sprintf("parley/v1/dek-wrap/v%d", version))
}

type defaultKeyProvider struct {
	cfg Config
}

func (p defaultKeyProvider) ResolveKey(_ context.Context, req KeyRequest) (KeyMaterial, error) {
	if strings.TrimSpace(p.cfg.KEKBase64) != "" {
		key, err := decodeBase64Key(p.cfg.KEKBase64)
		if err != nil {
			return KeyMaterial{}, err
		}
		return KeyMaterial{Key: key, Version: defaultKEKVersion, Source: "env"}, nil
	}
	if strings.TrimSpace(p.cfg.KEKFile) != "" {
		key, err := readKeyFile(strings.TrimSpace(p.cfg.KEKFile))
		if err != nil {
			return KeyMaterial{}, err
		}
		return KeyMaterial{Key: key, Version: defaultKEKVersion, Source: "file"}, nil
	}
	key, err := resolveTurnkeyKey(req.DataDir, req.ExistingDEK)
	if err != nil {
		return KeyMaterial{}, err
	}
	return KeyMaterial{Key: key, Version: defaultKEKVersion, Source: "turnkey"}, nil
}

func decodeBase64Key(encoded string) ([chacha20poly1305.KeySize]byte, error) {
	trimmed := strings.TrimSpace(encoded)
	raw, err := base64.StdEncoding.DecodeString(trimmed)
	if err != nil {
		raw, err = base64.RawStdEncoding.DecodeString(trimmed)
	}
	if err != nil {
		return [chacha20poly1305.KeySize]byte{}, ErrInvalidKEK
	}
	return keyFromBytes(raw)
}

func readKeyFile(path string) ([chacha20poly1305.KeySize]byte, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return [chacha20poly1305.KeySize]byte{}, ErrNoKEK
		}
		return [chacha20poly1305.KeySize]byte{}, fmt.Errorf("read secrets KEK file: %w", err)
	}
	return keyFromFileContent(content)
}

func keyFromFileContent(content []byte) ([chacha20poly1305.KeySize]byte, error) {
	if len(content) == chacha20poly1305.KeySize {
		return keyFromBytes(content)
	}
	trimmed := bytes.TrimSpace(content)
	raw, err := base64.StdEncoding.DecodeString(string(trimmed))
	if err != nil {
		raw, err = base64.RawStdEncoding.DecodeString(string(trimmed))
	}
	if err != nil {
		return [chacha20poly1305.KeySize]byte{}, ErrInvalidKEK
	}
	return keyFromBytes(raw)
}

func keyFromBytes(raw []byte) ([chacha20poly1305.KeySize]byte, error) {
	if len(raw) != chacha20poly1305.KeySize {
		return [chacha20poly1305.KeySize]byte{}, ErrInvalidKEK
	}
	var key [chacha20poly1305.KeySize]byte
	copy(key[:], raw)
	return key, nil
}

func resolveTurnkeyKey(dataDir string, existingDEK bool) ([chacha20poly1305.KeySize]byte, error) {
	path := turnkeyPath(dataDir)
	if key, err := readTurnkeyKey(path); err == nil {
		return key, nil
	} else if !errors.Is(err, ErrNoKEK) {
		return [chacha20poly1305.KeySize]byte{}, err
	}
	if existingDEK {
		return [chacha20poly1305.KeySize]byte{}, ErrNoKEK
	}
	return createTurnkeyKey(path)
}

func readTurnkeyKey(path string) ([chacha20poly1305.KeySize]byte, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return [chacha20poly1305.KeySize]byte{}, ErrNoKEK
		}
		return [chacha20poly1305.KeySize]byte{}, fmt.Errorf("read turnkey secrets KEK: %w", err)
	}
	if err := os.Chmod(filepath.Dir(path), 0o700); err != nil {
		return [chacha20poly1305.KeySize]byte{}, fmt.Errorf("secure turnkey secrets key dir: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return [chacha20poly1305.KeySize]byte{}, fmt.Errorf("secure turnkey secrets KEK: %w", err)
	}
	return keyFromFileContent(content)
}

func createTurnkeyKey(path string) ([chacha20poly1305.KeySize]byte, error) {
	var key [chacha20poly1305.KeySize]byte
	if _, err := rand.Read(key[:]); err != nil {
		return [chacha20poly1305.KeySize]byte{}, fmt.Errorf("generate turnkey secrets KEK: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return [chacha20poly1305.KeySize]byte{}, fmt.Errorf("create turnkey secrets key dir: %w", err)
	}
	if err := os.Chmod(filepath.Dir(path), 0o700); err != nil {
		return [chacha20poly1305.KeySize]byte{}, fmt.Errorf("secure turnkey secrets key dir: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return readTurnkeyKey(path)
		}
		return [chacha20poly1305.KeySize]byte{}, fmt.Errorf("create turnkey secrets KEK: %w", err)
	}
	content := []byte(base64.StdEncoding.EncodeToString(key[:]) + "\n")
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		return [chacha20poly1305.KeySize]byte{}, fmt.Errorf("write turnkey secrets KEK: %w", err)
	}
	if err := file.Close(); err != nil {
		return [chacha20poly1305.KeySize]byte{}, fmt.Errorf("close turnkey secrets KEK: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return [chacha20poly1305.KeySize]byte{}, fmt.Errorf("secure turnkey secrets KEK: %w", err)
	}
	return key, nil
}

func turnkeyPath(dataDir string) string {
	return filepath.Join(dataDir, turnkeyKeyDir, turnkeyKeyFile)
}

func stateForProviderError(err error) State {
	if errors.Is(err, ErrNoKEK) {
		return StateMissingKEK
	}
	return StateProviderError
}

func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }
