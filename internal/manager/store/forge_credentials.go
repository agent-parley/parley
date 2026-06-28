package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/agent-parley/parley/internal/shared/ids"
)

const forgeCredentialsTable = "forge_credentials"

type ForgeCredential struct {
	ID               string
	Host             string
	SecretCiphertext []byte
	CreatedAt        string
	UpdatedAt        string
}

type ForgeCredentialInput struct {
	ID               string
	Host             string
	SecretCiphertext []byte
}

func ForgeCredentialSecretAD(rowID string) (table, column, id string) {
	return forgeCredentialsTable, "secret_ciphertext", strings.TrimSpace(rowID)
}

func (s *Store) InsertForgeCredential(ctx context.Context, input ForgeCredentialInput) (ForgeCredential, error) {
	input = normalizeForgeCredentialInput(input)
	if err := validateForgeCredentialInput(input); err != nil {
		return ForgeCredential{}, err
	}
	credentialID := strings.TrimSpace(input.ID)
	if credentialID == "" {
		credentialID = ids.New("fcr")
	}
	now := nowRFC3339()
	credential := ForgeCredential{
		ID:               credentialID,
		Host:             input.Host,
		SecretCiphertext: append([]byte(nil), input.SecretCiphertext...),
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `INSERT INTO forge_credentials(id, host, secret_ciphertext, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`, credential.ID, credential.Host, credential.SecretCiphertext, credential.CreatedAt, credential.UpdatedAt)
	if err != nil {
		return ForgeCredential{}, fmt.Errorf("insert forge credential: %w", err)
	}
	return credential, nil
}

func (s *Store) GetForgeCredential(ctx context.Context, credentialID string) (ForgeCredential, error) {
	credentialID = strings.TrimSpace(credentialID)
	if credentialID == "" {
		return ForgeCredential{}, fmt.Errorf("forge credential id is required")
	}
	row := s.db.QueryRowContext(ctx, `SELECT id, host, secret_ciphertext, created_at, updated_at FROM forge_credentials WHERE id = ?`, credentialID)
	credential, err := scanForgeCredential(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return ForgeCredential{}, fmt.Errorf("get forge credential %s: %w", credentialID, err)
		}
		return ForgeCredential{}, fmt.Errorf("scan forge credential %s: %w", credentialID, err)
	}
	return credential, nil
}

func (s *Store) ListForgeCredentials(ctx context.Context) ([]ForgeCredential, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, host, secret_ciphertext, created_at, updated_at FROM forge_credentials ORDER BY updated_at DESC, id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list forge credentials: %w", err)
	}
	defer rows.Close()
	var credentials []ForgeCredential
	for rows.Next() {
		credential, err := scanForgeCredential(rows)
		if err != nil {
			return nil, fmt.Errorf("scan forge credential: %w", err)
		}
		credentials = append(credentials, credential)
	}
	return credentials, rows.Err()
}

func (s *Store) DeleteForgeCredential(ctx context.Context, credentialID string) error {
	credentialID = strings.TrimSpace(credentialID)
	if credentialID == "" {
		return fmt.Errorf("forge credential id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.ExecContext(ctx, `DELETE FROM forge_credentials WHERE id = ?`, credentialID)
	if err != nil {
		return fmt.Errorf("delete forge credential: %w", err)
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete forge credential rows affected: %w", err)
	}
	if changed == 0 {
		return fmt.Errorf("get forge credential %s: %w", credentialID, sql.ErrNoRows)
	}
	return nil
}

type forgeCredentialScanner interface {
	Scan(dest ...any) error
}

func scanForgeCredential(scanner forgeCredentialScanner) (ForgeCredential, error) {
	var credential ForgeCredential
	if err := scanner.Scan(&credential.ID, &credential.Host, &credential.SecretCiphertext, &credential.CreatedAt, &credential.UpdatedAt); err != nil {
		return ForgeCredential{}, err
	}
	return credential, nil
}

func normalizeForgeCredentialInput(input ForgeCredentialInput) ForgeCredentialInput {
	input.ID = strings.TrimSpace(input.ID)
	input.Host = strings.ToLower(strings.TrimSpace(input.Host))
	if input.Host == "" {
		input.Host = "github.com"
	}
	return input
}

func validateForgeCredentialInput(input ForgeCredentialInput) error {
	if input.Host == "" {
		return fmt.Errorf("forge credential host is required")
	}
	if len(input.SecretCiphertext) == 0 {
		return fmt.Errorf("forge credential secret is required")
	}
	return nil
}
