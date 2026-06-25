package secrets

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/crypto/chacha20poly1305"

	"github.com/agent-parley/parley/internal/manager/store"
)

func TestSealOpenRoundTripAndAADBinding(t *testing.T) {
	ctx := context.Background()
	svc, closeStore := newTestService(t, testKey(1))
	defer closeStore()

	ad := AssociatedData{Table: "external_notification_sinks", Column: "token_ciphertext", RowID: "sink_1"}
	plaintext := []byte("gotify-token")
	sealed, err := svc.Seal(ctx, plaintext, ad)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	opened, err := svc.Open(ctx, sealed, ad)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !bytes.Equal(opened, plaintext) {
		t.Fatalf("open = %q, want %q", opened, plaintext)
	}

	for _, wrongAD := range []AssociatedData{
		{Table: "other_table", Column: ad.Column, RowID: ad.RowID},
		{Table: ad.Table, Column: "other_column", RowID: ad.RowID},
		{Table: ad.Table, Column: ad.Column, RowID: "sink_2"},
	} {
		if _, err := svc.Open(ctx, sealed, wrongAD); !errors.Is(err, ErrAuthenticationFailed) {
			t.Fatalf("open with AD %+v error = %v, want ErrAuthenticationFailed", wrongAD, err)
		}
	}
}

func TestSealUsesFreshNonce(t *testing.T) {
	ctx := context.Background()
	svc, closeStore := newTestService(t, testKey(2))
	defer closeStore()

	ad := AssociatedData{Table: "external_notification_sinks", Column: "token_ciphertext", RowID: "sink_1"}
	plaintext := []byte("same plaintext")
	first, err := svc.Seal(ctx, plaintext, ad)
	if err != nil {
		t.Fatalf("seal first: %v", err)
	}
	second, err := svc.Seal(ctx, plaintext, ad)
	if err != nil {
		t.Fatalf("seal second: %v", err)
	}
	if bytes.Equal(first, second) {
		t.Fatal("two seals of same plaintext produced identical ciphertext")
	}
}

func TestEnvelopeDEKGeneratedOnceAndRewrapsWithoutDataRotation(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	key1 := testKey(3)
	key2 := testKey(4)

	st, err := store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	svc, err := New(ctx, st, Config{KEKBase64: keyBase64(key1)})
	if err != nil {
		t.Fatalf("new secrets: %v", err)
	}
	if !svc.Available() {
		t.Fatalf("secrets state = %s, err = %v", svc.State(), svc.Err())
	}
	ad := AssociatedData{Table: "external_notification_sinks", Column: "token_ciphertext", RowID: "sink_1"}
	sealed, err := svc.Seal(ctx, []byte("secret-value"), ad)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	meta1 := readKeyMetaForTest(t, st.DB())
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	st, err = store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	svc, err = New(ctx, st, Config{KEKBase64: keyBase64(key1)})
	if err != nil {
		t.Fatalf("new secrets after reopen: %v", err)
	}
	meta2 := readKeyMetaForTest(t, st.DB())
	if meta2.kekVersion != meta1.kekVersion || !bytes.Equal(meta2.wrappedDEK, meta1.wrappedDEK) {
		t.Fatal("existing DEK metadata changed on reopen with same KEK")
	}
	if got, err := svc.Open(ctx, sealed, ad); err != nil || string(got) != "secret-value" {
		t.Fatalf("open after reopen = %q, %v", got, err)
	}

	if err := svc.rewrapDEK(ctx, key2, 2); err != nil {
		t.Fatalf("rewrap: %v", err)
	}
	meta3 := readKeyMetaForTest(t, st.DB())
	if meta3.kekVersion != 2 {
		t.Fatalf("kek_version = %d, want 2", meta3.kekVersion)
	}
	if bytes.Equal(meta3.wrappedDEK, meta2.wrappedDEK) {
		t.Fatal("wrapped DEK did not change after rewrap")
	}
	if got, err := svc.Open(ctx, sealed, ad); err != nil || string(got) != "secret-value" {
		t.Fatalf("open after rewrap = %q, %v", got, err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store after rewrap: %v", err)
	}

	st, err = store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("reopen store after rewrap: %v", err)
	}
	svc, err = New(ctx, st, Config{KEKBase64: keyBase64(key2)})
	if err != nil {
		t.Fatalf("new secrets with rewrapped key: %v", err)
	}
	if got, err := svc.Open(ctx, sealed, ad); err != nil || string(got) != "secret-value" {
		t.Fatalf("open with rewrapped key = %q, %v", got, err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close rewrapped store: %v", err)
	}

	st, err = store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("reopen store with old key: %v", err)
	}
	defer st.Close()
	svc, err = New(ctx, st, Config{KEKBase64: keyBase64(key1)})
	if err != nil {
		t.Fatalf("new secrets with old key: %v", err)
	}
	if svc.Available() || svc.State() != StateKeyMismatch || !errors.Is(svc.Err(), ErrKeyMismatch) {
		t.Fatalf("old key state = available:%v state:%s err:%v, want key mismatch", svc.Available(), svc.State(), svc.Err())
	}
}

func TestKeyProviderResolutionOrderAndTurnkeyPermissions(t *testing.T) {
	ctx := context.Background()
	envKey := testKey(5)
	fileKey := testKey(6)
	dataDir := t.TempDir()
	keyFile := filepath.Join(t.TempDir(), "kek")
	if err := os.WriteFile(keyFile, []byte(keyBase64(fileKey)+"\n"), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}

	st, err := store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := New(ctx, st, Config{KEKBase64: keyBase64(envKey), KEKFile: keyFile}); err != nil {
		t.Fatalf("new secrets: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	st, err = store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	svc, err := New(ctx, st, Config{KEKFile: keyFile})
	if err != nil {
		t.Fatalf("new secrets with file key: %v", err)
	}
	if svc.Available() || svc.State() != StateKeyMismatch {
		t.Fatalf("file-only state after env+file init = available:%v state:%s, want key mismatch", svc.Available(), svc.State())
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close file-only store: %v", err)
	}

	turnkeyDir := t.TempDir()
	st, err = store.Open(ctx, turnkeyDir)
	if err != nil {
		t.Fatalf("open turnkey store: %v", err)
	}
	svc, err = New(ctx, st, Config{})
	if err != nil {
		t.Fatalf("new turnkey secrets: %v", err)
	}
	defer st.Close()
	if !svc.Available() {
		t.Fatalf("turnkey state = %s, err = %v", svc.State(), svc.Err())
	}
	assertMode(t, filepath.Join(turnkeyDir, "keys"), 0o700)
	assertMode(t, filepath.Join(turnkeyDir, "keys", "kek"), 0o600)
	content, err := os.ReadFile(filepath.Join(turnkeyDir, "keys", "kek"))
	if err != nil {
		t.Fatalf("read turnkey key: %v", err)
	}
	raw, err := base64.StdEncoding.DecodeString(string(bytes.TrimSpace(content)))
	if err != nil {
		t.Fatalf("decode turnkey key: %v", err)
	}
	if len(raw) != chacha20poly1305.KeySize {
		t.Fatalf("turnkey key length = %d, want %d", len(raw), chacha20poly1305.KeySize)
	}
}

func TestFailClosedMissingTurnkeyWithExistingDEKDoesNotOverwrite(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	st, err := store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := New(ctx, st, Config{}); err != nil {
		t.Fatalf("new turnkey secrets: %v", err)
	}
	metaBefore := readKeyMetaForTest(t, st.DB())
	keyPath := filepath.Join(dataDir, "keys", "kek")
	if err := os.Remove(keyPath); err != nil {
		t.Fatalf("remove turnkey key: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	st, err = store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st.Close()
	svc, err := New(ctx, st, Config{})
	if err != nil {
		t.Fatalf("new secrets without turnkey key: %v", err)
	}
	if svc.Available() || svc.State() != StateMissingKEK || !errors.Is(svc.Err(), ErrNoKEK) {
		t.Fatalf("missing key state = available:%v state:%s err:%v, want missing KEK", svc.Available(), svc.State(), svc.Err())
	}
	metaAfter := readKeyMetaForTest(t, st.DB())
	if metaAfter.kekVersion != metaBefore.kekVersion || !bytes.Equal(metaAfter.wrappedDEK, metaBefore.wrappedDEK) {
		t.Fatal("existing DEK metadata changed when turnkey KEK was missing")
	}
	if _, err := os.Stat(keyPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("turnkey key recreated with existing DEK; stat err = %v", err)
	}
}

func TestInvalidWrappedDEKMetadataIsDistinctFromWrongKEK(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	key := testKey(9)
	st, err := store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := New(ctx, st, Config{KEKBase64: keyBase64(key)}); err != nil {
		t.Fatalf("new secrets: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `UPDATE secrets_keymeta SET wrapped_dek = ? WHERE id = 1`, []byte("too-short")); err != nil {
		t.Fatalf("corrupt wrapped DEK: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	st, err = store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st.Close()
	svc, err := New(ctx, st, Config{KEKBase64: keyBase64(key)})
	if err != nil {
		t.Fatalf("new secrets with corrupted metadata: %v", err)
	}
	if svc.Available() || svc.State() != StateInvalidKeyMeta || !errors.Is(svc.Err(), ErrInvalidKeyMetadata) {
		t.Fatalf("corrupt metadata state = available:%v state:%s err:%v, want invalid key metadata", svc.Available(), svc.State(), svc.Err())
	}
}

func TestInvalidKeyMetaRowFailsClosedAndDoesNotOverwriteDEK(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	key := testKey(10)
	st, err := store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := New(ctx, st, Config{KEKBase64: keyBase64(key)}); err != nil {
		t.Fatalf("new secrets: %v", err)
	}
	metaBefore := readKeyMetaForTest(t, st.DB())
	if _, err := st.DB().ExecContext(ctx, `UPDATE secrets_keymeta SET kek_version = 0 WHERE id = 1`); err != nil {
		t.Fatalf("corrupt key metadata row: %v", err)
	}
	corruptMeta := readKeyMetaForTest(t, st.DB())
	if corruptMeta.kekVersion != 0 || !bytes.Equal(corruptMeta.wrappedDEK, metaBefore.wrappedDEK) {
		t.Fatal("test setup did not preserve wrapped DEK while corrupting metadata")
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	st, err = store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st.Close()
	svc, err := New(ctx, st, Config{KEKBase64: keyBase64(key)})
	if err != nil {
		t.Fatalf("new secrets with invalid key metadata row: %v", err)
	}
	if svc.Available() || svc.State() != StateInvalidKeyMeta || !errors.Is(svc.Err(), ErrInvalidKeyMetadata) {
		t.Fatalf("invalid key metadata row state = available:%v state:%s err:%v, want invalid key metadata", svc.Available(), svc.State(), svc.Err())
	}
	metaAfter := readKeyMetaForTest(t, st.DB())
	if metaAfter.kekVersion != corruptMeta.kekVersion || !bytes.Equal(metaAfter.wrappedDEK, corruptMeta.wrappedDEK) {
		t.Fatal("invalid key metadata row changed during fail-closed startup")
	}
}

func TestFailClosedWrongKEKDoesNotOverwriteDEK(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	key1 := testKey(7)
	key2 := testKey(8)
	st, err := store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := New(ctx, st, Config{KEKBase64: keyBase64(key1)}); err != nil {
		t.Fatalf("new secrets: %v", err)
	}
	metaBefore := readKeyMetaForTest(t, st.DB())
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	st, err = store.Open(ctx, dataDir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st.Close()
	svc, err := New(ctx, st, Config{KEKBase64: keyBase64(key2)})
	if err != nil {
		t.Fatalf("new secrets with wrong key: %v", err)
	}
	if svc.Available() || svc.State() != StateKeyMismatch || !errors.Is(svc.Err(), ErrKeyMismatch) {
		t.Fatalf("wrong key state = available:%v state:%s err:%v, want key mismatch", svc.Available(), svc.State(), svc.Err())
	}
	metaAfter := readKeyMetaForTest(t, st.DB())
	if metaAfter.kekVersion != metaBefore.kekVersion || !bytes.Equal(metaAfter.wrappedDEK, metaBefore.wrappedDEK) {
		t.Fatal("existing DEK metadata changed when wrong KEK was supplied")
	}
}

func TestXChaCha20Poly1305NewXContract(t *testing.T) {
	if _, err := chacha20poly1305.NewX(make([]byte, chacha20poly1305.KeySize-1)); err == nil {
		t.Fatal("NewX accepted short key")
	}
	aead, err := chacha20poly1305.NewX(make([]byte, chacha20poly1305.KeySize))
	if err != nil {
		t.Fatalf("NewX with 32-byte key: %v", err)
	}
	if aead.NonceSize() != chacha20poly1305.NonceSizeX || aead.NonceSize() != 24 {
		t.Fatalf("nonce size = %d, want 24", aead.NonceSize())
	}
	nonce := make([]byte, aead.NonceSize())
	ciphertext := aead.Seal(nil, nonce, []byte("plaintext"), []byte("aad-1"))
	if _, err := aead.Open(nil, nonce, ciphertext, []byte("aad-1")); err != nil {
		t.Fatalf("Open with original AAD: %v", err)
	}
	if _, err := aead.Open(nil, nonce, ciphertext, []byte("aad-2")); err == nil {
		t.Fatal("Open with different AAD succeeded")
	}
}

type keyMetaForTest struct {
	kekVersion int
	wrappedDEK []byte
}

func newTestService(t *testing.T, key [chacha20poly1305.KeySize]byte) (*Service, func()) {
	t.Helper()
	st, err := store.Open(context.Background(), t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	svc, err := New(context.Background(), st, Config{KEKBase64: keyBase64(key)})
	if err != nil {
		_ = st.Close()
		t.Fatalf("new secrets: %v", err)
	}
	if !svc.Available() {
		_ = st.Close()
		t.Fatalf("secrets state = %s, err = %v", svc.State(), svc.Err())
	}
	return svc, func() { _ = st.Close() }
}

func readKeyMetaForTest(t *testing.T, db *sql.DB) keyMetaForTest {
	t.Helper()
	var meta keyMetaForTest
	if err := db.QueryRow(`SELECT kek_version, wrapped_dek FROM secrets_keymeta WHERE id = 1`).Scan(&meta.kekVersion, &meta.wrappedDEK); err != nil {
		t.Fatalf("read key metadata: %v", err)
	}
	return meta
}

func testKey(seed byte) [chacha20poly1305.KeySize]byte {
	var key [chacha20poly1305.KeySize]byte
	for i := range key {
		key[i] = seed + byte(i)
	}
	return key
}

func keyBase64(key [chacha20poly1305.KeySize]byte) string {
	return base64.StdEncoding.EncodeToString(key[:])
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("mode %s = %#o, want %#o", path, got, want)
	}
}
