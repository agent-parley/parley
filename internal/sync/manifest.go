package sync

// Manifest is the future executor-handoff contract. It is intentionally
// schema-first: code state moves through Git, while Parley-owned state moves
// through explicit include/exclude manifests.
type Manifest struct {
	ID                    string     `json:"id"`
	SourceExecutorID      string     `json:"source_executor_id"`
	DestinationExecutorID string     `json:"destination_executor_id"`
	ProjectID             string     `json:"project_id"`
	RunID                 string     `json:"run_id,omitempty"`
	ExpectedGitRemote     string     `json:"expected_git_remote,omitempty"`
	ExpectedGitRef        string     `json:"expected_git_ref,omitempty"`
	Included              []Item     `json:"included"`
	Excluded              []Exclusion `json:"excluded"`
}

type Item struct {
	Kind        string `json:"kind"`
	RelativePath string `json:"relative_path"`
	SHA256      string `json:"sha256,omitempty"`
	Sensitivity string `json:"sensitivity"`
}

type Exclusion struct {
	RelativePath string `json:"relative_path"`
	Reason       string `json:"reason"`
}

var HardExclusions = []string{
	"credentials",
	"tokens",
	"ssh-keys",
	"raw-agent-sessions",
	".env",
	"container-sockets",
	"host-paths",
	"raw-sqlite-database",
	"parley.db",
	"parley.db-wal",
	"parley.db-shm",
	"secret-suspected-artifacts",
}
