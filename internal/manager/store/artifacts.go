package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/agent-parley/parley/internal/shared/report"
)

func (s *Store) SaveReportArtifact(ctx context.Context, rep report.Report) (Artifact, error) {
	if err := rep.Validate(); err != nil {
		return Artifact{}, fmt.Errorf("validate report before artifact write: %w", err)
	}
	content, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return Artifact{}, fmt.Errorf("marshal report artifact: %w", err)
	}
	return s.SaveArtifact(ctx, rep.RunID, "report", "application/json", append(content, '\n'), ".json")
}
