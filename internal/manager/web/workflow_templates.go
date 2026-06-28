package web

import (
	"github.com/agent-parley/parley/internal/manager/agentregistry"
	"github.com/agent-parley/parley/internal/manager/workflow"
	"github.com/agent-parley/parley/internal/shared/contract"
)

type WorkflowTemplatesData struct {
	Templates     []WorkflowTemplateSummaryData
	Notifications NotificationCenterData
	Notice        *Notice
	CSRF          string
	Title         string
}

type WorkflowTemplateSummaryData struct {
	ID          string
	Name        string
	Description string
	Predefined  bool
	Recommended bool
	Editable    bool
	StageCount  int
	CopyPath    string
	EditPath    string
}

type WorkflowTemplateEditData struct {
	Template      workflow.Template
	Settings      WorkflowTemplateSettingsData
	StageRows     []WorkflowTemplateStageRowData
	ReviewTargets []contract.ReviewTargetOption
	AgentProfiles []agentregistry.Profile
	SavePath      string
	Notifications NotificationCenterData
	Notice        *Notice
	Error         string
	CSRF          string
	Title         string
}

type WorkflowTemplateSettingsData struct {
	BranchPolicy    string
	PRBehavior      string
	MergePolicy     string
	RequiredChecks  string
	ForgeCredential string
	FixLoop         bool
	MaxFixLoops     int
}

type WorkflowTemplateStageRowData struct {
	ID              string
	Type            string
	Label           string
	Actor           string
	Target          string
	Order           int
	Enabled         bool
	Existing        bool
	Mandatory       bool
	Disableable     bool
	Reorderable     bool
	Agent           bool
	Review          bool
	Instructions    string
	ProfileID       string
	Required        bool
	ContextSettings string
	Timeout         string
	MaxAttempts     int
	Profile         string
	Intensity       string
}
