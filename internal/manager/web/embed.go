package web

import (
	"bytes"
	"embed"
	"fmt"
	"html/template"
	"strings"

	"github.com/agent-parley/parley/internal/manager/store"
)

//go:embed templates/*.html assets/*
var Embedded embed.FS

type Renderer interface {
	ExecutePage(name string, data any) (string, error)
	RenderRunFragments(store.RunBundle) (string, error)
}

type TemplateRenderer struct {
	templates *template.Template
}

type IndexData struct {
	Runs  []store.Run
	CSRF  string
	Title string
}

type RunData struct {
	Bundle store.RunBundle
	CSRF   string
	Title  string
}

func NewRenderer() (*TemplateRenderer, error) {
	funcs := template.FuncMap{"short": short, "statusClass": statusClass}
	tmpl, err := template.New("").Funcs(funcs).ParseFS(Embedded, "templates/*.html")
	if err != nil {
		return nil, fmt.Errorf("parse templates: %w", err)
	}
	return &TemplateRenderer{templates: tmpl}, nil
}

func (r *TemplateRenderer) ExecutePage(name string, data any) (string, error) {
	var buf bytes.Buffer
	if err := r.templates.ExecuteTemplate(&buf, name, data); err != nil {
		return "", fmt.Errorf("execute template %s: %w", name, err)
	}
	return buf.String(), nil
}

func (r *TemplateRenderer) RenderRunFragments(bundle store.RunBundle) (string, error) {
	var buf bytes.Buffer
	for _, name := range []string{"run_summary.html", "stage_statuses.html", "event_log.html", "artifacts.html"} {
		if err := r.templates.ExecuteTemplate(&buf, name, bundle); err != nil {
			return "", fmt.Errorf("execute fragment %s: %w", name, err)
		}
	}
	return compactHTML(buf.String()), nil
}

func short(s string) string {
	if len(s) <= 14 {
		return s
	}
	return s[:14]
}

func statusClass(status string) string {
	switch status {
	case "completed":
		return "status-completed"
	case "failed", "invalid":
		return "status-failed"
	case "running":
		return "status-running"
	default:
		return "status-pending"
	}
}

func compactHTML(in string) string {
	lines := strings.Split(in, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			out = append(out, line)
		}
	}
	return strings.Join(out, "")
}
