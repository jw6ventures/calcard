package ui

import (
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"time"
)

//go:embed templates/*
var templateFS embed.FS

var templates = mustParseTemplates()

var funcMap = template.FuncMap{
	"formatTime": func(t interface{}) string {
		switch v := t.(type) {
		case nil:
			return ""
		case time.Time:
			if v.IsZero() {
				return ""
			}
			return v.UTC().Format(time.RFC3339)
		case *time.Time:
			if v == nil {
				return ""
			}
			return v.UTC().Format(time.RFC3339)
		}
		return ""
	},
	"formatDate": func(t interface{}) string {
		switch v := t.(type) {
		case nil:
			return ""
		case time.Time:
			if v.IsZero() {
				return ""
			}
			return v.Format("Jan 2, 2006")
		case *time.Time:
			if v == nil {
				return ""
			}
			return v.Format("Jan 2, 2006")
		}
		return ""
	},
	"formatDateTime": func(t interface{}) string {
		switch v := t.(type) {
		case nil:
			return ""
		case time.Time:
			if v.IsZero() {
				return ""
			}
			return v.Format("Jan 2, 2006 3:04 PM")
		case *time.Time:
			if v == nil {
				return ""
			}
			return v.Format("Jan 2, 2006 3:04 PM")
		}
		return ""
	},
	"relativeTime": func(t interface{}) string {
		var timestamp time.Time
		switch v := t.(type) {
		case nil:
			return ""
		case time.Time:
			if v.IsZero() {
				return ""
			}
			timestamp = v
		case *time.Time:
			if v == nil {
				return ""
			}
			timestamp = *v
		default:
			return ""
		}

		diff := time.Since(timestamp)
		if diff < time.Minute {
			return "just now"
		}
		if diff < time.Hour {
			mins := int(diff.Minutes())
			if mins == 1 {
				return "1m ago"
			}
			return fmt.Sprintf("%dm ago", mins)
		}
		if diff < 24*time.Hour {
			hours := int(diff.Hours())
			if hours == 1 {
				return "1h ago"
			}
			return fmt.Sprintf("%dh ago", hours)
		}
		if diff < 7*24*time.Hour {
			days := int(diff.Hours() / 24)
			if days == 1 {
				return "1d ago"
			}
			return fmt.Sprintf("%dd ago", days)
		}
		return timestamp.Format("Jan 2")
	},
	"truncate": func(s string, length int) string {
		if len(s) <= length {
			return s
		}
		return s[:length] + "..."
	},
}

func mustParseTemplates() map[string]*template.Template {
	files, err := fs.Glob(templateFS, "templates/*.html")
	if err != nil {
		panic(err)
	}

	base := template.Must(template.New("base.html").Funcs(funcMap).ParseFS(templateFS, "templates/base.html"))

	sets := make(map[string]*template.Template)
	for _, file := range files {
		if file == "templates/base.html" {
			continue
		}

		set := template.Must(base.Clone())
		template.Must(set.ParseFS(templateFS, file))
		sets[file[len("templates/"):]] = set
	}

	return sets
}
