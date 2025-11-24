package ui

import (
	"embed"
	"html/template"
	"time"
)

//go:embed templates/*
var templateFS embed.FS

var templates = template.Must(template.New("base.html").Funcs(template.FuncMap{
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
}).ParseFS(templateFS, "templates/*.html"))
