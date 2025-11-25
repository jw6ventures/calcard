package ui

import (
	"embed"
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
