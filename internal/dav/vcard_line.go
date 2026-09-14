package dav

import "strings"

// parseVCardLine splits one unfolded content line into its group, name,
// parameters, and raw value. Parameter values may be quoted and quoted values
// may contain the ";", ":" and "," delimiters, so the split honours quoting
// rather than scanning for the first delimiter byte.
func parseVCardLine(line string) (vcardLine, bool) {
	colon := delimiterOutsideQuotes(line, ':')
	if colon <= 0 {
		return vcardLine{}, false
	}
	segments, ok := splitOutsideQuotes(line[:colon], ';')
	if !ok {
		return vcardLine{}, false
	}
	name := strings.TrimSpace(segments[0])
	parsed := vcardLine{value: line[colon+1:]}
	if dot := strings.IndexByte(name, '.'); dot >= 0 {
		parsed.group = name[:dot]
		name = name[dot+1:]
	}
	parsed.name = name
	if parsed.name == "" {
		return vcardLine{}, false
	}
	for _, segment := range segments[1:] {
		if strings.TrimSpace(segment) == "" {
			continue
		}
		paramName, paramValue, named := strings.Cut(segment, "=")
		if !named {
			// vCard 2.1 shorthand still found in stored data: a bare parameter
			// value is a TYPE value.
			parsed.addParamValue("TYPE", strings.TrimSpace(segment))
			continue
		}
		paramKey := strings.ToUpper(strings.TrimSpace(paramName))
		values, ok := splitOutsideQuotes(paramValue, ',')
		if !ok {
			values = []string{paramValue}
		}
		for _, value := range values {
			parsed.addParamValue(paramKey, unquoteParamValue(value))
		}
	}
	return parsed, true
}

func (l vcardLine) String() string {
	var b strings.Builder
	if l.group != "" {
		b.WriteString(l.group)
		b.WriteByte('.')
	}
	b.WriteString(l.name)
	for _, param := range l.params {
		b.WriteByte(';')
		b.WriteString(param.name)
		b.WriteByte('=')
		for i, value := range param.values {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(quoteParamValue(value))
		}
	}
	b.WriteByte(':')
	b.WriteString(l.value)
	return b.String()
}

func (l *vcardLine) paramValues(name string) []string {
	for _, param := range l.params {
		if param.name == name {
			return param.values
		}
	}
	return nil
}

func (l *vcardLine) hasParamValue(name, value string) bool {
	for _, candidate := range l.paramValues(name) {
		if strings.EqualFold(strings.TrimSpace(candidate), value) {
			return true
		}
	}
	return false
}

func (l *vcardLine) addParamValue(name, value string) {
	for i := range l.params {
		if l.params[i].name == name {
			l.params[i].values = append(l.params[i].values, value)
			return
		}
	}
	l.params = append(l.params, vcardLineParam{name: name, values: []string{value}})
}

func (l *vcardLine) setParamValues(name string, values []string) {
	if len(values) == 0 {
		l.removeParams(name)
		return
	}
	for i := range l.params {
		if l.params[i].name == name {
			l.params[i].values = values
			return
		}
	}
	l.params = append(l.params, vcardLineParam{name: name, values: values})
}

func (l *vcardLine) removeParams(names ...string) {
	kept := l.params[:0]
	for _, param := range l.params {
		drop := false
		for _, name := range names {
			if param.name == name {
				drop = true
				break
			}
		}
		if !drop {
			kept = append(kept, param)
		}
	}
	l.params = kept
}

// removeParamValues drops the named values from a parameter, removing the
// parameter entirely once nothing is left.
func (l *vcardLine) removeParamValues(name string, values ...string) {
	current := l.paramValues(name)
	if len(current) == 0 {
		return
	}
	kept := make([]string, 0, len(current))
	for _, candidate := range current {
		drop := false
		for _, value := range values {
			if strings.EqualFold(strings.TrimSpace(candidate), value) {
				drop = true
				break
			}
		}
		if !drop {
			kept = append(kept, candidate)
		}
	}
	l.setParamValues(name, kept)
}

func (l *vcardLine) replaceParamValue(name, from, to string) {
	current := l.paramValues(name)
	if len(current) == 0 {
		return
	}
	replaced := make([]string, len(current))
	copy(replaced, current)
	for i, value := range replaced {
		if strings.EqualFold(strings.TrimSpace(value), from) {
			replaced[i] = to
		}
	}
	l.setParamValues(name, replaced)
}

func unquoteParamValue(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= 2 && strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`) {
		return value[1 : len(value)-1]
	}
	return value
}

// quoteParamValue re-quotes a parameter value that contains a delimiter. RFC
// 6350 Section 3.3 gives quoted values no escape for the quote character
// itself, so an embedded quote can only be dropped.
func quoteParamValue(value string) string {
	if strings.ContainsAny(value, `,;:`) {
		return `"` + strings.ReplaceAll(value, `"`, "") + `"`
	}
	return value
}
