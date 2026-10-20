package ical

import (
	"fmt"
	"strings"
)

// maxComponentDepth bounds how many components may nest inside one another,
// VCALENDAR counting as the first level. A real calendar object nests at most
// about four deep (VCALENDAR, VEVENT, VALARM, and an RFC 9073 PARTICIPANT or
// VLOCATION inside that); sixteen leaves ample room above that while keeping
// a maliciously deep body, whose nesting a strict reader would otherwise walk
// and a lenient one would otherwise build, cheap to read.
const maxComponentDepth = 16

// ReadComponents reads the component structure of an iCalendar object from its
// unfolded lines. open is called for each component the reader keeps, with the
// kept component it is nested in, or nil for the root VCALENDAR, and with the
// component's name upper-cased; it returns the component's handle, or nil to
// leave the component and everything nested in it out. content is called for
// each content line of a kept component, as written. Blank lines are skipped,
// and a line is a BEGIN or END only when it starts with one.
//
// With strict set, structure is not forgiven: one VCALENDAR root holding every
// other component, each BEGIN paired with an END of the same name, and no
// content outside the root, or an error. Otherwise the reader reads past every
// structural defect a stored object can carry, so every reader of a stored
// object sees the same components:
//   - an END closes the innermost open component of that name and everything
//     opened inside it, and an END naming no open component is skipped;
//   - components still open when the lines run out are closed there;
//   - a further VCALENDAR, whether after the first or nested anywhere inside
//     it, continues the first, and content outside any VCALENDAR is skipped.
//
// It fails, whatever strict says, when there is no VCALENDAR, and with the
// first error open or content returns.
//
// Nesting past maxComponentDepth levels, VCALENDAR counting as the first, is
// refused with strict set. Read leniently, a component at that depth is kept,
// but nothing BEGIN opens past it is: open and content are not called for it
// or anything nested inside it, and its END, and every END and BEGIN inside
// it, are matched by count rather than by name, so a component before or
// after it, at or above the limit, still reads normally.
func ReadComponents[N any](lines []string, strict bool, open func(parent *N, name string) (*N, error), content func(component *N, line string) error) (*N, error) {
	return readComponents(lines, strict,
		func(parent *N, name string, _ int) (*N, error) { return open(parent, name) },
		content, nil)
}

// readComponents is ReadComponents telling open the index of the BEGIN line of
// each component, and, when closed is not nil, telling closed of each kept
// component other than the root the index just past the line that closes it:
// its END line, the END line of a component it is nested in, or the end of
// lines.
func readComponents[N any](lines []string, strict bool, open func(parent *N, name string, at int) (*N, error), content func(component *N, line string) error, closed func(component *N, end int)) (*N, error) {
	type openComponent struct {
		name string
		// node is the component's handle, or nil for one left out.
		node *N
	}
	var stack []openComponent
	var root *N
	closeFrom := func(depth, end int) {
		if closed != nil {
			for i := len(stack) - 1; i >= depth; i-- {
				if node := stack[i].node; node != nil && node != root {
					closed(node, end)
				}
			}
		}
		stack = stack[:depth]
	}
	// skippedDepth counts, in lenient mode, the BEGIN lines nested past
	// maxComponentDepth that have not yet been closed. Nothing in that
	// subtree is built or passed to content, so neither open nor the ToUpper
	// a BEGIN line's name would otherwise cost is paid for it; its END lines,
	// and any BEGIN and END nested inside it, only balance the count, since
	// nothing there is read well enough to match them by name.
	skippedDepth := 0
	for at, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if skippedDepth > 0 {
			switch {
			case HasPrefixFold(line, "BEGIN:"):
				skippedDepth++
			case HasPrefixFold(line, "END:"):
				skippedDepth--
			}
			continue
		}
		switch {
		case HasPrefixFold(line, "BEGIN:") && len(stack) >= maxComponentDepth:
			if strict {
				return nil, fmt.Errorf("component nesting deeper than %d levels", maxComponentDepth)
			}
			skippedDepth = 1
		case HasPrefixFold(line, "BEGIN:"):
			name := strings.TrimSpace(strings.ToUpper(line[len("BEGIN:"):]))
			var node *N
			switch {
			case strict && len(stack) == 0 && root != nil:
				return nil, fmt.Errorf("more than one VCALENDAR root")
			case strict && len(stack) == 0 && name != "VCALENDAR":
				return nil, fmt.Errorf("expected one VCALENDAR root")
			case strict && len(stack) > 0 && name == "VCALENDAR":
				return nil, fmt.Errorf("nested VCALENDAR component")
			case name == "VCALENDAR" && root != nil:
				// Reopening the root rather than nesting a second one keeps
				// what it holds where its readers look for it. The next
				// END:VCALENDAR closes this opening, together with whatever
				// was opened inside it.
				node = root
			case name == "VCALENDAR":
				var err error
				if node, err = open(nil, name, at); err != nil {
					return nil, err
				}
				root = node
			case len(stack) > 0 && stack[len(stack)-1].node != nil:
				var err error
				if node, err = open(stack[len(stack)-1].node, name, at); err != nil {
					return nil, err
				}
			}
			stack = append(stack, openComponent{name: name, node: node})
		case HasPrefixFold(line, "END:"):
			name := strings.TrimSpace(strings.ToUpper(line[len("END:"):]))
			if strict {
				if len(stack) == 0 {
					return nil, fmt.Errorf("END:%s without matching BEGIN", name)
				}
				if stack[len(stack)-1].name != name {
					return nil, fmt.Errorf("mismatched tags: BEGIN:%s ... END:%s", stack[len(stack)-1].name, name)
				}
			}
			for i := len(stack) - 1; i >= 0; i-- {
				if stack[i].name == name {
					closeFrom(i, at+1)
					break
				}
			}
		default:
			if len(stack) == 0 {
				if strict {
					return nil, fmt.Errorf("content outside VCALENDAR root")
				}
				continue
			}
			if node := stack[len(stack)-1].node; node != nil {
				if err := content(node, line); err != nil {
					return nil, err
				}
			}
		}
	}
	if strict && len(stack) != 0 {
		return nil, fmt.Errorf("unbalanced tags: BEGIN:%s without matching END", stack[len(stack)-1].name)
	}
	closeFrom(0, len(lines))
	if root == nil {
		return nil, fmt.Errorf("missing BEGIN:VCALENDAR")
	}
	return root, nil
}

// HasPrefixFold reports whether s begins with prefix under Unicode case
// folding, comparing only the bytes prefix spans so nothing is copied. A
// content line's name is ASCII (RFC 5545 §3.1), which is what prefix is meant
// to hold.
func HasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}
