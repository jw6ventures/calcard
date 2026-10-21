// Fails a template whose inline JavaScript builds an event handler out of data.
//
// An on* attribute is a JavaScript program written inside an HTML attribute
// value, and the attribute-value parser decodes character references before
// that program is compiled. escapeHtml turns an apostrophe into &#39;, the
// parser turns it back, and the value lands in the handler as an apostrophe --
// so a value escaped for markup is not inert in this one position, and no
// amount of escaping makes it so. Handlers are attached to elements instead,
// which is what this guards.
//
// A handler attribute written whole inside one string literal carries no data
// and is left alone. What is refused:
//   - a handler value still open when its string literal ends, because
//     whatever is concatenated on next becomes part of the program -- quoted
//     (onclick="edit(' + id + ')") or unquoted (onclick=edit(' + id + '));
//   - a handler value in a template literal that interpolates ${...};
//   - setAttribute with an on* name, which compiles its value the same way.
// Every markup sink -- innerHTML, outerHTML, insertAdjacentHTML,
// document.write -- is covered, because it is the string that is checked.
//
// Usage: node no_concatenated_handlers.mjs template.html
//        node no_concatenated_handlers.mjs --self-test
import fs from 'node:fs';
import assert from 'node:assert/strict';

// Collects the body of every string and template literal in a script, skipping
// comments and regular expression literals so their contents are not mistaken
// for strings (and their quotes not mistaken for delimiters). A template
// literal's body keeps its ${...} substitutions as written.
function stringLiterals(script) {
  const literals = [];
  const regexMayStart = (i) => {
    for (let j = i - 1; j >= 0; j--) {
      const c = script[j];
      if (c === ' ' || c === '\t' || c === '\n' || c === '\r') continue;
      return '(,=:[!&|?{};+-*%~^'.includes(c);
    }
    return true;
  };
  // Skips a quoted run starting at i and returns the index of its closer.
  const skipQuoted = (i) => {
    const quote = script[i];
    for (i++; i < script.length; i++) {
      if (script[i] === '\\') i++;
      else if (script[i] === quote) break;
    }
    return i;
  };
  for (let i = 0; i < script.length; i++) {
    const c = script[i];
    if (c === '/' && script[i + 1] === '/') {
      i = script.indexOf('\n', i);
      if (i < 0) break;
    } else if (c === '/' && script[i + 1] === '*') {
      i = script.indexOf('*/', i);
      if (i < 0) break;
      i++;
    } else if (c === '/' && regexMayStart(i)) {
      let inClass = false;
      for (i++; i < script.length; i++) {
        if (script[i] === '\\') {
          i++;
        } else if (script[i] === '[') {
          inClass = true;
        } else if (script[i] === ']') {
          inClass = false;
        } else if (script[i] === '\n' || (script[i] === '/' && !inClass)) {
          break;
        }
      }
    } else if (c === '"' || c === "'") {
      const start = i + 1;
      i = skipQuoted(i);
      literals.push({ body: script.slice(start, i), offset: start, template: false });
    } else if (c === '`') {
      const start = i + 1;
      for (i++; i < script.length; i++) {
        if (script[i] === '\\') {
          i++;
        } else if (script[i] === '`') {
          break;
        } else if (script[i] === '$' && script[i + 1] === '{') {
          let depth = 0;
          for (i++; i < script.length; i++) {
            if (script[i] === '{') depth++;
            else if (script[i] === '}' && --depth === 0) break;
            else if (script[i] === '"' || script[i] === "'" || script[i] === '`') i = skipQuoted(i);
          }
        }
      }
      literals.push({ body: script.slice(start, i), offset: start, template: true });
    }
  }
  return literals;
}

// The script text with comments blanked out, for the checks that read code
// rather than literals.
function withoutComments(script) {
  return script
    .replace(/\/\*[\s\S]*?\*\//g, (m) => m.replace(/[^\n]/g, ' '))
    .replace(/(^|[^:\\])\/\/[^\n]*/g, (m, lead) => lead + ' '.repeat(m.length - lead.length));
}

// An on* attribute inside markup starts a literal or follows whitespace or a
// closed attribute value, which keeps query strings such as ?onboarding= out.
const handlerAttribute = /(?:^|[\s"'\/])(on[a-z]+)\s*=\s*/gi;

function literalProblems(literal) {
  const problems = [];
  const { body } = literal;
  for (const match of body.matchAll(handlerAttribute)) {
    const name = match[1];
    const valueStart = match.index + match[0].length;
    let rest = body.slice(valueStart);
    let value;
    // The quote may be written escaped when it matches the literal's own
    // delimiter, and then its closing twin is escaped the same way.
    const quoted = /^(\\?)(["'])/.exec(rest);
    if (quoted) {
      rest = rest.slice(quoted[0].length);
      const closer = quoted[1] + quoted[2];
      const end = rest.indexOf(closer);
      if (end < 0) {
        problems.push(`the value of ${name} is still open when the string literal ends, so what is concatenated on next becomes part of the handler`);
        continue;
      }
      value = rest.slice(0, end);
    } else {
      const end = rest.search(/[\s>]/);
      if (end < 0) {
        problems.push(`the unquoted value of ${name} runs to the end of the string literal, so what is concatenated on next becomes part of the handler`);
        continue;
      }
      value = rest.slice(0, end);
    }
    if (literal.template && value.includes('${')) {
      problems.push(`the value of ${name} interpolates \${...} into the handler`);
    }
  }
  return problems;
}

const setHandlerAttribute = /\.setAttribute(?:NS)?\s*\((?:[^,()]*,)?\s*(["'`])\s*(on[a-z]+)\s*\1/gi;

// Every problem in one inline script, with its offset in that script.
function scriptProblems(script) {
  const problems = [];
  for (const literal of stringLiterals(script)) {
    for (const message of literalProblems(literal)) {
      problems.push({ offset: literal.offset, message });
    }
  }
  for (const match of withoutComments(script).matchAll(setHandlerAttribute)) {
    problems.push({ offset: match.index, message: `setAttribute('${match[2]}', ...) compiles its value as a handler` });
  }
  return problems;
}

const advice = 'Attach the handler with addEventListener instead of building it into markup.';

function checkTemplate(path) {
  const source = fs.readFileSync(path, 'utf8');
  const scripts = [...source.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)];
  assert.ok(scripts.length > 0, `${path} has no inline script to check`);
  const lineOf = (index) => source.slice(0, index).split('\n').length;
  const failures = [];
  for (const script of scripts) {
    const scriptOffset = script.index + script[0].indexOf(script[1]);
    for (const { offset, message } of scriptProblems(script[1])) {
      failures.push(`${path}:${lineOf(scriptOffset + offset)}: ${message}. ${advice}`);
    }
  }
  assert.deepEqual(failures, [], failures.join('\n'));
}

function selfTest() {
  const flagged = {
    'quoted, concatenated': `el.innerHTML = '<a onclick="edit(\\'' + id + '\\')">x</a>';`,
    'quoted with the literal delimiter, concatenated': `el.innerHTML = "<a onclick=\\"edit('" + id + "')\\">x</a>";`,
    'unquoted, concatenated': `el.innerHTML = '<a onclick=edit(' + id + ')>x</a>';`,
    'unquoted, cut at the equals sign': `el.innerHTML = '<a onclick=' + handler + '>x</a>';`,
    'template literal, quoted': 'el.innerHTML = `<a onclick="edit(\'${id}\')">x</a>`;',
    'template literal, unquoted': 'el.innerHTML = `<a onclick=edit(${id})>x</a>`;',
    'template literal, nested substitution': 'el.innerHTML = `<a onclick="edit(${ids[`${i}`]})">x</a>`;',
    'mixed case and spacing': `el.innerHTML = '<a OnClick = "go(' + id + ')">x</a>';`,
    'setAttribute with data': `btn.setAttribute('onclick', 'edit(' + id + ')');`,
    'setAttribute with a constant': `btn.setAttribute("onmouseover", "show()");`,
    'setAttributeNS': 'btn.setAttributeNS(null, `onclick`, code);',
    'insertAdjacentHTML, concatenated': `list.insertAdjacentHTML('beforeend', '<li onmouseover="show(' + i + ')">' + name + '</li>');`,
    'insertAdjacentHTML, template literal': 'list.insertAdjacentHTML(\'beforeend\', `<li onclick="pick(\'${name}\')">x</li>`);',
    'outerHTML': `row.outerHTML = '<tr ondblclick="open(' + row.id + ')"></tr>';`,
  };
  const benign = {
    'a constant handler': `el.innerHTML = '<button onclick="closeModal()">Close</button>';`,
    'a constant unquoted handler': `el.innerHTML = '<button onclick=closeModal()>Close</button>';`,
    'addEventListener': `btn.addEventListener('click', function() { edit(id); });`,
    'data outside any handler': `el.innerHTML = '<a href="' + safeHref(url) + '">' + escapeHtml(label) + '</a>';`,
    'template literal without a handler': 'el.innerHTML = `<a href="${url}">${label}</a>`;',
    'template literal with a constant handler': 'el.innerHTML = `<button onclick="closeModal()">${label}</button>`;',
    'a query string': `location.href = '/dashboard?onboarding=' + step;`,
    'a comment': `// el.innerHTML = '<a onclick="x(' + id + ')">';\nvar a = 1;`,
    'a block comment': `/* btn.setAttribute('onclick', code); */ var a = 1;`,
    'a regular expression': `var re = /onclick="/; var b = re.test(s);`,
    'another attribute': `btn.setAttribute('title', label);`,
  };
  for (const [name, script] of Object.entries(flagged)) {
    assert.ok(scriptProblems(script).length > 0, `not flagged: ${name}: ${script}`);
  }
  for (const [name, script] of Object.entries(benign)) {
    assert.deepEqual(scriptProblems(script), [], `flagged but inert: ${name}: ${script}`);
  }
}

if (process.argv[2] === '--self-test') {
  selfTest();
} else {
  checkTemplate(process.argv[2]);
}
