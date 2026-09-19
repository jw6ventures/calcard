import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';

const source = fs.readFileSync(process.argv[2], 'utf8');
function extract(name, optional) {
  const start = source.indexOf('function ' + name + '(');
  if (start < 0) {
    assert.ok(optional, `missing ${name}`);
    return '';
  }
  let depth = 0, end = source.indexOf('{', start);
  for (; end < source.length; end++) {
    if (source[end] === '{') depth++;
    if (source[end] === '}' && --depth === 0) break;
  }
  return source.slice(start, end + 1);
}

// Stands in for the browser's textContent -> innerHTML round trip, which
// escapes the markup delimiters and leaves quotes alone. A helper still built
// on the DOM therefore runs here and fails the quote assertions below rather
// than erroring on a missing document.
function element() {
  return {
    set textContent(value) { this._text = value === null ? '' : String(value); },
    get innerHTML() { return this._text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); },
  };
}
const context = { document: { createElement: element }, URL, window: { location: { origin: 'https://calcard.example' } } };
const hasSafeHref = source.includes('function safeHref(');
vm.runInNewContext(
  extract('escapeHtml') + '\n' + extract('safeHref', true) +
    '\nglobalThis._escapeHtml = escapeHtml;' + (hasSafeHref ? ' globalThis._safeHref = safeHref;' : ''),
  context,
);
const escapeHtml = context._escapeHtml;
const safeHref = context._safeHref;

// Every escapeHtml result in these templates is interpolated into markup that
// is then assigned to innerHTML, and several land inside a double-quoted
// attribute. A helper that leaves quotes intact lets a value close the
// attribute and open an event handler.
for (const [input, forbidden] of [
  ['" onmouseover="alert(1)', '"'],
  ["' onmouseover='alert(1)", "'"],
]) {
  const escaped = escapeHtml(input);
  assert.ok(!escaped.includes(forbidden), `escapeHtml left a bare ${forbidden} in ${JSON.stringify(escaped)}`);
}
assert.equal(escapeHtml('<img src=x onerror=alert(1)>'), '&lt;img src=x onerror=alert(1)&gt;');
assert.equal(escapeHtml('a & b'), 'a &amp; b');
// & must be rewritten before the other replacements, or the entities they
// introduce get their ampersand escaped a second time.
assert.equal(escapeHtml('&lt;'), '&amp;lt;');

if (!hasSafeHref) {
  process.exit(0);
}

// safeHref output is placed directly inside href="...". Returning the caller's
// original text rather than the parsed URL carries any quote straight through.
const hostile = 'https://example.com/"onmouseover="alert(1)';
const href = safeHref(hostile);
assert.ok(!href.includes('"'), `safeHref left a bare quote in ${JSON.stringify(href)}`);
assert.ok(!safeHref("https://example.com/'onmouseover='alert(1)").includes("'"), 'safeHref left a bare apostrophe');

// The scheme allow list still has to hold, and ordinary links still have to work.
for (const blocked of ['javascript:alert(1)', 'data:text/html,<script>alert(1)</script>', 'vbscript:alert(1)']) {
  assert.equal(safeHref(blocked), '', `safeHref admitted ${blocked}`);
}
assert.ok(safeHref('https://example.com/a?b=1').startsWith('https://example.com/a'), 'safeHref dropped a valid https URL');
assert.ok(safeHref('mailto:someone@example.com').startsWith('mailto:'), 'safeHref dropped a valid mailto URL');
