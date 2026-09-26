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
const context = { document: { createElement: element }, URL, window: {} };
const hasSafeHref = source.includes('function safeHref(');
const hasSafeUrl = source.includes('function safeUrl(');
vm.runInNewContext(
  [
    extract('escapeHtml'),
    extract('safeUrl', true),
    extract('safeHref', true),
    'globalThis._escapeHtml = escapeHtml;',
    hasSafeUrl ? 'globalThis._safeUrl = safeUrl;' : '',
    hasSafeHref ? 'globalThis._safeHref = safeHref;' : '',
  ].join('\n'),
  context,
);
const escapeHtml = context._escapeHtml;
const safeHref = context._safeHref;
const safeUrl = context._safeUrl;

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

// safeUrl is the scheme gate for a value that is set as a property or an
// attribute rather than concatenated into markup, so it returns the parsed URL
// rather than an escaped one. Deciding the scheme by inspecting the caller's
// text -- trim, then startsWith('javascript:') -- misses every spelling the URL
// parser normalizes away, and the browser navigates by the parsed URL.
if (hasSafeUrl) {
  for (const blocked of [
    'javascript:alert(1)',
    'JaVaScRiPt:alert(1)',
    '  javascript:alert(1)',
    'java\tscript:alert(1)',
    'java\nscript:alert(1)',
    'java\rscript:alert(1)',
    '\u0001javascript:alert(1)',
    'data:text/html,<script>alert(1)</script>',
    'vbscript:alert(1)',
  ]) {
    assert.equal(safeUrl(blocked), '', `safeUrl admitted ${JSON.stringify(blocked)}`);
  }
  // A relative URL resolves against this site, so a stored value could put a
  // link to one of the site's own pages, under text of its choosing, into a
  // modal. Only absolute URLs are links here.
  for (const relative of [
    '/calendars?error=Your+session+expired.+Sign+in+again',
    'calendars',
    '//evil.test/login',
    '?error=x',
    '#top',
  ]) {
    assert.equal(safeUrl(relative), '', `safeUrl admitted the relative URL ${JSON.stringify(relative)}`);
  }
  assert.ok(safeUrl('https://example.com/a?b=1').startsWith('https://example.com/a'), 'safeUrl dropped a valid https URL');
  assert.equal(safeUrl('tel:+15551234567'), 'tel:+15551234567', 'safeUrl dropped a telephone link');
  assert.ok(safeUrl('mailto:someone@example.com').startsWith('mailto:'), 'safeUrl dropped a valid mailto URL');
  // Unescaped on purpose: this value is assigned, not interpolated. The URL
  // parser is what keeps a quote out of it.
  assert.ok(!safeUrl('https://example.com/"x').includes('"'), 'safeUrl left a bare quote in a parsed URL');
}

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
