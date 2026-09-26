// Runs the shared sanitizeHtml over the hostile HTML descriptions it
// is expected to defuse, and checks the attributes that survive.
//
// sanitizeHtml is handed the X-ALT-DESC of an event, which arrives over CalDAV
// PUT or an ICS import and is never sanitized on the way in. A filter that
// removes named attributes keeps every attribute nobody thought of, so what is
// asserted here is the opposite rule: only the attributes named in the allow
// list survive, on the tags that have a use for them.
//
// node has no DOM, so the shim below is one -- enough of a parser and enough of
// a tree for the sanitizer's own walk to run unchanged. It is deliberately
// literal about the parts the sanitizer depends on: attribute names arrive
// lowercased, character references in an attribute value are decoded before the
// sanitizer sees it (which is how java&#9;script: is spelled), and a void
// element takes no children. Elements the parser builds are marked, so the
// test can tell them from the ones the sanitizer builds itself.
//
// Usage: node description_sanitizer.mjs safe_html_js.tmpl
import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';

const path = process.argv[2];
const source = fs.readFileSync(path, 'utf8');

function extract(name) {
  const start = source.indexOf('function ' + name + '(');
  assert.ok(start >= 0, `missing ${name}`);
  let depth = 0, end = source.indexOf('{', start);
  for (; end < source.length; end++) {
    if (source[end] === '{') depth++;
    if (source[end] === '}' && --depth === 0) break;
  }
  return source.slice(start, end + 1);
}

const ELEMENT_NODE = 1, TEXT_NODE = 3, FRAGMENT_NODE = 11;
const VOID = new Set(['area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input', 'link', 'meta', 'param', 'source', 'track', 'wbr']);
const RAW_TEXT = new Set(['script', 'style', 'textarea']);
const NAMED_REFS = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: ' ', Tab: '\t', NewLine: '\n' };

function decodeRefs(text) {
  return text.replace(/&(#[xX][0-9a-fA-F]+|#\d+|[a-zA-Z]+);/g, (whole, body) => {
    if (body[0] === '#') {
      const code = body[1] === 'x' || body[1] === 'X'
        ? parseInt(body.slice(2), 16)
        : parseInt(body.slice(1), 10);
      return Number.isInteger(code) && code > 0 && code <= 0x10ffff ? String.fromCodePoint(code) : whole;
    }
    const named = NAMED_REFS[body];
    return named === undefined ? whole : named;
  });
}

class DomNode {
  constructor(nodeType) {
    this.nodeType = nodeType;
    this.childNodes = [];
    this.parentNode = null;
  }

  get firstChild() {
    return this.childNodes.length ? this.childNodes[0] : null;
  }

  appendChild(child) {
    if (child.nodeType === FRAGMENT_NODE) {
      for (const grandchild of Array.from(child.childNodes)) this.appendChild(grandchild);
      return child;
    }
    if (child.parentNode) child.parentNode.removeChild(child);
    child.parentNode = this;
    this.childNodes.push(child);
    return child;
  }

  removeChild(child) {
    const at = this.childNodes.indexOf(child);
    assert.ok(at >= 0, 'removeChild on a node that is not a child');
    this.childNodes.splice(at, 1);
    child.parentNode = null;
    return child;
  }

  replaceChild(fresh, stale) {
    if (fresh.parentNode) fresh.parentNode.removeChild(fresh);
    const at = this.childNodes.indexOf(stale);
    assert.ok(at >= 0, 'replaceChild on a node that is not a child');
    this.childNodes[at] = fresh;
    fresh.parentNode = this;
    stale.parentNode = null;
    return stale;
  }
}

class TextNode extends DomNode {
  constructor(data) {
    super(TEXT_NODE);
    this.data = String(data);
  }

  get textContent() { return this.data; }
  set textContent(value) { this.data = String(value); }
}

class FragmentNode extends DomNode {
  constructor() { super(FRAGMENT_NODE); }
}

class ElementNode extends DomNode {
  constructor(tag) {
    super(ELEMENT_NODE);
    this.tagName = String(tag).toUpperCase();
    this._attributes = new Map();
  }

  // A fresh array each read, which is what Array.from(node.attributes) needs to
  // survive the removals the sanitizer makes while iterating.
  get attributes() {
    return Array.from(this._attributes, ([name, value]) => ({ name, value }));
  }

  getAttribute(name) {
    const key = String(name).toLowerCase();
    return this._attributes.has(key) ? this._attributes.get(key) : null;
  }

  setAttribute(name, value) { this._attributes.set(String(name).toLowerCase(), String(value)); }
  removeAttribute(name) { this._attributes.delete(String(name).toLowerCase()); }
  hasAttribute(name) { return this._attributes.has(String(name).toLowerCase()); }

  get textContent() {
    return this.childNodes.map((child) => child.textContent).join('');
  }

  set textContent(value) {
    this.childNodes = [];
    this.appendChild(new TextNode(value));
  }

  set innerHTML(html) {
    this.childNodes = [];
    parseInto(this, String(html));
  }
}

function parseAttributes(text) {
  const attributes = [];
  const pattern = /([^\s"'>/=]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'=<>`]*)))?/g;
  for (let match = pattern.exec(text); match !== null; match = pattern.exec(text)) {
    const value = match[2] !== undefined ? match[2] : match[3] !== undefined ? match[3] : match[4] !== undefined ? match[4] : '';
    attributes.push({ name: match[1].toLowerCase(), value: decodeRefs(value) });
  }
  return attributes;
}

function parseInto(container, html) {
  const open = [container];
  const top = () => open[open.length - 1];
  const addText = (text) => { if (text) top().appendChild(new TextNode(decodeRefs(text))); };
  let at = 0;
  while (at < html.length) {
    const lt = html.indexOf('<', at);
    if (lt < 0) {
      addText(html.slice(at));
      break;
    }
    addText(html.slice(at, lt));
    const rest = html.slice(lt);
    if (rest.startsWith('<!--')) {
      const end = html.indexOf('-->', lt);
      at = end < 0 ? html.length : end + 3;
      continue;
    }
    if (rest.startsWith('<!') || rest.startsWith('<?')) {
      const end = html.indexOf('>', lt);
      at = end < 0 ? html.length : end + 1;
      continue;
    }
    const closing = /^<\/([a-zA-Z][^\s>]*)\s*>?/.exec(rest);
    if (closing) {
      const name = closing[1].toLowerCase();
      for (let depth = open.length - 1; depth > 0; depth--) {
        if (open[depth].tagName.toLowerCase() === name) {
          open.length = depth;
          break;
        }
      }
      at = lt + closing[0].length;
      continue;
    }
    const opening = /^<([a-zA-Z][^\s/>]*)((?:[^>"']|"[^"]*"|'[^']*')*)>?/.exec(rest);
    if (!opening) {
      addText('<');
      at = lt + 1;
      continue;
    }
    const name = opening[1].toLowerCase();
    const element = new ElementNode(name);
    element.parsed = true;
    for (const attribute of parseAttributes(opening[2])) element.setAttribute(attribute.name, attribute.value);
    top().appendChild(element);
    at = lt + opening[0].length;
    if (VOID.has(name) || /\/\s*>$/.test(opening[0])) continue;
    if (RAW_TEXT.has(name)) {
      const end = html.toLowerCase().indexOf('</' + name, at);
      const text = end < 0 ? html.slice(at) : html.slice(at, end);
      if (text) element.appendChild(new TextNode(text));
      at = end < 0 ? html.length : end;
      continue;
    }
    open.push(element);
  }
}

function createInertDocument() {
  const doc = {
    createElement: (tag) => new ElementNode(tag),
    createTextNode: (data) => new TextNode(data),
    createDocumentFragment: () => new FragmentNode(),
  };
  doc.body = new ElementNode('body');
  return doc;
}

const context = {
  Node: { ELEMENT_NODE, TEXT_NODE },
  URL,
  window: {},
  document: {
    implementation: { createHTMLDocument: () => createInertDocument() },
    createElement: (tag) => new ElementNode(tag),
    createTextNode: (data) => new TextNode(data),
    createDocumentFragment: () => new FragmentNode(),
  },
};
context.globalThis = context;

vm.runInNewContext(
  [
    extract('unescapeText'),
    extract('safeUrl'),
    extract('sanitizeHtml'),
    'globalThis._sanitizeHtml = sanitizeHtml;',
  ].join('\n'),
  context,
);

const sanitizeHtml = context._sanitizeHtml;

function serialize(node) {
  if (node.nodeType === TEXT_NODE) return node.data;
  if (node.nodeType === ELEMENT_NODE) {
    const tag = node.tagName.toLowerCase();
    const attributes = node.attributes.map(({ name, value }) => ` ${name}="${value}"`).join('');
    return `<${tag}${attributes}>` + node.childNodes.map(serialize).join('') + `</${tag}>`;
  }
  return node.childNodes.map(serialize).join('');
}

function sanitize(html) {
  const fragment = sanitizeHtml(html);
  const elements = [];
  const walk = (node) => {
    if (node.nodeType === ELEMENT_NODE) elements.push(node);
    node.childNodes.forEach(walk);
  };
  walk(fragment);
  // Nothing the parser built may leave the sanitizer: a parsed element carries
  // state no attribute shows, such as the custom-element "is" value it was
  // created with, which removing the attribute does not undo.
  for (const element of elements) {
    assert.ok(!element.parsed, `a parsed <${element.tagName.toLowerCase()}> was returned rather than rebuilt: ${serialize(fragment)}`);
  }
  return { fragment, elements, html: serialize(fragment) };
}

const only = (html, tag) => {
  const result = sanitize(html);
  const matches = result.elements.filter((element) => element.tagName.toLowerCase() === tag);
  assert.equal(matches.length, 1, `expected one <${tag}> in ${result.html}`);
  return { element: matches[0], result };
};

const attributesOf = (element) => Object.fromEntries(element.attributes.map(({ name, value }) => [name, value]));

// A description that names an id the modal looks up answers that lookup with
// its own node: the sanitized fragment is in the document before the lookups
// run, and an injected id that precedes the real element in tree order is what
// getElementById returns. calendar_view.html builds the Edit buttons into
// #event-action-buttons and fills the edit form by id; all_calendars_view.html
// fills the day list by id. None of it executes script, and all of it puts the
// user's controls under the description's control.
for (const clobbered of [
  'event-action-buttons',
  'event-html-desc',
  'modal-body',
  'edit-event-form',
  'delete-event-form',
  'edit-summary',
  'day-modal-events',
]) {
  const { element, result } = only(`<div id="${clobbered}">planted</div>`, 'div');
  assert.equal(
    element.getAttribute('id'),
    null,
    `a description keeps id="${clobbered}", which clobbers the modal's own lookup: ${result.html}`,
  );
  assert.equal(element.textContent, 'planted', 'the sanitizer dropped the element text');
}

// An allow list is a closed set, so nothing outside it survives on any allowed
// tag -- including the attributes that are inert today and the ones a future
// browser gives a meaning to.
const refused = {
  a: ['id', 'class', 'style', 'onclick', 'onmouseover', 'target', 'ping', 'download', 'formaction', 'data-uid', 'srcset', 'align'],
  div: ['id', 'class', 'style', 'onload', 'background', 'data-uid', 'accesskey', 'contenteditable', 'draggable', 'tabindex'],
  p: ['id', 'class', 'style', 'align', 'onanimationstart', 'is'],
  table: ['id', 'class', 'style', 'background', 'width', 'border', 'action'],
  td: ['id', 'class', 'style', 'background', 'colspan', 'nowrap'],
  span: ['id', 'class', 'style', 'onfocus', 'poster', 'srcdoc'],
};
for (const [tag, names] of Object.entries(refused)) {
  for (const name of names) {
    const { element, result } = only(`<${tag} ${name}="x">text</${tag}>`, tag);
    assert.equal(
      element.getAttribute(name),
      null,
      `<${tag}> kept ${name}, which the attribute allow list does not name: ${result.html}`,
    );
  }
}

// title is advisory text the browser renders as a tooltip; it is never parsed as
// markup, script or a URL, and a description that labels a link or an
// abbreviation has a real use for it.
const titled = only('<a href="https://example.test/doc" title="The spec">spec</a>', 'a');
assert.equal(titled.element.getAttribute('title'), 'The spec', `title was dropped: ${titled.result.html}`);
assert.equal(only('<span title="note">x</span>', 'span').element.getAttribute('title'), 'note');

// A surviving link keeps its href, and the sanitizer supplies the rel that
// severs the opener handle and the referrer rather than trusting the one the
// description wrote.
const link = only(
  '<a href="https://example.test/a?b=1" target="_self" rel="opener" id="modal-body" class="btn" style="position:fixed" onclick="alert(1)">go</a>',
  'a',
);
assert.deepEqual(
  attributesOf(link.element),
  { href: 'https://example.test/a?b=1', target: '_blank', rel: 'noopener noreferrer' },
  `the surviving <a> carries attributes the allow list does not name: ${link.result.html}`,
);
assert.equal(
  only('<a href="mailto:someone@example.test">mail</a>', 'a').element.getAttribute('href'),
  'mailto:someone@example.test',
);

// href is a URL sink only on <a>. On every other allowed tag it is an attribute
// the allow list does not name, so it goes whatever its value is.
for (const tag of ['div', 'span', 'p', 'td']) {
  const { element, result } = only(`<${tag} href="https://example.test/">x</${tag}>`, tag);
  assert.equal(element.getAttribute('href'), null, `<${tag}> kept an href: ${result.html}`);
}

// A relative href resolves against this site, and would give the description a
// link to one of the site's own pages under text of its choosing.
for (const relative of ['/calendars?error=Your+session+expired', 'settings', '//evil.test/', '?error=x']) {
  const { element, result } = only(`<a href="${relative}">Sign in again</a>`, 'a');
  assert.deepEqual(attributesOf(element), {}, `a relative href survived: ${result.html}`);
}

// The scheme gate still has to hold, and an <a> whose href does not survive it
// is not a link -- it gets no href, and no target or rel to dress it as one.
for (const blocked of [
  'javascript:alert(1)',
  'JaVaScRiPt:alert(1)',
  'java&#9;script:alert(1)',
  'java&#10;script:alert(1)',
  '&#1;javascript:alert(1)',
  'data:text/html,<script>alert(1)</script>',
  'vbscript:alert(1)',
]) {
  const { element, result } = only(`<a href="${blocked}">click</a>`, 'a');
  assert.deepEqual(
    attributesOf(element),
    {},
    `<a href="${blocked}"> survived with attributes: ${result.html}`,
  );
}

// The tag allow list and the text it preserves are unchanged by any of the
// above: a refused tag still collapses to its text, and the structure a
// description legitimately uses still arrives.
const collapsed = sanitize('<script>alert(1)</script><img src=x onerror=alert(1)><div>kept</div>');
assert.equal(
  collapsed.elements.filter((element) => ['script', 'img'].includes(element.tagName.toLowerCase())).length,
  0,
  `a refused tag survived: ${collapsed.html}`,
);
assert.ok(collapsed.html.includes('alert(1)'), `the refused script's text was dropped: ${collapsed.html}`);
assert.ok(collapsed.html.includes('<div>kept</div>'), `an allowed tag was dropped: ${collapsed.html}`);

const nested = sanitize('<div><ul><li><a href="https://example.test/" id="event-action-buttons">deep</a></li></ul></div>');
assert.equal(
  nested.html,
  '<div><ul><li><a href="https://example.test/" target="_blank" rel="noopener noreferrer">deep</a></li></ul></div>',
  `the walk did not reach a nested element: ${nested.html}`,
);

// The iCal unescaping the sanitizer does before parsing is part of its contract:
// X-ALT-DESC arrives with the property value escapes still in it.
const unescaped = sanitize('line one\\nline two\\, and\\; more');
assert.equal(unescaped.html, 'line one\nline two, and; more', `iCal escapes were not undone: ${unescaped.html}`);

// The unescaping is one pass: an escaped backslash followed by "n" is a
// backslash and an "n", not a backslash and a line break.
const backslash = sanitize('C:\\\\new\\Nline');
assert.equal(backslash.html, 'C:\\new\nline', `escapes were undone in the wrong order: ${JSON.stringify(backslash.html)}`);
