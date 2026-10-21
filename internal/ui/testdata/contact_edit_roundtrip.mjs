// Opens addressbook_view.html's own edit form on each contact the page was
// served, and prints the fields the form would submit.
//
// Usage: node contact_edit_roundtrip.mjs addressbook_view.html safe_html_js.tmpl contacts.json
//
// contacts.json is the page's rawContacts array as the server rendered it.
// The output is one object per contact: its UID and the submitted fields,
// keyed by form field name.
import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';

const [pagePath, partialPath, contactsPath] = process.argv.slice(2);
const source = [pagePath, partialPath].map((path) => fs.readFileSync(path, 'utf8')).join('\n')
  .replaceAll('{{.AddressBook.ID}}', '1');

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

function control(id) {
  return {
    id,
    value: '',
    classList: { add() {}, remove() {} },
    style: {},
    validationMessage: '',
    setCustomValidity(message) { this.validationMessage = String(message); },
    set innerHTML(html) {
      this._html = String(html);
      this._options = [...this._html.matchAll(/<option value="([^"]*)"/g)].map((m) => m[1]);
    },
    get innerHTML() { return this._html || ''; },
  };
}

const byId = {};
const context = {
  document: {
    createElement: (tag) => control(tag),
    getElementById(id) {
      if (!byId[id]) byId[id] = control(id);
      return byId[id];
    },
  },
  window: {},
};
context.globalThis = context;

// A select only takes a value one of its options offers, as in the browser.
for (const id of ['edit-birthday-month', 'edit-birthday-day']) {
  const el = context.document.getElementById(id);
  Object.defineProperty(el, 'value', {
    get() { return this._value || ''; },
    set(next) {
      const options = id === 'edit-birthday-month'
        ? ['', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
        : this._options || [''];
      this._value = options.includes(String(next)) ? String(next) : '';
    },
  });
}

vm.runInNewContext(
  [
    extract('unescapeText'),
    extract('unfoldLines'),
    extract('indexOutsideQuotes'),
    extract('splitOutsideQuotes'),
    extract('unquoteParam'),
    extract('getType'),
    extract('splitUnescaped'),
    extract('parseVCardDate'),
    extract('parseVCardDateProperty'),
    extract('parseVCard'),
    extract('birthdayDaysInMonth'),
    extract('syncBirthdayDayOptions'),
    extract('updateBirthdayHidden'),
    extract('setBirthdayFields'),
    extract('showEditContactModal'),
    'globalThis._parseVCard = parseVCard;',
    'globalThis._showEditContactModal = showEditContactModal;',
  ].join('\n'),
  context,
);

const fields = {
  display_name: 'edit-display-name',
  first_name: 'edit-first-name',
  last_name: 'edit-last-name',
  company: 'edit-company',
  email: 'edit-email',
  phone: 'edit-phone',
  birthday: 'edit-birthday',
  notes: 'edit-notes',
  etag: 'edit-etag',
};

// The page maps each served contact the same way before opening the editor.
const served = JSON.parse(fs.readFileSync(contactsPath, 'utf8'));
const submitted = served.map((raw) => {
  const contact = context._parseVCard(raw.VCard);
  contact.uid = raw.UID;
  contact.etag = raw.ETag;
  context._showEditContactModal(contact);
  const form = {};
  for (const [name, id] of Object.entries(fields)) {
    form[name] = byId[id].value;
  }
  return { uid: raw.UID, form };
});
process.stdout.write(JSON.stringify(submitted));
