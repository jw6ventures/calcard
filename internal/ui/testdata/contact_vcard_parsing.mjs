// Runs addressbook_view.html's own vCard reader, and the editor and modal that
// consume it, over the values it has to read back exactly.
//
// A birthday goes from the stored card into the edit form and back out as the
// value the server writes, so any drift in the read is written back into the
// contact on the next save. Every case here is run through that whole trip:
// parse, fill the form, read the field the form submits.
//
// Usage: node contact_vcard_parsing.mjs addressbook_view.html safe_html_js.tmpl
import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';

const source = process.argv.slice(2).map((path) => fs.readFileSync(path, 'utf8')).join('\n')
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
  const node = {
    id,
    value: '',
    children: [],
    classList: { add() {}, remove() {} },
    style: {},
    validationMessage: '',
    setCustomValidity(message) { this.validationMessage = String(message); },
    addEventListener() {},
    appendChild(child) { this.children.push(child); return child; },
    set innerHTML(html) {
      this._html = String(html);
      // A select built from options only takes a value one of them offers.
      this._options = [...this._html.matchAll(/<option value="([^"]*)"/g)].map((m) => m[1]);
    },
    get innerHTML() { return this._html || ''; },
  };
  return node;
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
  URL,
  window: { canEditAddressBook: false },
};
context.globalThis = context;

vm.runInNewContext(
  [
    extract('escapeHtml'),
    extract('unescapeText'),
    extract('safeUrl'),
    extract('unfoldLines'),
    extract('indexOutsideQuotes'),
    extract('splitOutsideQuotes'),
    extract('unquoteParam'),
    extract('getType'),
    extract('splitUnescaped'),
    extract('parseVCardDate'),
    extract('parseVCardDateProperty'),
    extract('formatVCardDate'),
    extract('contactPhotoSrc'),
    extract('websiteHref'),
    extract('parseVCard'),
    extract('getInitials'),
    extract('formatAddress'),
    extract('birthdayDaysInMonth'),
    extract('syncBirthdayDayOptions'),
    extract('updateBirthdayHidden'),
    extract('setBirthdayFields'),
    "var modal = document.getElementById('contact-modal');",
    extract('showEditContactModal'),
    'globalThis._showEditContactModal = showEditContactModal;',
    extract('showContactModal'),
    'globalThis._parseVCard = parseVCard;',
    'globalThis._setBirthdayFields = setBirthdayFields;',
    'globalThis._showContactModal = showContactModal;',
    'globalThis._formatVCardDate = formatVCardDate;',
  ].join('\n'),
  context,
);

const card = (...lines) => ['BEGIN:VCARD', 'VERSION:3.0', 'FN:Ada Lovelace', ...lines, 'END:VCARD'].join('\r\n');

// The select shim only keeps values it has an option for, as the browser does.
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

function roundTrip(bday) {
  const contact = context._parseVCard(card('BDAY' + bday));
  context._setBirthdayFields('edit', contact.bday);
  return { contact, submitted: byId['edit-birthday'].value };
}

// Each value is what a client may store, and what the form must submit back.
for (const [stored, submitted] of [
  [':--02-29', '--02-29'],
  [':--0415', '--04-15'],
  [':--04-15', '--04-15'],
  [':1990-04-15', '1990-04-15'],
  [':19900415', '1990-04-15'],
  [':1900-02-15', '1900-02-15'],
  [':2000-02-29', '2000-02-29'],
  [':0415-04-15', '0415-04-15'],
  // A date-time names the day its writer meant; its zone must not move it.
  [':1990-04-15T00:00:00Z', '1990-04-15'],
  [':19900415T000000Z', '1990-04-15'],
  [':1990-04-15T23:30:00-08:00', '1990-04-15'],
  [';VALUE=date:1990-04-15', '1990-04-15'],
  // Apple clients that cannot write a year-less date name a stand-in year.
  [';X-APPLE-OMIT-YEAR=1604:1604-03-15', '--03-15'],
  [';X-APPLE-OMIT-YEAR=1604:1990-03-15', '1990-03-15'],
]) {
  const { contact, submitted: got } = roundTrip(stored);
  assert.equal(got, submitted, `BDAY${stored} came back from the form as ${JSON.stringify(got)} (parsed ${JSON.stringify(contact.bday)})`);
}

// A value that is not a whole date is not guessed at: the form shows no
// birthday rather than one the card never named.
for (const stored of [':1990-02-30', ':--02-30', ':1990-13-01', ':April 15', ':--04', ':1990-04', ':']) {
  const { contact, submitted } = roundTrip(stored);
  assert.equal(contact.bday, null, `BDAY${stored} parsed as ${JSON.stringify(contact.bday)}`);
  assert.equal(submitted, '');
}

// The detail view shows the date the card names, with a year only when it has
// one: never "March 1, 1900" for a year-less 29 February, never "January 1,
// 415" for --0415.
const expected = (year, month, day) => {
  const at = new Date(0);
  at.setUTCFullYear(year === null ? 2000 : year, month - 1, day);
  const options = { month: 'long', day: 'numeric', timeZone: 'UTC' };
  if (year !== null) options.year = 'numeric';
  return at.toLocaleDateString(undefined, options);
};
for (const [stored, year, month, day] of [
  ['--02-29', null, 2, 29],
  ['--0415', null, 4, 15],
  ['1900-02-15', 1900, 2, 15],
  ['1990-04-15T00:00:00Z', 1990, 4, 15],
]) {
  const contact = context._parseVCard(card('BDAY:' + stored, 'ANNIVERSARY:' + stored));
  context._showContactModal(contact);
  const html = byId['modal-body'].innerHTML;
  const want = expected(year, month, day);
  assert.equal(html.split(want).length - 1, 2, `BDAY:${stored} and ANNIVERSARY:${stored} did not both render as ${want}: ${html}`);
  if (year === null) {
    assert.ok(!/\b1900\b/.test(html), `a year-less date rendered with a stand-in year: ${html}`);
  }
}
{
  const want = expected(null, 2, 29);
  assert.equal(context._formatVCardDate({ year: null, month: 2, day: 29 }), want);
  assert.ok(!/March|1900/.test(want), `the leap-day check itself is broken: ${want}`);
}

// A custom date that is not a date is shown as the text it is, not dropped and
// not passed to a date formatter.
{
  const contact = context._parseVCard(card('item1.X-CUSTOM1;X-ABLABEL=Graduation:<b>spring</b>'));
  context._showContactModal(contact);
  const html = byId['modal-body'].innerHTML;
  assert.ok(html.includes('&lt;b&gt;spring&lt;/b&gt;'), `the custom value was not shown escaped: ${html}`);
}

// Structured values are split only on separators that are not escaped, and
// TEXT escapes are undone in one pass, so an escaped backslash followed by "n"
// stays a backslash and an "n".
{
  const contact = context._parseVCard(card(
    'N:O\\;Brien;Ada\\, Countess;;;',
    'ORG:Acme\\; Inc;Research',
    'NOTE:C:\\\\new\\nline',
    'CATEGORIES:friends\\, family,work',
    'ADR;TYPE=home:;;1 Main St\\; Apt 2;Springfield;;;',
  ));
  assert.equal(contact.org, 'Acme; Inc', 'ORG was split on an escaped semicolon');
  assert.equal(contact.n.last, 'O;Brien');
  assert.equal(contact.n.first, 'Ada, Countess');
  assert.equal(contact.note, 'C:\\new\nline', `NOTE unescaped to ${JSON.stringify(contact.note)}`);
  assert.deepEqual([...contact.categories], ['friends, family', 'work']);
  assert.equal(contact.addresses[0].street, '1 Main St; Apt 2');
  assert.equal(contact.addresses[0].city, 'Springfield');
}

// An embedded photo becomes a data: URL an <img> can show; a photo the page
// will not load falls back to the initials rather than an empty circle.
{
  const photo = context._parseVCard(card('PHOTO;ENCODING=b;TYPE=JPEG:/9j/4AAQSkZJRg==')).photo;
  assert.equal(photo, 'data:image/jpeg;base64,/9j/4AAQSkZJRg==');

  context._showContactModal(context._parseVCard(card('PHOTO;ENCODING=b;TYPE=JPEG:/9j/4AAQSkZJRg==')));
  let html = byId['modal-body'].innerHTML;
  assert.ok(html.includes('<img src="data:image/jpeg;base64,/9j/4AAQSkZJRg=="'), `the embedded photo was not shown: ${html}`);

  context._showContactModal(context._parseVCard(card('PHOTO;VALUE=uri:data:image/png;base64,iVBORw0KGgo=')));
  html = byId['modal-body'].innerHTML;
  assert.ok(html.includes('<img src="data:image/png;base64,iVBORw0KGgo="'), `a vCard 4 data: photo was not shown: ${html}`);

  for (const refused of [
    'PHOTO;ENCODING=b;TYPE=svg+xml:PHN2Zz48L3N2Zz4=',
    'PHOTO;VALUE=uri:data:text/html;base64,PHNjcmlwdD4=',
    'PHOTO;VALUE=uri:http://example.test/a"onerror="alert(1)',
  ]) {
    context._showContactModal(context._parseVCard(card(refused)));
    html = byId['modal-body'].innerHTML;
    if (refused.includes('http://')) {
      assert.ok(!html.includes('"onerror'), `a photo URL broke out of its attribute: ${html}`);
      continue;
    }
    assert.ok(!html.includes('<img'), `${refused} was loaded: ${html}`);
    assert.ok(html.includes('>AL</div>'), `${refused} left neither a photo nor the initials: ${html}`);
    assert.ok(!html.includes('has-photo'), `${refused} is styled as a photo it does not show: ${html}`);
  }
}

// The server's structured edit writes into the first occurrence of each
// single-valued property the form edits, so the form has to show that one:
// showing a later duplicate would save its value over the first.
{
  const contact = context._parseVCard([
    'BEGIN:VCARD',
    'VERSION:3.0',
    'FN:First Name',
    'N:First;Given;;;',
    'ORG:First Org',
    'NOTE:first note',
    'BDAY:1990-04-15',
    'EMAIL:first@example.com',
    'TEL:+1 555 0100',
    'FN:Second Name',
    'N:Second;Other;;;',
    'ORG:Second Org',
    'NOTE:second note',
    'BDAY:--12-31',
    'EMAIL:second@example.com',
    'TEL:+1 555 0199',
    'END:VCARD',
  ].join('\r\n'));
  assert.equal(contact.fn, 'First Name');
  assert.equal(contact.n.last, 'First');
  assert.equal(contact.n.first, 'Given');
  assert.equal(contact.org, 'First Org');
  assert.equal(contact.note, 'first note');
  assert.deepEqual({ ...contact.bday }, { year: 1990, month: 4, day: 15 });
  assert.equal(contact.emails[0].value, 'first@example.com');
  assert.equal(contact.phones[0].value, '+1 555 0100');

  context._setBirthdayFields('edit', contact.bday);
  assert.equal(byId['edit-birthday'].value, '1990-04-15', 'the form took a later BDAY');
}

// The first occurrence wins even when it is not a date the page can show: the
// server edits that one, so falling through to a later BDAY would put a value
// in the form that saving then writes over the first.
{
  const contact = context._parseVCard(card('BDAY:sometime in April', 'BDAY:1990-04-15'));
  assert.equal(contact.bday, null);
}

// A website written without a scheme is still shown. A host-like value links
// to its https:// form; anything else is shown as text, never as a link the
// page would resolve against this site.
{
  const contact = context._parseVCard(card(
    'URL;TYPE=work:www.example.com',
    'URL;TYPE=home:example.org/about',
    'URL;TYPE=other:/calendars?error=Session+expired',
    'URL;TYPE=blog:https://blog.example.net/',
    'URL;TYPE=bad:javascript:alert(1)',
  ));
  context._showContactModal(contact);
  const html = byId['modal-body'].innerHTML;
  assert.ok(html.includes('<a href="https://www.example.com/" target="_blank" rel="noopener noreferrer">www.example.com</a>'), `a bare host was not linked: ${html}`);
  assert.ok(html.includes('<a href="https://example.org/about" target="_blank" rel="noopener noreferrer">example.org/about</a>'), `a bare host with a path was not linked: ${html}`);
  assert.ok(html.includes('<a href="https://blog.example.net/"'), `an absolute URL was not linked: ${html}`);
  for (const text of ['/calendars?error=Session+expired', 'javascript:alert(1)']) {
    assert.ok(html.includes('<span class="modal-item-value">' + text + '</span>'), `${text} was not shown as text: ${html}`);
  }
  assert.ok(!html.includes('href="/calendars'), `a relative URL became a link: ${html}`);
  assert.ok(!html.includes('href="javascript:'), `a javascript: URL became a link: ${html}`);
}

// A quoted parameter value may hold a colon (RFC 6350 section 3.3), so the
// value starts at the first colon outside quotes. Split at the first colon of
// all, the birthday parses as nothing and the form would post it as a removal.
{
  const { contact, submitted } = roundTrip(';X-FOO="a:b":1990-04-15');
  assert.deepEqual({ ...contact.bday }, { year: 1990, month: 4, day: 15 });
  assert.equal(submitted, '1990-04-15');
  const other = context._parseVCard(card('EMAIL;X-LABEL="a;b:c";TYPE=work:ada@example.com', 'NOTE;X-SRC="x:y;z":hello'));
  assert.equal(other.emails[0].value, 'ada@example.com');
  assert.equal(other.emails[0].type, 'work');
  assert.equal(other.note, 'hello');
}

// Year 0 is a stand-in for "no year", as the server's birthday page treats it;
// in the year box it would fall below its minimum and block every save.
{
  const { contact, submitted } = roundTrip(':0000-04-15');
  assert.equal(contact.bday.year, null, `year 0 was kept: ${JSON.stringify(contact.bday)}`);
  assert.equal(submitted, '--04-15');
  assert.equal(byId['edit-birthday-year'].value, '');
}

// A quoted X-APPLE-OMIT-YEAR still names the stand-in year.
{
  const { contact, submitted } = roundTrip(';X-APPLE-OMIT-YEAR="1604":1604-03-15');
  assert.equal(contact.bday.year, null, `quoted omit-year was ignored: ${JSON.stringify(contact.bday)}`);
  assert.equal(submitted, '--03-15');
}

// BDAY;VALUE=text is free text, and a date-time needs a real time part: the
// server reads neither as a date, so the form must not either.
for (const stored of [';VALUE=text:1990-04-15', ';VALUE="TEXT":1990-04-15', ':1990-04-15Tx', ':1990-04-15T', ':1990-04-15Tnoon']) {
  const { contact, submitted } = roundTrip(stored);
  assert.equal(contact.bday, null, `BDAY${stored} parsed as ${JSON.stringify(contact.bday)}`);
  assert.equal(submitted, '');
}

// The editor posts the ETag of the card it was built from, so a form built
// before another client's change is refused instead of overwriting it.
{
  const contact = context._parseVCard(card('TEL:+1 555'));
  contact.uid = 'ada';
  contact.etag = 'etag-1';
  context._showEditContactModal(contact);
  assert.equal(byId['edit-etag'].value, 'etag-1', 'the edit form does not carry the ETag');
}

// A year-less date-time names its date as the server reads it.
for (const [stored, submitted] of [[':--0415T1200', '--04-15'], [':--04-15T12:00:00Z', '--04-15']]) {
  const { contact, submitted: got } = roundTrip(stored);
  assert.equal(got, submitted, `BDAY${stored} came back as ${JSON.stringify(got)} (parsed ${JSON.stringify(contact.bday)})`);
}
