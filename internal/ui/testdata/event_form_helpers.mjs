// Runs the calendar templates' own attendee formatting and create-form date
// check. Source-level assertions in templates_test.go only prove the helpers
// are present; these prove they behave.
import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';

const source = fs.readFileSync(process.argv[2], 'utf8');

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

const context = {};
vm.createContext(context);
vm.runInContext(extract('formatEmailValue'), context);

const email = 'James@jameswilliams.business';
const emailCases = [
  // A CN repeating the address is what rendered it twice on the "Trek" event.
  [`mailto:${email}`, {CN: email}, email],
  // The repeat is still a repeat when the two differ only in case.
  [`mailto:${email}`, {CN: email.toLowerCase()}, email],
  [`MAILTO:${email}`, {CN: email}, email],
  // A CN that is genuinely a name is still shown.
  [`mailto:${email}`, {CN: 'James Williams'}, `James Williams <${email}>`],
  [`mailto:${email}`, {}, email],
  [`mailto:${email}`, null, email],
  // Whitespace around the CN does not make it a distinct name.
  [`mailto:${email}`, {CN: `  ${email}  `}, email],
  ['', {CN: 'James Williams'}, ''],
];
for (const [value, params, want] of emailCases) {
  const got = context.formatEmailValue(value, params);
  assert.equal(got, want, `formatEmailValue(${JSON.stringify(value)}, ${JSON.stringify(params)}) = ${JSON.stringify(got)}, want ${JSON.stringify(want)}`);
}

// createEventDateError only exists on the single-calendar page, which is the
// one carrying the create modal.
if (source.includes('function createEventDateError(')) {
  vm.runInContext(extract('createEventDateError'), context);
  const dateCases = [
    // The reported case: an end before the start, rejected before submitting.
    ['2026-09-17T10:00', '2026-09-17T09:00', false, true],
    ['2026-09-18T10:00', '2026-09-17T10:00', false, true],
    // A timed event of zero length is one the server accepts too.
    ['2026-09-17T10:00', '2026-09-17T10:00', false, false],
    ['2026-09-17T10:00', '2026-09-17T11:00', false, false],
    // An all-day event may start and end on the same day; the form's end date
    // is inclusive.
    ['2026-09-17', '2026-09-17', true, false],
    ['2026-09-17', '2026-09-16', true, true],
    ['2026-09-17', '2026-09-18', true, false],
    // An incomplete form is the browser's required-field job, not this check's.
    ['', '2026-09-17T11:00', false, false],
    ['2026-09-17T10:00', '', false, false],
  ];
  for (const [start, end, allDay, wantError] of dateCases) {
    const got = context.createEventDateError(start, end, allDay);
    assert.equal(
      Boolean(got), wantError,
      `createEventDateError(${JSON.stringify(start)}, ${JSON.stringify(end)}, ${allDay}) = ${JSON.stringify(got)}, want ${wantError ? 'an error' : 'null'}`,
    );
  }
}

// A stored STATUS, CLASS or TRANSP the select does not offer is given an
// option of its own, so saving the form unchanged submits it; the option goes
// again when the select is set to another event's value.
{
  class FakeOption {
    constructor(value) { this.value = value; this.textContent = value; this.attributes = {}; }
    setAttribute(name, value) { this.attributes[name] = value; }
    hasAttribute(name) { return Object.prototype.hasOwnProperty.call(this.attributes, name); }
  }
  class FakeSelect {
    constructor(values) { this.options = values.map((v) => new FakeOption(v)); this.selected = ''; }
    remove(i) { this.options.splice(i, 1); }
    appendChild(option) { this.options.push(option); }
    // A select takes only a value one of its options holds.
    set value(v) { this.selected = this.options.some((o) => o.value === v) ? v : ''; }
    get value() { return this.selected; }
  }
  const selectContext = { document: { createElement: () => new FakeOption('') } };
  vm.createContext(selectContext);
  vm.runInContext(extract('selectStoredValue'), selectContext);
  const select = new FakeSelect(['', 'PUBLIC', 'PRIVATE', 'CONFIDENTIAL']);
  selectContext.selectStoredValue(select, 'X-TEAM-ONLY');
  assert.equal(select.value, 'X-TEAM-ONLY', 'an x-name CLASS is selected');
  selectContext.selectStoredValue(select, 'PRIVATE');
  assert.equal(select.value, 'PRIVATE');
  assert.deepEqual(select.options.map((o) => o.value), ['', 'PUBLIC', 'PRIVATE', 'CONFIDENTIAL'],
    'the option added for the earlier event is gone');
  selectContext.selectStoredValue(select, undefined);
  assert.equal(select.value, '');
  assert.equal(select.options.length, 4);
}

// switchEditScope only exists on the single-calendar page; the aggregate page
// reopens its editor on the series itself.
if (source.includes('function switchEditScope(')) {
  const master = { uid: 'u', summary: 'Series', recurrenceId: null };
  const occurrence = { uid: 'u', summary: 'Moved', recurrenceId: new Date(0) };
  const calls = [];
  const scopeContext = {
    window: { baseEvents: [master], currentEditEvent: occurrence, currentEditOccurrence: null },
  };
  scopeContext.showEditEventModal = (event, scope) => {
    calls.push(['show', event.summary, scope]);
    scopeContext.window.currentEditEvent = event;
    scopeContext.window.currentEditOccurrence = null;
  };
  scopeContext.setEditScope = (scope) => calls.push(['scope', scope]);
  vm.createContext(scopeContext);
  vm.runInContext(extract('switchEditScope'), scopeContext);

  scopeContext.switchEditScope('series');
  scopeContext.switchEditScope('occurrence');
  assert.deepEqual(calls, [['show', 'Series', 'series'], ['show', 'Moved', 'occurrence']],
    'the series scope shows the master, and switching back shows the occurrence');

  calls.length = 0;
  scopeContext.window.currentEditEvent = master;
  scopeContext.switchEditScope('occurrence');
  scopeContext.switchEditScope('series');
  assert.deepEqual(calls, [['scope', 'occurrence'], ['scope', 'series']],
    'an editor opened on the series only switches scope');
}
