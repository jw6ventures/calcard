// Exercises calendar_view.html's create/edit-modal all-day end conversion.
//
// This is a sibling of all_day_end_roundtrip.mjs rather than an extension of
// it: calendar_view.html has no counterpart to all_calendars_view.html's
// single setEditorDateValues. showCreateEventModal and showEditEventModal
// each fill their own dtstart/dtend inputs directly, showEditEventModal's
// body embeds a Go template pipeline ({{.Calendar.ID}}), and both drag in a
// much larger DOM surface (recurrence fields, delete forms, scope buttons)
// that a shared extractor would have to fake regardless of which template it
// targets. Stubbing all of that inside the other script's fake DOM would
// have made it harder to follow for both templates; a second script that
// neutralizes the one template expression and fakes only what these two
// functions touch stays readable for each.
import fs from 'node:fs';

const html = fs.readFileSync(process.argv[2], 'utf8');
function extract(name) {
  const start = html.indexOf('function ' + name + '(');
  if (start < 0) throw new Error('missing ' + name);
  let depth = 0, i = html.indexOf('{', start);
  for (; i < html.length; i++) {
    if (html[i] === '{') depth++;
    else if (html[i] === '}') { depth--; if (depth === 0) break; }
  }
  return html.slice(start, i + 1);
}

const pure = ['startOfDay', 'addDays', 'inclusiveAllDayEnd', 'exclusiveAllDayEnd',
              'formatDateForInput', 'formatDateTimeForInput',
              'parseServerDate', 'normalizeServerEvent']
  .map(extract).join('\n');

// showEditEventModal's only server-templated expression, so it can run
// outside Go's html/template like the rest of the extracted source.
const showEdit = extract('showEditEventModal').replace('{{.Calendar.ID}}', '1');
const showCreate = extract('showCreateEventModal');
const toggle = extract('toggleAllDay');

const src = [pure, showCreate, showEdit, toggle].join('\n');

// getElementById hands out a fresh, remembered stub for any id, so the real
// functions can read back what they wrote without this harness enumerating
// every field showEditEventModal happens to touch (recurrence selects, scope
// buttons, ...) -- none of which the dtstart/dtend conversion depends on.
function makeElement() {
  return {
    value: '', type: '', checked: false, action: '', textContent: '', innerHTML: '',
    style: {}, classList: { add() {}, remove() {} },
    focus() {}, appendChild() {}, removeChild() {},
  };
}
const elements = {};
const document = {
  getElementById(id) {
    if (!elements[id]) elements[id] = makeElement();
    return elements[id];
  },
  querySelectorAll() { return []; },
  querySelector() { return makeElement(); },
};

// Everything showEditEventModal calls beyond the date fields: recurrence and
// scope wiring that never reads or writes dtstart/dtend.
const noop = () => {};
const window = {};
const mod = new Function(
  'document', 'window', 'alert', 'setReminderList',
  'toggleRecurrenceOptions', 'toggleRecurrenceEnd', 'setEditScope',
  src + '\nreturn {startOfDay, addDays, inclusiveAllDayEnd, exclusiveAllDayEnd,' +
    ' formatDateForInput, formatDateTimeForInput, parseServerDate, normalizeServerEvent,' +
    ' showCreateEventModal, showEditEventModal, toggleAllDay};'
)(document, window, noop, noop, noop, noop, noop);

// The real code reaches these off window because showEditEventModal and
// showCreateEventModal live outside the closure that defines them.
window.startOfDay = mod.startOfDay;
window.inclusiveAllDayEnd = mod.inclusiveAllDayEnd;
window.exclusiveAllDayEnd = mod.exclusiveAllDayEnd;

let failures = 0;
const check = (label, got, want) => {
  const ok = got === want;
  if (!ok) failures++;
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${label}: got ${got}, want ${want}`);
};

// The server's half of the contract: the submitted end date is inclusive,
// DTEND stored is that date plus one day.
const submitToDTEND = (dateStr) => mod.addDays(new Date(dateStr + 'T00:00:00'), 1);

// --- showEditEventModal ----------------------------------------------------

let dtstart = new Date(2025, 6, 24);
let dtend = new Date(2025, 6, 25); // one-day event, exclusive DTEND
for (let save = 1; save <= 3; save++) {
  mod.showEditEventModal({ uid: 'e1', allDay: true, dtstart, dtend });
  const shown = document.getElementById('edit-dtend').value;
  check(`edit save ${save}: shows end`, shown, '2025-07-24');
  dtend = submitToDTEND(shown);
  check(`edit save ${save}: stored DTEND`, mod.formatDateForInput(dtend), '2025-07-25');
}

dtstart = new Date(2025, 6, 24);
dtend = new Date(2025, 6, 27); // three-day event
mod.showEditEventModal({ uid: 'e2', allDay: true, dtstart, dtend });
check('edit multi-day: shows end', document.getElementById('edit-dtend').value, '2025-07-26');

// A stored all-day DTEND equal to DTSTART is degenerate but storable, and is
// the shape that showed an end before the start.
mod.showEditEventModal({ uid: 'e3', allDay: true, dtstart: new Date(2025, 6, 24), dtend: new Date(2025, 6, 24) });
check('edit degenerate DTEND == DTSTART', document.getElementById('edit-dtend').value, '2025-07-24');

// A timed event is untouched by the all-day conversion.
mod.showEditEventModal({
  uid: 'e4', allDay: false, timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
  dtstart: new Date(2025, 6, 24, 10, 0), dtend: new Date(2025, 6, 24, 10, 30),
});
check('edit timed: shows end', document.getElementById('edit-dtend').value, '2025-07-24T10:30');

// The server payload path: normalizeServerEvent rebuilds an all-day date on
// local midnight from the UTC instant the server sends, the same shape
// showEditEventModal's own helpers expect.
let serverEvent = mod.normalizeServerEvent({
  uid: 'e5', allDay: true, dtstart: '2025-07-24T00:00:00Z', dtend: '2025-07-25T00:00:00Z',
});
mod.showEditEventModal(serverEvent);
check('edit server payload: shows end', document.getElementById('edit-dtend').value, '2025-07-24');

serverEvent = mod.normalizeServerEvent({
  uid: 'e6', allDay: true, dtstart: '2025-07-24T00:00:00Z', dtend: '2025-07-24T00:00:00Z',
});
mod.showEditEventModal(serverEvent);
check('edit server payload degenerate: shows end', document.getElementById('edit-dtend').value, '2025-07-24');

// --- showCreateEventModal ---------------------------------------------------

mod.showCreateEventModal({ start: new Date(2025, 6, 24), end: new Date(2025, 6, 25), allDay: true });
check('create: shows end', document.getElementById('create-dtend').value, '2025-07-24');

mod.showCreateEventModal({ start: new Date(2025, 6, 24), end: new Date(2025, 6, 24), allDay: true });
check('create degenerate: shows end', document.getElementById('create-dtend').value, '2025-07-24');

// --- toggleAllDay ------------------------------------------------------------

// Toggling a same-day timed event to all-day must give one day, not two.
document.getElementById('create-all-day').checked = true;
document.getElementById('create-dtstart').value = '2025-07-24T10:00';
document.getElementById('create-dtend').value = '2025-07-24T10:30';
mod.toggleAllDay('create', false);
check('toggle same-day timed to all-day', document.getElementById('create-dtend').value, '2025-07-24');

// Toggling a timed event that ends on a later day keeps that last day.
document.getElementById('create-all-day').checked = true;
document.getElementById('create-dtstart').value = '2025-07-24T10:00';
document.getElementById('create-dtend').value = '2025-07-26T10:30';
mod.toggleAllDay('create', false);
check('toggle multi-day timed to all-day', document.getElementById('create-dtend').value, '2025-07-26');

// A timed end already before its start must not carry over as an all-day end
// before its start.
document.getElementById('create-all-day').checked = true;
document.getElementById('create-dtstart').value = '2025-07-24T10:00';
document.getElementById('create-dtend').value = '2025-07-20T09:00';
mod.toggleAllDay('create', false);
check('toggle end-before-start clamps start', document.getElementById('create-dtstart').value, '2025-07-24');
check('toggle end-before-start clamps end', document.getElementById('create-dtend').value, '2025-07-24');

console.log(failures === 0 ? '\nALL PASS' : `\n${failures} FAILURES`);
process.exit(failures === 0 ? 0 : 1);
