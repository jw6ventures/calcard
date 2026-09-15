// Exercises the aggregate calendar editor's all-day end conversion.
//
// The editor works in the exclusive DTEND an all-day VEVENT stores, while the
// form's end date names the last day the event covers and the server adds a day
// back on submit. Handing the stored value to the input unconverted re-counted a
// day the event already had, so every save lengthened it by one. There is no
// JavaScript test runner in this repository; internal/ui/templates_test.go runs
// this under node when node is available.
import fs from 'node:fs';

// Pull the real helpers straight out of the template so this exercises the
// shipped code, not a paraphrase of it.
const html = fs.readFileSync(process.argv[2], 'utf8');
function extract(name) {
  const start = html.indexOf('function ' + name + '(');
  if (start < 0) throw new Error('missing ' + name);
  let depth = 0, i = html.indexOf('{', start);
  const from = i;
  for (; i < html.length; i++) {
    if (html[i] === '{') depth++;
    else if (html[i] === '}') { depth--; if (depth === 0) break; }
  }
  return html.slice(start, i + 1);
}
const src = ['formatLocalDate','formatDateInput','formatDateTimeInput','startOfDay','addDays',
             'inclusiveAllDayEnd','exclusiveAllDayEnd','setEditorDateValues',
             'parseServerDate','normalizeServerEvent','makeOccurrenceKey']
  .map(extract).join('\n');

// Enough of a DOM for setEditorDateValues, which is the function that actually
// fills the form. Testing the helpers alone would miss a call site that stopped
// using them.
const inputs = {
  'event-dtstart': {type: 'datetime-local', value: ''},
  'event-dtend': {type: 'datetime-local', value: ''},
  'event-all-day': {checked: false},
};
const document = {getElementById: (id) => inputs[id]};
// normalizeServerEvent only reaches parseICAL when the server sent no dtstart,
// which none of these payloads do.
const parseICAL = () => ({});
const mod = new Function('document', 'parseICAL', src +
  '\nreturn {formatLocalDate, startOfDay, addDays, inclusiveAllDayEnd, exclusiveAllDayEnd,' +
  ' setEditorDateValues, parseServerDate, normalizeServerEvent, makeOccurrenceKey};')(document, parseICAL);

// editorEnd runs the real form fill and reports the end date it wrote.
const editorEnd = (start, exclusiveEnd) => {
  mod.setEditorDateValues(start, exclusiveEnd, true);
  return inputs['event-dtend'].value;
};

// The server's half of the contract: FormatICalDateTime adds one day to an
// all-day end, so the submitted date is inclusive and DTEND is exclusive.
const submitToDTEND = (dateStr) => {
  const d = new Date(dateStr + 'T00:00:00');
  return mod.addDays(d, 1);
};

let failures = 0;
const check = (label, got, want) => {
  const ok = got === want;
  if (!ok) failures++;
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${label}: got ${got}, want ${want}`);
};

// A one-day all-day event on 2025-07-24: DTSTART 20250724, DTEND 20250725.
let dtstart = new Date('2025-07-24T00:00:00');
let dtend = new Date('2025-07-25T00:00:00');
for (let save = 1; save <= 3; save++) {
  const shown = editorEnd(dtstart, dtend);
  check(`save ${save}: editor shows end`, shown, '2025-07-24');
  dtend = submitToDTEND(shown);
  check(`save ${save}: stored DTEND`, mod.formatLocalDate(dtend), '2025-07-25');
}

// A three-day event, 2025-07-24..26 => DTEND 20250727.
dtstart = new Date('2025-07-24T00:00:00');
dtend = new Date('2025-07-27T00:00:00');
let shown = editorEnd(dtstart, dtend);
check('multi-day: editor shows end', shown, '2025-07-26');
check('multi-day: stored DTEND', mod.formatLocalDate(submitToDTEND(shown)), '2025-07-27');

// Creating from the all-day lane passes an exclusive end of day+1.
const lane = new Date('2025-07-24T00:00:00');
shown = editorEnd(lane, mod.addDays(lane, 1));
check('create from lane: editor shows end', shown, '2025-07-24');
check('create from lane: stored DTEND', mod.formatLocalDate(submitToDTEND(shown)), '2025-07-25');

// Toggling a timed 10:00-10:30 event to all-day must give one day, not two.
let start = new Date('2025-07-24T10:00:00'), end = new Date('2025-07-24T10:30:00');
start = mod.startOfDay(start);
end = mod.exclusiveAllDayEnd(start, mod.startOfDay(end));
shown = editorEnd(start, end);
check('toggle same-day timed to all-day', shown, '2025-07-24');

// Toggling a timed event that ends on a later day keeps that last day.
start = mod.startOfDay(new Date('2025-07-24T10:00:00'));
end = mod.exclusiveAllDayEnd(start, mod.startOfDay(new Date('2025-07-26T10:30:00')));
shown = editorEnd(start, end);
check('toggle multi-day timed to all-day', shown, '2025-07-26');

// A degenerate stored DTEND equal to DTSTART must not show a date before start.
shown = editorEnd(new Date('2025-07-24T00:00:00'), new Date('2025-07-24T00:00:00'));
check('degenerate DTEND == DTSTART', shown, '2025-07-24');


// --- The server-JSON path -------------------------------------------------
//
// Everything above builds its dates the way parseICALDate does, from local
// calendar parts. The page does not normally get them that way: the server
// parses an all-day DATE as UTC midnight (parseCalendarEventDate) and emits it
// as RFC3339 (addToPayload), so normalizeServerEvent receives an instant while
// every reader downstream -- the month grid, the day modal, the agenda list and
// the editor -- takes local calendar parts off it. West of UTC that instant
// lands on the previous day, so the event displayed and saved one day early and
// walked backwards on every save.

// serverPayload is the shape addToPayload emits for an all-day event.
const serverPayload = (startDate, endDate) => ({
  uid: 'all-day', allDay: true,
  dtstart: startDate + 'T00:00:00Z',
  dtend: endDate + 'T00:00:00Z',
});

// storeSubmission is the server's half of a save: DTSTART is the submitted
// start, DTEND the submitted end plus a day, and both come back as UTC midnight.
const storeSubmission = (startStr, endStr) =>
  serverPayload(startStr, mod.formatLocalDate(submitToDTEND(endStr)));

let ev = mod.normalizeServerEvent(serverPayload('2025-07-24', '2025-07-25'));
check('server payload: event lands on its own day',
  mod.formatLocalDate(ev.dtstart), '2025-07-24');
// The grid subtracts a day from an exclusive all-day end to get the last day
// covered, so this is the day the event is drawn through.
check('server payload: last day covered',
  mod.formatLocalDate(mod.addDays(ev.dtend, -1)), '2025-07-24');

for (let save = 1; save <= 3; save++) {
  mod.setEditorDateValues(ev.dtstart, ev.dtend, true);
  const s = inputs['event-dtstart'].value, e = inputs['event-dtend'].value;
  check(`server round-trip save ${save}: form start`, s, '2025-07-24');
  check(`server round-trip save ${save}: form end`, e, '2025-07-24');
  ev = mod.normalizeServerEvent(storeSubmission(s, e));
}

// A three-day all-day event, 2025-07-24..26, stored as DTEND 20250727.
ev = mod.normalizeServerEvent(serverPayload('2025-07-24', '2025-07-27'));
mod.setEditorDateValues(ev.dtstart, ev.dtend, true);
check('server payload multi-day: form start', inputs['event-dtstart'].value, '2025-07-24');
check('server payload multi-day: form end', inputs['event-dtend'].value, '2025-07-26');

// RECURRENCE-ID and EXDATE of an all-day series arrive the same way, and
// expandRecurringEvents matches them against dtstart through makeOccurrenceKey.
// A key built from a differently-shifted value never matches.
const series = mod.normalizeServerEvent(Object.assign(
  serverPayload('2025-07-24', '2025-07-25'),
  {rrule: 'FREQ=DAILY', exdates: ['2025-07-26T00:00:00Z'],
   recurrenceId: '2025-07-24T00:00:00Z', recurrenceIdAllDay: true}));
check('all-day exdate lands on its own day',
  mod.makeOccurrenceKey(series.exdates[0], true), '2025-07-26');
check('all-day recurrence id lands on its own day',
  mod.makeOccurrenceKey(series.recurrenceId, true), '2025-07-24');
check('recurrence id keys with dtstart',
  mod.makeOccurrenceKey(series.recurrenceId, true),
  mod.makeOccurrenceKey(series.dtstart, true));

// A timed event must keep its instant: normalizing it onto a local date would
// move the event by the UTC offset.
const timed = mod.normalizeServerEvent({
  uid: 'timed', allDay: false,
  dtstart: '2025-07-24T15:00:00Z', dtend: '2025-07-24T16:00:00Z',
});
check('timed event keeps its instant',
  timed.dtstart.toISOString(), '2025-07-24T15:00:00.000Z');

console.log(failures === 0 ? '\nALL PASS' : `\n${failures} FAILURES`);
process.exit(failures === 0 ? 0 : 1);
