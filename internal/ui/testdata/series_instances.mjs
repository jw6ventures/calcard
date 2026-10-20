// Runs a calendar view's own placement of the events its endpoint serves and
// prints what it shows: for each events JSON file given, one line holding
// each occurrence as {summary, allDay, start, end, recurrenceId,
// recurrenceIdAllDay, recurrenceTimezone}, in order. A date is printed as
// the local date it lands on and an instant in UTC. Every occurrence has to
// be marked as one and carry its series.
//
// Usage: node series_instances.mjs <view as rendered> <events JSON>...
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
context.globalThis = context;
vm.runInNewContext(
  [
    'function parseICAL() { throw new Error("the server sent an event without a DTSTART"); }',
    extract('parseServerDate'),
    extract('recurrenceIsFloating'),
    extract('normalizeServerEvent'),
    extract('overrideRecurrenceIdAllDay'),
    extract('expandRecurringEvents'),
    'globalThis._show = function(served) { return expandRecurringEvents(served.map(normalizeServerEvent)); };',
  ].join('\n'),
  context,
);

function show(path) {
  const occurrences = context._show(JSON.parse(fs.readFileSync(path, 'utf8')));
  for (const occurrence of occurrences) {
    assert.equal(occurrence.isOccurrence, true, `${occurrence.dtstart} is not marked as an occurrence`);
    assert.equal(occurrence.seriesEvent.uid, 'series', `${occurrence.dtstart} lost its series`);
  }
  return occurrences.sort((a, b) => a.dtstart - b.dtstart);
}

const local = (date) => [date.getFullYear(), date.getMonth() + 1, date.getDate()].map((n) => String(n).padStart(2, '0')).join('-');
const shown = (date, allDay) => (date === null ? null : allDay ? local(date) : date.toISOString());
for (const path of process.argv.slice(3)) {
  console.log(JSON.stringify(show(path).map((o) => ({
    summary: o.summary,
    allDay: !!o.allDay,
    start: shown(o.dtstart, o.allDay),
    end: shown(o.dtend, o.allDay),
    recurrenceId: shown(o.recurrenceId, o.recurrenceIdAllDay),
    recurrenceIdAllDay: o.recurrenceIdAllDay,
    recurrenceTimezone: o.recurrenceTimezone || '',
  }))));
}
