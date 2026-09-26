// Runs a calendar view's own parseICAL over DATE and DATE-TIME values, and
// checks that each lands on the instant RFC 5545 section 3.3.5 gives it: a
// value ending in Z is UTC, and a floating or TZID value is wall-clock time,
// which this page shows in the browser's zone. Run under several TZ values:
// reading a UTC value as local is only visible where the zone is not UTC.
//
// Usage: node ical_date_parsing.mjs calendar_view.html safe_html_js.tmpl
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

const context = {};
context.globalThis = context;
vm.runInNewContext(
  [
    extract('unescapeText'),
    extract('unfoldLines'),
    extract('parseICALDate'),
    extract('parseRRule'),
    extract('parseTriggerMinutes'),
    extract('formatEmailValue'),
    extract('parseICAL'),
    'globalThis._parseICAL = parseICAL;',
    'globalThis._parseICALDate = parseICALDate;',
  ].join('\n'),
  context,
);

const zone = Intl.DateTimeFormat().resolvedOptions().timeZone;
const iso = (date) => date.toISOString();
const local = (date) => [date.getFullYear(), date.getMonth() + 1, date.getDate(), date.getHours(), date.getMinutes(), date.getSeconds()];

const event = context._parseICAL([
  'BEGIN:VCALENDAR',
  'BEGIN:VEVENT',
  'UID:utc',
  'DTSTART:20250724T130000Z',
  'DTEND:20250724T143000Z',
  'RRULE:FREQ=DAILY;UNTIL=20250730T130000Z',
  'EXDATE:20250725T130000Z,20250726T130000Z',
  'EXDATE:20250727T130000Z',
  'END:VEVENT',
  'END:VCALENDAR',
].join('\r\n'));

assert.equal(iso(event.dtstart), '2025-07-24T13:00:00.000Z', `DTSTART in ${zone}`);
assert.equal(iso(event.dtend), '2025-07-24T14:30:00.000Z', `DTEND in ${zone}`);
// Every date of a multi-valued EXDATE is excluded, not only the first.
assert.deepEqual(
  [...event.exdates].map(iso),
  ['2025-07-25T13:00:00.000Z', '2025-07-26T13:00:00.000Z', '2025-07-27T13:00:00.000Z'],
  `EXDATE in ${zone}`,
);
assert.equal(iso(context._parseICALDate(event.rruleParsed.UNTIL, {})), '2025-07-30T13:00:00.000Z', `UNTIL in ${zone}`);

// Floating and TZID values are wall-clock times, and a DATE is local midnight.
const floating = context._parseICAL([
  'BEGIN:VEVENT',
  'UID:floating',
  'DTSTART;TZID=America/New_York:20250724T090000',
  'DTEND:20250724T100000',
  'RECURRENCE-ID;VALUE=DATE:20250726',
  'END:VEVENT',
].join('\r\n'));
assert.deepEqual(local(floating.dtstart), [2025, 7, 24, 9, 0, 0], `TZID DTSTART in ${zone}`);
assert.deepEqual(local(floating.dtend), [2025, 7, 24, 10, 0, 0], `floating DTEND in ${zone}`);
assert.deepEqual(local(floating.recurrenceId), [2025, 7, 26, 0, 0, 0], `DATE RECURRENCE-ID in ${zone}`);

// A value that is not a date is an invalid Date, not a guess.
assert.ok(Number.isNaN(context._parseICALDate('2025-07-24', {}).getTime()), 'an extended-format value was accepted');
