// Runs a calendar view's own reading of an UNTIL as the series' last day, and
// the recurrence summary that names that day. RFC 5545 section 3.3.10 makes a
// DATE UNTIL inclusive of its day, which the server ends in the zone the
// DTSTART is read in: its TZID zone, or UTC for a DTSTART with no TZID, which
// is how the server reads one and sends it to the page. The UI writes a timed
// series' UNTIL as the last second of the picked day in the event's zone, in
// UTC, which the summary has to name as that day again. Run under several TZ
// values: a DATE read as midnight, or a day or UTC value read in the browser's
// zone, is only wrong where the zone is not the event's.
//
// Usage: node recurrence_until.mjs <calendar_view.html as rendered>
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
    extract('parseICALDate'),
    extract('listedOffsets'),
    extract('intlOffsets'),
    extract('timeZoneClock'),
    extract('rruleUntilDay'),
    extract('formatRRule'),
    extract('recurrenceSummary'),
    'globalThis._untilDay = rruleUntilDay;',
    'globalThis._recurrenceSummary = recurrenceSummary;',
    'globalThis._formatRRule = formatRRule;',
  ].join('\n'),
  context,
);

const zone = Intl.DateTimeFormat().resolvedOptions().timeZone;
const day = (date) => [date.getFullYear(), date.getMonth() + 1, date.getDate()];

const timed = (timezone) => ({ allDay: false, timezone });
// The day a series ends on is the UNTIL's date in the event's own zone.
assert.deepEqual(day(context._untilDay('20260302T225959Z', timed('Europe/Berlin'))), [2026, 3, 2], `Berlin UNTIL in ${zone}`);
assert.deepEqual(day(context._untilDay('20260303T075959Z', timed('America/Los_Angeles'))), [2026, 3, 2], `Los Angeles UNTIL in ${zone}`);
assert.deepEqual(day(context._untilDay('20260302T235959Z', timed('UTC'))), [2026, 3, 2], `UTC UNTIL in ${zone}`);
assert.deepEqual(day(context._untilDay('20260302', timed('Europe/Berlin'))), [2026, 3, 2], `DATE UNTIL in ${zone}`);
assert.deepEqual(day(context._untilDay('20260302T090000', timed('Europe/Berlin'))), [2026, 3, 2], `floating UNTIL in ${zone}`);
// A timed series with no TZID, or one Intl does not know, is read in UTC, as
// the server reads both and writes their UNTIL.
assert.deepEqual(day(context._untilDay('20260302T235959Z', timed(undefined))), [2026, 3, 2], `UNTIL with no zone in ${zone}`);
assert.deepEqual(day(context._untilDay('20260302T235959Z', timed('Not/AZone'))), [2026, 3, 2], `unknown zone in ${zone}`);
assert.deepEqual(day(context._untilDay('20260302T235959Z', timed('Eastern Standard Time'))), [2026, 3, 2], `Outlook zone name in ${zone}`);
// An all-day series ends on its DATE UNTIL's day, and on a UTC UNTIL's UTC date.
assert.deepEqual(day(context._untilDay('20260302', { allDay: true })), [2026, 3, 2], `all-day DATE UNTIL in ${zone}`);
assert.deepEqual(day(context._untilDay('20260302T000000Z', { allDay: true })), [2026, 3, 2], `all-day UTC UNTIL in ${zone}`);
assert.deepEqual(day(context._untilDay('20260302T235959Z', { allDay: true })), [2026, 3, 2], `all-day late UTC UNTIL in ${zone}`);
assert.ok(Number.isNaN(context._untilDay('2026-03-02', timed('UTC')).getTime()), 'an extended-format UNTIL was accepted');

// The recurrence summary names that same day: a timed series saved with no
// time zone ends at 23:59:59 UTC on 31 January, which is 1 February east of
// UTC.
const until = (event) => context._formatRRule(event).replace(/^.*, until /, '');
const local = (y, m, d) => new Date(y, m - 1, d).toLocaleDateString();
assert.equal(
  context._formatRRule({ rrule: 'FREQ=WEEKLY;UNTIL=20240131T235959Z', allDay: false }),
  'Weekly, until ' + local(2024, 1, 31),
  `summary of a series with no zone in ${zone}`,
);
assert.equal(until({ rrule: 'FREQ=WEEKLY;UNTIL=20260302T225959Z', allDay: false, timezone: 'Europe/Berlin' }), local(2026, 3, 2), `summary of a Berlin series in ${zone}`);
assert.equal(until({ rrule: 'FREQ=WEEKLY;UNTIL=20260302T235959Z', allDay: false, timezone: 'Not/AZone' }), local(2026, 3, 2), `summary in an unknown zone in ${zone}`);
assert.equal(until({ rrule: 'FREQ=WEEKLY;UNTIL=20260302', allDay: true }), local(2026, 3, 2), `summary of an all-day series in ${zone}`);
assert.equal(until({ rrule: 'FREQ=WEEKLY;UNTIL=20260302T000000Z', allDay: true }), local(2026, 3, 2), `summary of an all-day series with a UTC UNTIL in ${zone}`);
assert.equal(context._formatRRule({ rrule: 'FREQ=DAILY;COUNT=3', allDay: false }), 'Daily, 3 times', `summary of a counted series in ${zone}`);

// A zone the object defines is read through the offsets the server lists for
// it, which win over Intl's zone of the same name: the server writes the UNTIL
// of an "Eastern Standard Time" series ending on 25 March 2024 as that day's
// last second in daylight time, 03:59:59 UTC on the 26th.
const eastern = {
  'Eastern Standard Time': [
    { from: '2024-03-01T00:00:00Z', offset: -18000 },
    { from: '2024-03-10T07:00:00Z', offset: -14400 },
  ],
};
const listed = { allDay: false, timezone: 'Eastern Standard Time', timezoneOffsets: eastern };
assert.deepEqual(day(context._untilDay('20240326T035959Z', listed)), [2024, 3, 25], `listed zone UNTIL in ${zone}`);
assert.equal(
  until({ ...listed, rrule: 'FREQ=WEEKLY;BYDAY=MO;UNTIL=20240326T035959Z' }),
  local(2024, 3, 25),
  `summary of a series in a listed zone in ${zone}`,
);
const berlinListed = { allDay: false, timezone: 'Europe/Berlin', timezoneOffsets: { 'Europe/Berlin': [{ from: '2026-01-01T00:00:00Z', offset: -18000 }] } };
assert.deepEqual(day(context._untilDay('20260303T020000Z', berlinListed)), [2026, 3, 2], `listing for a zone Intl knows in ${zone}`);

// The details of a series say when the view cannot show all of it: when the
// range holds more instances than the server lists, and when the server
// could not list any.
assert.equal(context._recurrenceSummary({ rrule: 'FREQ=DAILY;COUNT=3', instances: [], instancesComplete: true }), 'Daily, 3 times', `complete summary in ${zone}`);
assert.match(context._recurrenceSummary({ rrule: 'FREQ=HOURLY', instances: [], instancesComplete: false }), /not every instance/, `partial summary in ${zone}`);
assert.match(context._recurrenceSummary({ rrule: 'FREQ=DAILY' }), /only its first instance/, `unlisted summary in ${zone}`);
assert.match(context._recurrenceSummary({ instances: [], instancesComplete: true }), /^On set dates$/, `RDATE-only summary in ${zone}`);
