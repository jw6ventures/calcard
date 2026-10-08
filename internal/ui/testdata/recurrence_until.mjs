// Runs a calendar view's own recurrence expansion over series that end on an
// UNTIL, the reading of an UNTIL as the series' last day, and the recurrence
// summary that names that day. RFC 5545 section
// 3.3.10 makes a DATE UNTIL inclusive of its day, so a timed occurrence on that
// day is part of the series. The server ends that day in the zone the DTSTART
// is read in: its TZID zone, or UTC for a DTSTART with no TZID, which is how
// the server reads one and sends it to the page. The UI writes a timed series'
// UNTIL as the last second of the picked day in the event's zone, in UTC, which
// the editor has to show as that day again for an edit to keep it. Run under
// several TZ values: a DATE read as midnight, or a day or UTC value read in the
// browser's zone, is only wrong where the zone is not the event's.
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
    extract('rruleUntilLimit'),
    extract('rruleUntilDay'),
    extract('makeOccurrenceKey'),
    extract('recurrenceZoneClock'),
    extract('recurrenceCalendar'),
    extract('expandRecurringEvents'),
    extract('formatRRule'),
    'globalThis._expand = expandRecurringEvents;',
    'globalThis._untilDay = rruleUntilDay;',
    'globalThis._formatRRule = formatRRule;',
  ].join('\n'),
  context,
);

const zone = Intl.DateTimeFormat().resolvedOptions().timeZone;
const day = (date) => [date.getFullYear(), date.getMonth() + 1, date.getDate()];
const march = [new Date(2026, 2, 1), new Date(2026, 2, 31, 23, 59, 59)];

function series(dtstart, rrule, allDay, timezone, timezoneOffsets) {
  const rruleParsed = {};
  rrule.split(';').forEach((part) => {
    const [key, value] = part.split('=');
    rruleParsed[key] = value;
  });
  return { uid: 'series', dtstart, dtend: null, allDay, timezone, timezoneOffsets, rrule, rruleParsed, exdates: [], overrides: [] };
}

function occurrences(event, range = march) {
  // Array.from builds the list in this realm, which deepEqual compares by prototype.
  return Array.from(context._expand([event], range[0], range[1]), (occurrence) => occurrence.dtstart);
}

const instants = (dates) => dates.map((d) => d.toISOString());
const spring = [new Date(Date.UTC(2026, 1, 1)), new Date(Date.UTC(2026, 3, 1))];
const spring2024 = [new Date(Date.UTC(2024, 1, 1)), new Date(Date.UTC(2024, 3, 1))];

// A timed series with no TZID whose DATE UNTIL is the day of an occurrence
// keeps it...
assert.deepEqual(
  instants(occurrences(series(new Date(Date.UTC(2026, 0, 5, 9, 0)), 'FREQ=WEEKLY;UNTIL=20260302', false))),
  ['2026-03-02T09:00:00.000Z'],
  `timed series with a DATE UNTIL in ${zone}`,
);
// ...and one whose DATE UNTIL is the day before does not.
assert.deepEqual(
  occurrences(series(new Date(Date.UTC(2026, 0, 5, 9, 0)), 'FREQ=WEEKLY;UNTIL=20260301', false)),
  [],
  `timed series ending the day before in ${zone}`,
);
// The UNTIL day of a series with no TZID is a UTC day, which a 22:00 UTC
// occurrence is on wherever it is viewed.
assert.deepEqual(
  instants(occurrences(series(new Date(Date.UTC(2026, 1, 27, 22, 0)), 'FREQ=DAILY;UNTIL=20260302', false), spring)),
  ['2026-02-27T22:00:00.000Z', '2026-02-28T22:00:00.000Z', '2026-03-01T22:00:00.000Z', '2026-03-02T22:00:00.000Z'],
  `timed series with no TZID ending on a UTC day in ${zone}`,
);
// So is that of a series whose TZID the browser does not know, which the
// server reads as UTC.
assert.deepEqual(
  instants(occurrences(series(new Date(Date.UTC(2026, 1, 27, 22, 0)), 'FREQ=DAILY;UNTIL=20260302', false, 'Not/AZone'), spring)),
  ['2026-02-27T22:00:00.000Z', '2026-02-28T22:00:00.000Z', '2026-03-01T22:00:00.000Z', '2026-03-02T22:00:00.000Z'],
  `timed series in an unknown zone ending on a UTC day in ${zone}`,
);

// A series with a TZID ends at the end of its UNTIL day in that zone: a daily
// 22:00 Los Angeles series ending on 10 January keeps 10 January's occurrence,
// which is already 11 January in UTC and further east.
const january = [new Date(Date.UTC(2026, 0, 1)), new Date(Date.UTC(2026, 1, 1))];
assert.deepEqual(
  instants(occurrences(series(new Date(Date.UTC(2026, 0, 6, 6, 0)), 'FREQ=DAILY;UNTIL=20260110', false, 'America/Los_Angeles'), january)),
  [
    '2026-01-06T06:00:00.000Z', '2026-01-07T06:00:00.000Z', '2026-01-08T06:00:00.000Z',
    '2026-01-09T06:00:00.000Z', '2026-01-10T06:00:00.000Z', '2026-01-11T06:00:00.000Z',
  ],
  `Los Angeles series with a DATE UNTIL in ${zone}`,
);
// A daily 09:00 Auckland series ending on 2 March leaves out 3 March's
// occurrence, which is still 2 March in UTC and further west.
assert.deepEqual(
  instants(occurrences(series(new Date(Date.UTC(2026, 1, 25, 20, 0)), 'FREQ=DAILY;UNTIL=20260302', false, 'Pacific/Auckland'), spring)),
  ['2026-02-25T20:00:00.000Z', '2026-02-26T20:00:00.000Z', '2026-02-27T20:00:00.000Z', '2026-02-28T20:00:00.000Z', '2026-03-01T20:00:00.000Z'],
  `Auckland series with a DATE UNTIL in ${zone}`,
);
// Santiago springs forward at midnight on 6 September 2026, so that day has no
// 00:00 and 5 September ends at 04:00 UTC...
const september = [new Date(Date.UTC(2026, 8, 1)), new Date(Date.UTC(2026, 9, 1))];
assert.deepEqual(
  instants(occurrences(series(new Date(Date.UTC(2026, 8, 2, 3, 30)), 'FREQ=DAILY;UNTIL=20260905', false, 'America/Santiago'), september)),
  ['2026-09-02T03:30:00.000Z', '2026-09-03T03:30:00.000Z', '2026-09-04T03:30:00.000Z', '2026-09-05T03:30:00.000Z', '2026-09-06T03:30:00.000Z'],
  `Santiago series ending on a day the next midnight is skipped in ${zone}`,
);
// ...and falls back at midnight on 5 April 2026, reading 23:00-24:00 on 4 April
// twice, so that day runs to 04:00 UTC.
const april = [new Date(Date.UTC(2026, 3, 1)), new Date(Date.UTC(2026, 4, 1))];
assert.deepEqual(
  instants(occurrences(series(new Date(Date.UTC(2026, 3, 2, 2, 30)), 'FREQ=DAILY;UNTIL=20260404', false, 'America/Santiago'), april)),
  ['2026-04-02T02:30:00.000Z', '2026-04-03T02:30:00.000Z', '2026-04-04T02:30:00.000Z', '2026-04-05T02:30:00.000Z'],
  `Santiago series ending on a day read twice in ${zone}`,
);
// An all-day series keeps its DATE UNTIL's day.
assert.deepEqual(
  occurrences(series(new Date(2026, 0, 5), 'FREQ=WEEKLY;UNTIL=20260302', true)).map(day),
  [[2026, 3, 2]],
  `all-day series with a DATE UNTIL in ${zone}`,
);
// The UTC UNTIL the UI writes for a Europe/Berlin series ending on 2 March
// keeps the 09:00 Berlin occurrence that day, wherever it is viewed.
assert.deepEqual(
  occurrences(series(new Date(Date.UTC(2026, 0, 5, 8, 0)), 'FREQ=WEEKLY;UNTIL=20260302T225959Z', false)).map((d) => d.toISOString()),
  ['2026-03-02T08:00:00.000Z'],
  `timed series with a UTC UNTIL in ${zone}`,
);

// A floating UNTIL beside a timed DTSTART, which RFC 5545 section 3.3.10 does
// not allow but clients write, is read in the zone the DTSTART is read in, as
// the server reads it: 09:00 Berlin on 2 March keeps that day's 09:00 Berlin
// occurrence, and 09:00 with no TZID is 09:00 UTC.
assert.deepEqual(
  instants(occurrences(series(new Date(Date.UTC(2026, 0, 5, 8, 0)), 'FREQ=WEEKLY;UNTIL=20260302T090000', false, 'Europe/Berlin'))),
  ['2026-03-02T08:00:00.000Z'],
  `Berlin series with a floating UNTIL in ${zone}`,
);
assert.deepEqual(
  instants(occurrences(series(new Date(Date.UTC(2026, 0, 5, 9, 0)), 'FREQ=WEEKLY;UNTIL=20260302T090000', false))),
  ['2026-03-02T09:00:00.000Z'],
  `timed series with no TZID and a floating UNTIL in ${zone}`,
);
assert.deepEqual(
  occurrences(series(new Date(Date.UTC(2026, 0, 5, 9, 0)), 'FREQ=WEEKLY;UNTIL=20260302T085959', false)),
  [],
  `timed series with no TZID and a floating UNTIL before its last instance in ${zone}`,
);
// An all-day series' DATEs are UTC midnights to the server, so a UTC UNTIL
// keeps the instance on its UTC date and none after.
assert.deepEqual(
  occurrences(series(new Date(2026, 0, 5), 'FREQ=WEEKLY;UNTIL=20260302T000000Z', true)).map(day),
  [[2026, 3, 2]],
  `all-day series with a UTC UNTIL on its last day in ${zone}`,
);
assert.deepEqual(
  occurrences(series(new Date(2026, 0, 5), 'FREQ=WEEKLY;UNTIL=20260301T235959Z', true)),
  [],
  `all-day series with a UTC UNTIL the day before in ${zone}`,
);

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
// A DATE UNTIL ends where the next day begins on the listed clock: a daily
// 22:00 series ending on 12 March keeps that day's instance, 02:00 UTC on the
// 13th.
assert.deepEqual(
  instants(occurrences(series(new Date('2024-03-09T03:00:00Z'), 'FREQ=DAILY;UNTIL=20240312', false, 'Eastern Standard Time', eastern), spring2024)),
  ['2024-03-09T03:00:00.000Z', '2024-03-10T03:00:00.000Z', '2024-03-11T02:00:00.000Z', '2024-03-12T02:00:00.000Z', '2024-03-13T02:00:00.000Z'],
  `listed zone series with a DATE UNTIL in ${zone}`,
);
