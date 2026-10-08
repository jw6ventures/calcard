// Runs a calendar view's own recurrence expansion over series whose TZID zone
// skips or repeats their time of day. RFC 5545 section 3.3.10 leaves an
// instance the rule generates on a skipped local time out of the series, and
// out of its COUNT, so a weekly Sunday 02:30 Europe/Berlin series of four runs
// 15 and 22 March and 5 and 12 April 2026, as CalDAV clients and the server
// expand it. A repeated 02:30 is its first occurrence (section 3.3.5). The
// series repeats on its own zone's wall clock wherever the browser is, and one
// with no TZID or a TZID Intl does not know repeats on UTC's, so the script
// runs under several TZ values and expects the same instants in each.
//
// Usage: node recurrence_skipped_local_time.mjs <calendar_view.html as rendered>
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
    'globalThis._expand = expandRecurringEvents;',
  ].join('\n'),
  context,
);

const zone = Intl.DateTimeFormat().resolvedOptions().timeZone;
const range = [new Date(Date.UTC(2026, 0, 1)), new Date(Date.UTC(2027, 0, 1))];

function series(dtstart, timezone, rrule, timezoneOffsets) {
  const rruleParsed = {};
  rrule.split(';').forEach((part) => {
    const [key, value] = part.split('=');
    rruleParsed[key] = value;
  });
  return { uid: 'series', dtstart: new Date(dtstart), dtend: null, allDay: false, timezone, timezoneOffsets, rrule, rruleParsed, exdates: [], overrides: [] };
}

function starts(event) {
  return Array.from(context._expand([event], range[0], range[1]), (occurrence) => occurrence.dtstart.toISOString());
}

const cases = [
  {
    name: 'weekly Sunday 02:30 across the Berlin spring change',
    event: series('2026-03-15T01:30:00Z', 'Europe/Berlin', 'FREQ=WEEKLY;COUNT=4'),
    want: ['2026-03-15T01:30:00.000Z', '2026-03-22T01:30:00.000Z', '2026-04-05T00:30:00.000Z', '2026-04-12T00:30:00.000Z'],
  },
  {
    name: 'weekly on BYDAY=SU across the Berlin spring change',
    event: series('2026-03-15T01:30:00Z', 'Europe/Berlin', 'FREQ=WEEKLY;BYDAY=SU;COUNT=4'),
    want: ['2026-03-15T01:30:00.000Z', '2026-03-22T01:30:00.000Z', '2026-04-05T00:30:00.000Z', '2026-04-12T00:30:00.000Z'],
  },
  {
    name: 'a repeated 02:30 is its first occurrence',
    event: series('2026-10-18T00:30:00Z', 'Europe/Berlin', 'FREQ=WEEKLY;COUNT=3'),
    want: ['2026-10-18T00:30:00.000Z', '2026-10-25T00:30:00.000Z', '2026-11-01T01:30:00.000Z'],
  },
  {
    name: 'daily 02:30 across the New York spring change',
    event: series('2026-03-07T07:30:00Z', 'America/New_York', 'FREQ=DAILY;COUNT=3'),
    want: ['2026-03-07T07:30:00.000Z', '2026-03-09T06:30:00.000Z', '2026-03-10T06:30:00.000Z'],
  },
  {
    name: 'a Berlin 09:00 series keeps its wall clock across the change',
    event: series('2026-03-22T08:00:00Z', 'Europe/Berlin', 'FREQ=WEEKLY;COUNT=2'),
    want: ['2026-03-22T08:00:00.000Z', '2026-03-29T07:00:00.000Z'],
  },
  {
    name: 'an UNTIL bounds the series on its instants',
    event: series('2026-03-15T01:30:00Z', 'Europe/Berlin', 'FREQ=WEEKLY;UNTIL=20260405T003000Z'),
    want: ['2026-03-15T01:30:00.000Z', '2026-03-22T01:30:00.000Z', '2026-04-05T00:30:00.000Z'],
  },
];
for (const { name, event, want } of cases) {
  assert.deepEqual(starts(event), want, `${name} viewed in ${zone}`);
}

// A series with no TZID, or with one Intl does not know, repeats on UTC's wall
// clock, as the server reads and expands both: its instants keep their UTC
// time of day and weekday across the browser's own transitions, and BYDAY
// names UTC weekdays.
const utcCases = [
  {
    name: 'daily 09:00 UTC across the Berlin and New York autumn changes',
    dtstart: '2026-10-24T09:00:00Z', rrule: 'FREQ=DAILY;COUNT=10',
    want: [
      '2026-10-24T09:00:00.000Z', '2026-10-25T09:00:00.000Z', '2026-10-26T09:00:00.000Z', '2026-10-27T09:00:00.000Z',
      '2026-10-28T09:00:00.000Z', '2026-10-29T09:00:00.000Z', '2026-10-30T09:00:00.000Z', '2026-10-31T09:00:00.000Z',
      '2026-11-01T09:00:00.000Z', '2026-11-02T09:00:00.000Z',
    ],
  },
  {
    name: 'weekly on Mondays at 23:30 UTC across the New York spring change',
    dtstart: '2026-03-02T23:30:00Z', rrule: 'FREQ=WEEKLY;BYDAY=MO;COUNT=3',
    want: ['2026-03-02T23:30:00.000Z', '2026-03-09T23:30:00.000Z', '2026-03-16T23:30:00.000Z'],
  },
  {
    name: 'monthly on the 15th at 23:30 UTC',
    dtstart: '2026-01-15T23:30:00Z', rrule: 'FREQ=MONTHLY;BYMONTHDAY=15;COUNT=3',
    want: ['2026-01-15T23:30:00.000Z', '2026-02-15T23:30:00.000Z', '2026-03-15T23:30:00.000Z'],
  },
];
for (const { name, dtstart, rrule, want } of utcCases) {
  for (const timezone of [undefined, 'UTC', 'Not/AZone', 'Eastern Standard Time']) {
    assert.deepEqual(starts(series(dtstart, timezone, rrule)), want, `${name} with TZID ${timezone} viewed in ${zone}`);
  }
}

// A zone the object defines reaches the browser as the server's listing of
// the offsets that definition keeps, in the shape the server sends it: each
// entry holds from its instant until the next. The series repeats on the wall
// clock that listing gives, as the server expands it, whether or not Intl
// knows the zone's name: Outlook's "Eastern Standard Time" springs forward on
// 10 March 2024 and falls back on 3 November.
const eastern = {
  'Eastern Standard Time': [
    { from: '2024-03-01T00:00:00Z', offset: -18000 },
    { from: '2024-03-10T07:00:00Z', offset: -14400 },
    { from: '2024-11-03T06:00:00Z', offset: -18000 },
  ],
};
const listedCases = [
  {
    name: 'weekly Monday 09:00 across the spring change',
    event: series('2024-03-04T14:00:00Z', 'Eastern Standard Time', 'FREQ=WEEKLY;BYDAY=MO;COUNT=4', eastern),
    want: ['2024-03-04T14:00:00.000Z', '2024-03-11T13:00:00.000Z', '2024-03-18T13:00:00.000Z', '2024-03-25T13:00:00.000Z'],
  },
  {
    name: 'daily 02:30 leaves out the skipped hour',
    event: series('2024-03-09T07:30:00Z', 'Eastern Standard Time', 'FREQ=DAILY;COUNT=4', eastern),
    want: ['2024-03-09T07:30:00.000Z', '2024-03-11T06:30:00.000Z', '2024-03-12T06:30:00.000Z', '2024-03-13T06:30:00.000Z'],
  },
  {
    name: 'a repeated 01:30 is its first occurrence',
    event: series('2024-11-02T05:30:00Z', 'Eastern Standard Time', 'FREQ=DAILY;COUNT=3', eastern),
    want: ['2024-11-02T05:30:00.000Z', '2024-11-03T05:30:00.000Z', '2024-11-04T06:30:00.000Z'],
  },
  {
    // Before the listing's first entry the first entry's offset holds, and
    // after its last the last's.
    name: 'instances outside the listing take its nearest entry',
    event: series('2024-02-19T14:00:00Z', 'Eastern Standard Time', 'FREQ=MONTHLY;COUNT=12', eastern),
    want: [
      '2024-02-19T14:00:00.000Z', '2024-03-19T13:00:00.000Z', '2024-04-19T13:00:00.000Z', '2024-05-19T13:00:00.000Z',
      '2024-06-19T13:00:00.000Z', '2024-07-19T13:00:00.000Z', '2024-08-19T13:00:00.000Z', '2024-09-19T13:00:00.000Z',
      '2024-10-19T13:00:00.000Z', '2024-11-19T14:00:00.000Z', '2024-12-19T14:00:00.000Z', '2025-01-19T14:00:00.000Z',
    ],
  },
  {
    // The server reads the object's definition of a zone even where the
    // browser has one of that name, so the listing wins: this object's
    // America/New_York never changes its offset.
    name: 'a listing for a zone Intl knows',
    event: series('2024-03-04T14:00:00Z', 'America/New_York', 'FREQ=WEEKLY;COUNT=3', {
      'America/New_York': [{ from: '2024-01-01T00:00:00Z', offset: -18000 }],
    }),
    want: ['2024-03-04T14:00:00.000Z', '2024-03-11T14:00:00.000Z', '2024-03-18T14:00:00.000Z'],
  },
  {
    // A listing for another zone, an empty one, or a name inherited from
    // Object.prototype is no listing for the event's zone.
    name: 'no listing for the zone falls back to UTC for a name Intl does not know',
    event: series('2024-03-04T14:00:00Z', 'Eastern Standard Time', 'FREQ=WEEKLY;COUNT=3', { 'Pacific Standard Time': eastern['Eastern Standard Time'] }),
    want: ['2024-03-04T14:00:00.000Z', '2024-03-11T14:00:00.000Z', '2024-03-18T14:00:00.000Z'],
  },
  {
    name: 'an empty listing',
    event: series('2024-03-04T14:00:00Z', 'Eastern Standard Time', 'FREQ=WEEKLY;COUNT=3', { 'Eastern Standard Time': [] }),
    want: ['2024-03-04T14:00:00.000Z', '2024-03-11T14:00:00.000Z', '2024-03-18T14:00:00.000Z'],
  },
  {
    name: 'a zone named for an Object.prototype property',
    event: series('2024-03-04T14:00:00Z', 'constructor', 'FREQ=WEEKLY;COUNT=3', {}),
    want: ['2024-03-04T14:00:00.000Z', '2024-03-11T14:00:00.000Z', '2024-03-18T14:00:00.000Z'],
  },
];
const listedRange = [new Date(Date.UTC(2024, 0, 1)), new Date(Date.UTC(2025, 1, 1))];
for (const { name, event, want } of listedCases) {
  const got = Array.from(context._expand([event], listedRange[0], listedRange[1]), (occurrence) => occurrence.dtstart.toISOString());
  assert.deepEqual(got, want, `${name} viewed in ${zone}`);
}
