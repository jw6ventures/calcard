// Runs a calendar view's own recurrence expansion over series bounded by a
// COUNT. RFC 5545 section 3.3.10 makes COUNT bound the instances the RRULE
// generates, and section 3.8.5.1 removes an EXDATE's instance from that set
// afterwards, so FREQ=DAILY;COUNT=5 with one EXDATE is four instances over five
// days, as the server expands it. An instance on a local time its zone skips
// is never generated and does not count. An overridden instance is still the
// one its slot generated, so it counts once. The script runs under several TZ
// values and expects the same instants in each.
//
// Usage: node recurrence_count.mjs <calendar_view.html as rendered>
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

function series({ dtstart, allDay = false, timezone, rrule, exdates = [], overrides = [] }) {
  const rruleParsed = {};
  rrule.split(';').forEach((part) => {
    const [key, value] = part.split('=');
    rruleParsed[key] = value;
  });
  return { uid: 'series', dtstart, dtend: null, allDay, timezone, rrule, rruleParsed, exdates, overrides };
}

function expand(event) {
  // Array.from builds the list in this realm, which deepEqual compares by prototype.
  return Array.from(context._expand([event], range[0], range[1]));
}

const starts = (event) => expand(event).map((occurrence) => occurrence.dtstart.toISOString());
const days = (event) => expand(event).map(({ dtstart }) => [dtstart.getFullYear(), dtstart.getMonth() + 1, dtstart.getDate()]);

const cases = [
  {
    name: 'daily COUNT=5 with one EXDATE in Europe/Berlin',
    event: series({
      dtstart: new Date('2026-06-01T07:00:00Z'), timezone: 'Europe/Berlin', rrule: 'FREQ=DAILY;COUNT=5',
      exdates: [new Date('2026-06-03T07:00:00Z')],
    }),
    want: ['2026-06-01T07:00:00.000Z', '2026-06-02T07:00:00.000Z', '2026-06-04T07:00:00.000Z', '2026-06-05T07:00:00.000Z'],
  },
  {
    name: 'daily COUNT=5 with one EXDATE and no TZID',
    event: series({
      dtstart: new Date('2026-06-01T09:00:00Z'), rrule: 'FREQ=DAILY;COUNT=5',
      exdates: [new Date('2026-06-03T09:00:00Z')],
    }),
    want: ['2026-06-01T09:00:00.000Z', '2026-06-02T09:00:00.000Z', '2026-06-04T09:00:00.000Z', '2026-06-05T09:00:00.000Z'],
  },
  {
    name: 'an EXDATE on the DTSTART',
    event: series({
      dtstart: new Date('2026-06-01T09:00:00Z'), rrule: 'FREQ=DAILY;COUNT=3',
      exdates: [new Date('2026-06-01T09:00:00Z')],
    }),
    want: ['2026-06-02T09:00:00.000Z', '2026-06-03T09:00:00.000Z'],
  },
  {
    // 29 March 2026 has no 02:30 in Berlin, so it is no instance and does not
    // count, while 22 March is an instance the EXDATE then removes.
    name: 'a skipped local time and an EXDATE in one Berlin series',
    event: series({
      dtstart: new Date('2026-03-15T01:30:00Z'), timezone: 'Europe/Berlin', rrule: 'FREQ=WEEKLY;COUNT=4',
      exdates: [new Date('2026-03-22T01:30:00Z')],
    }),
    want: ['2026-03-15T01:30:00.000Z', '2026-04-05T00:30:00.000Z', '2026-04-12T00:30:00.000Z'],
  },
  {
    name: 'an overridden instance counts once',
    event: series({
      dtstart: new Date('2026-06-01T09:00:00Z'), rrule: 'FREQ=DAILY;COUNT=3',
      overrides: [{ recurrenceId: new Date('2026-06-02T09:00:00Z'), dtstart: new Date('2026-06-02T12:00:00Z') }],
    }),
    want: ['2026-06-01T09:00:00.000Z', '2026-06-02T12:00:00.000Z', '2026-06-03T09:00:00.000Z'],
  },
];
for (const { name, event, want } of cases) {
  assert.deepEqual(starts(event), want, `${name} viewed in ${zone}`);
}

// Every occurrence, overridden or not, carries the series it came from, which
// the aggregate editor opens to edit or move the whole series.
for (const { name, event } of cases) {
  for (const occurrence of expand(event)) {
    assert.equal(occurrence.seriesEvent, event, `${name}: an occurrence on ${occurrence.dtstart.toISOString()} lost its series`);
  }
}

// An all-day series names its EXDATE by date.
assert.deepEqual(
  days(series({
    dtstart: new Date(2026, 5, 1), allDay: true, rrule: 'FREQ=DAILY;COUNT=5',
    exdates: [new Date(2026, 5, 3)],
  })),
  [[2026, 6, 1], [2026, 6, 2], [2026, 6, 4], [2026, 6, 5]],
  `all-day daily COUNT=5 with one EXDATE viewed in ${zone}`,
);
