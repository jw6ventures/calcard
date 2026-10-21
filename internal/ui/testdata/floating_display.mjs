// Runs a calendar view's own reading of the events JSON its endpoint served
// for a floating series and its overrides, and its time grid, and checks that
// each is placed and labelled at its wall clock whatever zone the browser is in
// (run under several TZ values): floating times are not instants.
//
// Usage: node floating_display.mjs <view as rendered> <events JSON>
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

const context = {Date, Math, console};
vm.runInNewContext([
  'function parseICAL() { return {}; }',
  ...['parseICALDate', 'isFloatingTime', 'recurrenceIsFloating', 'parseServerDate', 'normalizeServerEvent', 'overrideRecurrenceIdAllDay', 'expandRecurringEvents', 'formatDate'].map(extract),
  'globalThis._normalize = normalizeServerEvent; globalThis._expand = expandRecurringEvents; globalThis._formatDate = formatDate;',
].join('\n'), Object.assign(context, {globalThis: context}));

const served = JSON.parse(fs.readFileSync(process.argv[3], 'utf8'));
const events = served.map(context._normalize);
const occurrences = context._expand(events);
const wall = (date) => [date.getFullYear(), date.getMonth() + 1, date.getDate(), date.getHours(), date.getMinutes()];
const zone = Intl.DateTimeFormat().resolvedOptions().timeZone;

// The series is a daily 09:00 to 10:00 from 4 March 2024, in no zone; the 5th
// is excluded and the 6th moved to 11:00 to 12:00.
const byDay = new Map(occurrences.filter((o) => o.uid === 'floating').map((o) => [o.dtstart.getDate(), o]));
assert.deepEqual([...byDay.keys()].sort((a, b) => a - b), [4, 6, 7, 8, 9, 10, 11], `instances in ${zone}`);
for (const [day, o] of byDay) {
  const [startHour, endHour] = day === 6 ? [11, 12] : [9, 10];
  assert.deepEqual(wall(o.dtstart), [2024, 3, day, startHour, 0], `start of the ${day}th in ${zone}`);
  assert.deepEqual(wall(o.dtend), [2024, 3, day, endHour, 0], `end of the ${day}th in ${zone}`);
  assert.deepEqual(wall(o.recurrenceId), [2024, 3, day, 9, 0], `RECURRENCE-ID of the ${day}th in ${zone}`);
  // The popover's "When" text, and the list and month views' times, are this
  // Date's own, so they say what the editor does.
  assert.equal(context._formatDate(o.dtstart, false), new Date(2024, 2, day, startHour, 0).toLocaleDateString(undefined, {
    weekday: 'short', year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
  }), `popover text of the ${day}th in ${zone}`);
}
const series = events.find((e) => e.uid === 'floating');
assert.deepEqual(series.exdates.map(wall), [[2024, 3, 5, 9, 0]], `EXDATE in ${zone}`);
assert.deepEqual(wall(series.dtstart), [2024, 3, 4, 9, 0], `series start in ${zone}`);

// A UTC series and an all-day series, beside it, are not moved by it.
const utc = events.find((e) => e.uid === 'utc');
assert.equal(utc.dtstart.toISOString(), '2024-03-04T09:00:00.000Z', 'a UTC series is the instant it names');

// The day and week grids put each instance at its wall clock.
function element() {
  return {style: {setProperty() {}}, dataset: {}, children: [], appendChild(el) { this.children.push(el); }, addEventListener() {}, classList: {contains() { return false; }}};
}
for (const currentView of ['day', 'week']) {
  for (const [day, o] of byDay) {
    const elements = {};
    const grid = {
      Date, Math, currentView, currentDate: new Date(2024, 2, day), canCreate: false, CAL_COLOR: '#123456',
      baseEvents: [o], expandRecurringEvents(list) { return list; }, isEventVisible() { return true; }, writableCalendars() { return []; },
      document: {getElementById(id) { return elements[id] ||= element(); }, createElement: element},
    };
    vm.runInNewContext(['startOfDay', 'startOfWeek', 'addDays', 'eventIntersectsDay', 'renderTimeGridView'].map(extract).join('\n') + '\nrenderTimeGridView();', grid);
    const placed = elements['time-grid-body'].children.flatMap((el) => el.children).find((el) => el.className === 'timed-event');
    assert.ok(placed, `${currentView}: the ${day}th is missing in ${zone}`);
    const minute = (day === 6 ? 11 : 9) * 60;
    assert.ok(Math.abs(parseFloat(placed.style.top) - minute / 30 * 28) < 0.001, `${currentView}: the ${day}th is at ${placed.style.top} in ${zone}`);
    assert.ok(Math.abs(parseFloat(placed.style.height) - 55) < 0.001, `${currentView}: the ${day}th is ${placed.style.height} tall in ${zone}`);
  }
}
