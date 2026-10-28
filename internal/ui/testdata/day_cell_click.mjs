// Clicks each part of a month grid day cell that has events and checks where
// the click goes: a cell the user can add to is selected, showing its view and
// add buttons, wherever it is clicked, while a read-only cell's date still
// opens the day's events.
//
// Usage: node day_cell_click.mjs <view template>
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

function element() {
  return {
    style: {}, dataset: {}, children: [], listeners: {}, parent: null,
    appendChild(el) { el.parent = this; this.children.push(el); },
    addEventListener(type, fn) { (this.listeners[type] ||= []).push(fn); },
    setAttribute() {},
  };
}
function click(el) {
  let stopped = false;
  const event = {stopPropagation() { stopped = true; }};
  for (let node = el; node && !stopped; node = node.parent) {
    for (const fn of node.listeners.click || []) fn(event);
  }
}

const day = new Date(2026, 9, 15);
const events = [{dtstart: day, dtend: new Date(2026, 9, 15, 1), summary: 'Standup'}];
for (const writable of [true, false]) {
  const calls = [];
  const grid = element();
  const context = {
    Date, canCreate: writable, writableCalendars() { return writable ? [{}] : []; },
    expandedEvents: events, EYE_ICON_SVG: '', PLUS_ICON_SVG: '',
    selectDayCell() { calls.push('select'); }, showDayModal() { calls.push('day list'); },
    clearDayCellSelection() {}, showEventModal() {}, openCreateEditor() {}, setView() {}, addDays() {},
    document: {createElement: element},
    grid, events,
  };
  vm.runInNewContext(extract('createDayCell') + '\ncreateDayCell(grid, 15, 2026, 9, false, false, events);', context);
  const cell = grid.children[0];
  const number = cell.children.find((el) => el.className.split(' ').includes('day-number'));
  const list = cell.children.find((el) => el.className === 'day-events');
  for (const [part, el] of [['cell', cell], ['date', number], ['event list', list]]) {
    calls.length = 0;
    click(el);
    const want = writable ? ['select'] : part === 'date' ? ['day list'] : [];
    assert.deepEqual(calls, want, `${writable ? 'writable' : 'read-only'} cell: clicking its ${part}`);
  }
}
