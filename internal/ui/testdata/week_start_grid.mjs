// Lays out a view's month grid, and for a calendar view its week and month
// ranges, from every first day of the week, and checks each starts on that
// day: the headers rotate, the first column holds that weekday, and every row
// is a full week running in date order.
//
// Usage: node week_start_grid.mjs <view template> <month render function> <week start partial>
import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';

const [templatePath, renderName, partialPath] = process.argv.slice(2);
const source = fs.readFileSync(templatePath, 'utf8');
const partial = fs.readFileSync(partialPath, 'utf8')
  .replace('{{define "week_start_js"}}', '').replace('{{end}}', '');
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
  return {style: {}, children: [], appendChild(el) { this.children.push(el); }};
}

const names = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
const isCalendarView = source.includes('function visibleRange(');
const helpers = isCalendarView ? ['startOfDay', 'addDays', 'startOfWeek', 'visibleRange'] : [];
const script = partial + '\n' + helpers.map(extract).join('\n') + '\n' + extract(renderName) + '\n' + renderName + '();';

// February 2026 begins on a Sunday and fills four rows exactly; the others
// begin on each remaining weekday.
const months = [[2026, 1], [2026, 5], [2026, 8], [2026, 3], [2026, 0], [2025, 7], [2026, 7]];
for (let weekStart = 0; weekStart < 7; weekStart++) {
  for (const [year, month] of months) {
    const label = `week start ${names[weekStart]}, ${year}-${month + 1}`;
    const elements = {};
    const cells = [];
    const currentDate = new Date(year, month, 15);
    const context = {
      Date, WEEK_START: weekStart, currentDate, currentView: 'month', currentListMonth: currentDate,
      baseEvents: [], expandedEvents: [], expandRecurringEvents() { return []; }, isEventVisible() { return true; }, renderUpcoming() {},
      createDayCell(grid, day, y, m, otherMonth) { cells.push({date: new Date(y, m, day), otherMonth}); },
      document: {getElementById(id) { return elements[id] ||= element(); }, createElement: element},
    };
    vm.runInNewContext(script, context);

    const headers = elements['calendar-grid'].children.map((el) => el.textContent);
    assert.deepEqual(headers, [...names.slice(weekStart), ...names.slice(0, weekStart)], `${label}: headers`);
    assert.equal(cells.length % 7, 0, `${label}: rows are full weeks`);
    assert.equal(cells[0].date.getDay(), weekStart, `${label}: first column`);
    cells.forEach((cell, i) => {
      assert.equal(cell.date.getDay(), names.indexOf(headers[i % 7]), `${label}: cell ${i} sits under its weekday`);
      if (i > 0) {
        const expected = new Date(cells[i - 1].date);
        expected.setDate(expected.getDate() + 1);
        assert.equal(cell.date.getTime(), expected.getTime(), `${label}: cell ${i} follows the one before`);
      }
      assert.equal(cell.otherMonth, cell.date.getMonth() !== month, `${label}: cell ${i} month flag`);
    });
    const inMonth = cells.filter((cell) => !cell.otherMonth);
    assert.equal(inMonth.length, new Date(year, month + 1, 0).getDate(), `${label}: every day of the month`);
    assert.ok(cells.findIndex((cell) => !cell.otherMonth) < 7, `${label}: no leading row of other days`);
    assert.ok(cells.slice(-7).some((cell) => !cell.otherMonth), `${label}: no trailing row of other days`);

    if (isCalendarView) {
      const range = vm.runInNewContext('visibleRange()', context);
      assert.equal(range.start.getTime(), cells[0].date.getTime(), `${label}: month range starts at the first cell`);

      context.currentView = 'week';
      for (let offset = 0; offset < 7; offset++) {
        context.currentDate = new Date(year, month, 10 + offset);
        const week = vm.runInNewContext('visibleRange()', context);
        assert.equal(week.start.getDay(), weekStart, `${label}: week of day ${10 + offset} starts on the first day`);
        assert.ok(week.start <= context.currentDate && context.currentDate < week.end, `${label}: week of day ${10 + offset} holds it`);
      }
    }
  }
}
