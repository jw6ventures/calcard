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
  return {style:{setProperty(){}}, dataset:{}, children:[], appendChild(el){this.children.push(el)}, addEventListener(){}, classList:{contains(){return false}}};
}
const cases = [
  ['spring noon', '2026-03-08T12:00:00', '2026-03-08T13:00:00', 720, 55],
  ['fall noon', '2026-11-01T12:00:00', '2026-11-01T13:00:00', 720, 55],
  ['spring gap', '2026-03-08T01:30:00-05:00', '2026-03-08T03:30:00-04:00', 90, 111],
  ['first repeated hour', '2026-11-01T01:30:00-04:00', '2026-11-01T01:45:00-04:00', 90, 20],
  ['second repeated hour', '2026-11-01T01:30:00-05:00', '2026-11-01T01:45:00-05:00', 90, 20],
  ['backward jump', '2026-11-01T01:45:00-04:00', '2026-11-01T01:15:00-05:00', 105, 20],
  ['spring midnight', '2026-03-08T23:00:00', '2026-03-09T00:00:00', 1380, 55],
  ['fall midnight', '2026-11-01T23:00:00', '2026-11-02T00:00:00', 1380, 55],
  ['next day clip', '2026-11-01T23:00:00', '2026-11-02T02:00:00', 1380, 55],
  ['last five minutes', '2026-11-01T23:55:00', '2026-11-02T00:00:00', 1435, 5 / 30 * 28],
];
for (const currentView of ['day', 'week']) {
  for (const [name, startText, endText, minute, height] of cases) {
    const elements = {};
    const start = new Date(startText);
    const context = {
      Date, Math, currentView, currentDate:start, canCreate:false, CAL_COLOR:'#123456',
      baseEvents:[{dtstart:start,dtend:new Date(endText),summary:name}],
      expandRecurringEvents(events){return events}, isEventVisible(){return true}, writableCalendars(){return []},
      document:{getElementById(id){return elements[id] ||= element()}, createElement:element},
    };
    vm.runInNewContext(['startOfDay','startOfWeek','addDays','eventIntersectsDay','renderTimeGridView'].map(extract).join('\n')+'\nrenderTimeGridView();', context);
    const event = elements['time-grid-body'].children.flatMap(el=>el.children).find(el=>el.className==='timed-event');
    assert.ok(event, `${currentView}: ${name} is missing`);
    assert.ok(Math.abs(parseFloat(event.style.top) - minute / 30 * 28) < 0.001, `${currentView}: ${name} top=${event.style.top}`);
    assert.ok(Math.abs(parseFloat(event.style.height) - height) < 0.001, `${currentView}: ${name} height=${event.style.height}`);
  }
}
