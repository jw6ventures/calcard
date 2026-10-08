// Runs a calendar view's own create and edit forms and prints, as JSON, what
// each would submit; templates_test.go reads the values back the way the
// server does.
//
// Usage: node timed_editor_roundtrip.mjs <view as rendered> [<events JSON>]
//
// Without an events JSON it opens the editor on a set of timed events and
// prints {cases, create}. Given the events JSON the view's endpoint served for
// March 2024, it prints {instances, summary, summaryWant, forms}: every
// occurrence the view's expansion places in that month as a free-busy period,
// the series' recurrence summary, and the editor's values for the
// "outlook-standup" series and two of its occurrences.
import fs from 'node:fs';
const html = fs.readFileSync(process.argv[2], 'utf8');
function extract(name) {
    const start = html.indexOf('function ' + name + '(');
    if (start < 0) throw new Error('missing ' + name);
    let depth = 0, i = html.indexOf('{', start);
    for (; i < html.length; i++) {
        if (html[i] === '{') depth++;
        else if (html[i] === '}' && --depth === 0) break;
    }
    return html.slice(start, i + 1);
}
const aggregate = process.argv[2].includes('all_calendars');
const shared = ['parseICALDate', 'listedOffsets', 'intlOffsets', 'timeZoneClock', 'rruleUntilDay', 'rruleUntilLimit',
    'makeOccurrenceKey', 'recurrenceZoneClock', 'recurrenceCalendar', 'expandRecurringEvents',
    'parseServerDate', 'normalizeServerEvent', 'formatRRule', 'formatDateTimeForInput', 'browserTimeZone'];
const names = shared.concat(aggregate
    ? ['formatLocalDate', 'formatDateInput', 'formatRecurrenceInput', 'setEditorDateValues', 'inclusiveAllDayEnd', 'startOfDay', 'addDays', 'rruleForEvent', 'openEditEditor', 'openCreateEditor']
    : ['formatDateForInput', 'formatRecurrenceIdForInput', 'showEditEventModal', 'showCreateEventModal', 'setEditScope', 'toggleAllDay']);
const elements = {};
function element() {
    return {value: '', style: {}, classList: {add() {}, remove() {}}, focus() {}};
}
const document = {
    getElementById(id) { return elements[id] ||= element(); },
    querySelectorAll() { return []; },
    querySelector() { return element(); }
};
const source = names.map(extract).join('\n').replace('{{.Calendar.ID}}', '1');
const stubs = `
    var window = {}, currentEditorEvent, editorForm = {}, modal = document.getElementById('modal'), editorModal = modal;
    var editorCalendar = {value: '1'};
    function writableCalendars() { return [{id: 1, canCreate: true}]; }
    function resetEditorFields() { document.getElementById('event-timezone').value = ''; }
    function populateEditorCalendars() {}
    function addReminderRow() {}
    function setReminderList() {}
    function toggleRecurrenceOptions() {}
    function toggleRecurrenceEnd() {}
    function currentReturnPath() { return '/calendars'; }
    function parseICAL() { return {}; }
    window.rruleUntilDay = rruleUntilDay;
    window.formatDateTimeForInput = formatDateTimeForInput;
    window.timeZoneClock = timeZoneClock;
    window.browserTimeZone = browserTimeZone;
`;
const view = new Function('document', stubs + source + '\nreturn {' +
    'open: ' + (aggregate ? 'openEditEditor' : 'showEditEventModal') + ',' +
    'create: ' + (aggregate ? 'openCreateEditor' : 'function(start, end) { showCreateEventModal({start: start, end: end, allDay: false}); }') + ',' +
    'normalizeServerEvent: normalizeServerEvent, expandRecurringEvents: expandRecurringEvents, formatRRule: formatRRule};')(document);
const prefix = aggregate ? 'event' : 'edit';
const read = suffix => document.getElementById(prefix + suffix).value;

// What the editor holds once it has been opened on event.
function editorValues(event) {
    view.open(event);
    const recurrenceID = read('-recurrence-id');
    const recurrenceZone = read('-recurrence-timezone');
    if (document.getElementById('delete-recurrence-id').value !== recurrenceID ||
        document.getElementById('delete-recurrence-timezone').value !== recurrenceZone) {
        throw new Error('delete form changed the occurrence identity');
    }
    return {values: [read('-dtstart'), read('-dtend'), recurrenceID], zone: read('-timezone'), recurrenceZone,
            until: read('-recurrence-until')};
}

if (process.argv[3]) {
    const served = JSON.parse(fs.readFileSync(process.argv[3], 'utf8'));
    const events = served.map(view.normalizeServerEvent);
    const occurrences = view.expandRecurringEvents(events, new Date('2024-03-01T00:00:00Z'), new Date('2024-04-01T00:00:00Z'));
    const stamp = date => date.toISOString().replace(/[-:]|\.\d{3}/g, '');
    const series = events.find(event => event.uid === 'outlook-standup');
    const occurrenceAt = (instant, uid = 'outlook-standup') => {
        const found = occurrences.find(o => o.uid === uid && o.dtstart.toISOString() === instant);
        if (!found) throw new Error('no occurrence of ' + uid + ' at ' + instant);
        return found;
    };
    console.log(JSON.stringify({
        instances: occurrences.map(o => stamp(o.dtstart) + '/' + stamp(o.dtend)).sort(),
        summary: view.formatRRule(series),
        summaryWant: 'Weekly on Monday, until ' + new Date(2024, 2, 25).toLocaleDateString(),
        forms: {
            series: editorValues(series),
            override: editorValues(occurrenceAt('2024-03-11T15:00:00.000Z')),
            occurrence: editorValues(occurrenceAt('2024-03-18T13:00:00.000Z')),
            decades: editorValues(events.find(event => event.uid === 'outlook-decades')),
            decadesOccurrence: editorValues(occurrenceAt('2024-03-05T20:00:00.000Z', 'outlook-decades')),
        },
    }));
    process.exit(0);
}

function format(start, end, zone, until) {
    const rrule = 'FREQ=WEEKLY;UNTIL=' + until;
    return editorValues({uid: 'timed', calendarId: 1, canEdit: true, canDelete: true, isOccurrence: true,
        dtstart: start, dtend: end, timezone: zone, recurrenceId: start, recurrenceTimezone: zone || 'UTC',
        rrule, rruleParsed: {FREQ: 'WEEKLY', UNTIL: until}});
}
// Each UNTIL is the one the server writes for a series ending on 31 July 2025:
// that day's last second in the event's zone, or in UTC for an event with no
// zone or one the server does not know.
const cases = [
    ['2025-07-24T09:00:00-04:00', '2025-07-24T10:00:00-04:00', 'America/New_York', '20250801T035959Z'],
    ['2025-03-09T01:30:00-05:00', '2025-03-09T03:30:00-04:00', 'America/New_York', '20250801T035959Z'],
    ['2025-07-24T09:00:00+05:30', '2025-07-24T10:00:00+05:30', 'Asia/Kolkata', '20250731T182959Z'],
    ['2025-07-24T09:00:00Z', '2025-07-24T10:00:00Z', 'UTC', '20250731T235959Z'],
    ['2025-07-24T09:00:15Z', '2025-07-24T10:00:25Z', '', '20250731T235959Z'],
    ['2025-07-24T13:00:00Z', '2025-07-24T14:00:00Z', 'Eastern Standard Time', '20250731T235959Z'],
    ['2025-07-24T23:30:00Z', '2025-07-25T00:30:00Z', 'Not/AZone', '20250731T235959Z'],
];

// The create form prefills a slot picked on the browser's grid, so its times
// are the browser's wall clock and the zone it submits has to be the browser's.
// The aggregate view's create and edit forms are one form, which an edit
// leaves holding the event's zone.
const edited = cases.map(([start, end, zone, until]) => ({start, end, rruleUntil: until, ...format(new Date(start), new Date(end), zone, until)}));
const slot = [new Date('2025-07-24T14:00:00Z'), new Date('2025-07-24T14:30:00Z')];
view.create(slot[0], slot[1], false);
const createPrefix = aggregate ? 'event' : 'create';
const created = suffix => document.getElementById(createPrefix + suffix).value;

console.log(JSON.stringify({
    cases: edited,
    create: {start: slot[0].toISOString(), end: slot[1].toISOString(), values: [created('-dtstart'), created('-dtend')], zone: created('-timezone')},
}));
