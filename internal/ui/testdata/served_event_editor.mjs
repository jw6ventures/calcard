// Opens a calendar view's own editor on an event of the events JSON its
// endpoint served and prints, as JSON, what the editor would submit for it.
//
// Usage: node served_event_editor.mjs <view as rendered> <events JSON> <uid> [<occurrence start, RFC 3339>]
//
// The occurrence start names a floating occurrence by its wall clock read as UTC.
// Without an occurrence start the editor is opened on the event itself, the
// series; with one, on the occurrence of the series starting then. It prints
// {values, zone, recurrenceZone, recurrenceFloating, deleteRecurrenceFloating, placeholder, deleteZone, deleteRecurrenceZone}:
// the start, end and RECURRENCE-ID values, the time zone fields and floating
// markers of the edit and delete forms, and the hint the zone field shows when it is blank.
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
const shared = ['parseICALDate', 'isFloatingTime', 'listedOffsets', 'intlOffsets', 'timeZoneClock', 'editorTimeZone', 'editorRecurrenceTimeZone', 'recurrenceIsFloating', 'editorRecurrenceFloating', 'editorClock', 'editorRecurrenceClock',
    'overrideRecurrenceIdAllDay', 'expandRecurringEvents', 'parseServerDate', 'normalizeServerEvent', 'formatDateTimeForInput',
    'browserTimeZone', 'showPartialRecurrence', 'refreshPartialRecurrence'];
const names = shared.concat(aggregate
    ? ['formatLocalDate', 'formatDateInput', 'formatRecurrenceInput', 'setEditorDateValues', 'inclusiveAllDayEnd', 'startOfDay', 'addDays', 'openEditEditor']
    : ['formatDateForInput', 'formatRecurrenceIdForInput', 'showEditEventModal', 'setEditScope', 'toggleAllDay']);
const elements = {};
function element() {
    return {value: '', style: {}, classList: {add() {}, remove() {}}, focus() {}, setAttribute() {}};
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
    function resetEditorFields() {}
    function populateEditorCalendars() {}
    function addReminderRow() {}
    function setReminderList() {}
    function toggleRecurrenceOptions() {}
    function toggleRecurrenceEnd() {}
    function currentReturnPath() { return '/calendars'; }
    function parseICAL() { return {}; }
    function selectStoredValue(select, value) { select.value = value || ''; }
    window.selectStoredValue = selectStoredValue;
    window.showPartialRecurrence = showPartialRecurrence;
    window.refreshPartialRecurrence = refreshPartialRecurrence;
    window.formatDateTimeForInput = formatDateTimeForInput;
    window.timeZoneClock = timeZoneClock;
    window.editorTimeZone = editorTimeZone;
    window.editorRecurrenceTimeZone = editorRecurrenceTimeZone;
    window.editorRecurrenceFloating = editorRecurrenceFloating;
    window.editorClock = editorClock;
    window.editorRecurrenceClock = editorRecurrenceClock;
    window.browserTimeZone = browserTimeZone;
`;
const view = new Function('document', stubs + source + '\nreturn {' +
    'open: ' + (aggregate ? 'openEditEditor' : 'showEditEventModal') + ',' +
    'normalizeServerEvent: normalizeServerEvent, expandRecurringEvents: expandRecurringEvents};')(document);
const prefix = aggregate ? 'event' : 'edit';
const read = suffix => document.getElementById(prefix + suffix).value;

const served = JSON.parse(fs.readFileSync(process.argv[3], 'utf8'));
const events = served.map(view.normalizeServerEvent);
let event = events.find(e => e.uid === process.argv[4]);
if (!event) throw new Error('no event ' + process.argv[4]);
if (process.argv[5]) {
    // A floating occurrence is named by its wall clock read as UTC, as the server sends it.
    const pad = n => String(n).padStart(2, '0');
    const wallOf = d => d.floatingWall || (d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate()) + 'T' + pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds()));
    const named = o => o.floating
        ? new Date(wallOf(o.dtstart).padEnd(19, ':00') + 'Z').toISOString()
        : o.dtstart.toISOString();
    event = view.expandRecurringEvents(events).find(o => o.uid === event.uid && named(o) === process.argv[5]);
    if (!event) throw new Error('no occurrence at ' + process.argv[5]);
}
// The aggregate view opens only events it may edit.
event.canEdit = true;
event.canDelete = true;
view.open(event);
console.log(JSON.stringify({
    values: [read('-dtstart'), read('-dtend'), read('-recurrence-id')],
    zone: read('-timezone'),
    recurrenceZone: read('-recurrence-timezone'),
    recurrenceFloating: read('-recurrence-floating'),
    deleteRecurrenceFloating: document.getElementById('delete-recurrence-floating').value,
    placeholder: document.getElementById(prefix + '-timezone').placeholder,
    deleteZone: document.getElementById('delete-timezone').value,
    deleteRecurrenceZone: document.getElementById('delete-recurrence-timezone').value,
}));
