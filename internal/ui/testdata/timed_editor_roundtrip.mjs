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
const names = aggregate
    ? ['formatLocalDate', 'formatDateInput', 'formatDateTimeInput', 'formatRecurrenceInput', 'setEditorDateValues', 'inclusiveAllDayEnd', 'startOfDay', 'addDays', 'rruleForEvent', 'openEditEditor']
    : ['formatDateForInput', 'formatDateTimeForInput', 'formatRecurrenceIdForInput', 'showEditEventModal', 'setEditScope', 'toggleAllDay'];
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
    function resetEditorFields() {}
    function populateEditorCalendars() {}
    function addReminderRow() {}
    function setReminderList() {}
    function toggleRecurrenceOptions() {}
    function toggleRecurrenceEnd() {}
    function currentReturnPath() { return '/calendars'; }
`;
const open = new Function('document', stubs + source + '\nreturn ' + (aggregate ? 'openEditEditor' : 'showEditEventModal') + ';')(document);
function format(start, end, zone) {
    open({uid: 'timed', calendarId: 1, canEdit: true, canDelete: true, isOccurrence: true,
          dtstart: start, dtend: end, timezone: zone, recurrenceId: start, recurrenceTimezone: zone || 'UTC'});
    const prefix = aggregate ? 'event' : 'edit';
    const read = suffix => document.getElementById(prefix + suffix).value;
    const recurrenceID = read('-recurrence-id');
    const recurrenceZone = read('-recurrence-timezone');
    if (document.getElementById('delete-recurrence-id').value !== recurrenceID ||
        document.getElementById('delete-recurrence-timezone').value !== recurrenceZone) {
        throw new Error('delete form changed the occurrence identity');
    }
    return {values: [read('-dtstart'), read('-dtend'), recurrenceID], zone: read('-timezone'), recurrenceZone};
}
const cases = [
    ['2025-07-24T09:00:00-04:00', '2025-07-24T10:00:00-04:00', 'America/New_York'],
    ['2025-03-09T01:30:00-05:00', '2025-03-09T03:30:00-04:00', 'America/New_York'],
    ['2025-07-24T09:00:00+05:30', '2025-07-24T10:00:00+05:30', 'Asia/Kolkata'],
    ['2025-07-24T09:00:00Z', '2025-07-24T10:00:00Z', 'UTC'],
    ['2025-07-24T09:00:15Z', '2025-07-24T10:00:25Z', ''],
];
console.log(JSON.stringify(cases.map(([start, end, zone]) => ({start, end, ...format(new Date(start), new Date(end), zone)}))));
