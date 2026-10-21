// Opens a calendar view's own editor, one after another, on a series carrying
// each of the view models' repeat fields in the given JSON file, and prints,
// as JSON, for each the repeat fields the form would submit, keyed by field
// name as the server reads them, and the note shown beside them, or "" for
// none. Every series' RRULE is "FREQ=TEST", which the note names.
//
// Usage: node recurrence_prefill.mjs <view as rendered> <forms JSON>
import fs from 'node:fs';
const html = fs.readFileSync(process.argv[2], 'utf8');
const forms = JSON.parse(fs.readFileSync(process.argv[3], 'utf8'));
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
const prefix = aggregate ? 'event' : 'edit';
const shared = ['parseICALDate', 'listedOffsets', 'intlOffsets', 'timeZoneClock', 'editorTimeZone', 'editorRecurrenceTimeZone', 'recurrenceIsFloating', 'editorRecurrenceFloating', 'editorClock', 'editorRecurrenceClock', 'isFloatingTime', 'formatDateTimeForInput', 'browserTimeZone',
    'showPartialRecurrence', 'refreshPartialRecurrence'];
const names = shared.concat(aggregate
    ? ['formatLocalDate', 'formatDateInput', 'formatRecurrenceInput', 'setEditorDateValues', 'inclusiveAllDayEnd', 'startOfDay',
       'addDays', 'resetEditorFields', 'openEditEditor']
    : ['formatDateForInput', 'formatRecurrenceIdForInput', 'showEditEventModal', 'setEditScope', 'toggleAllDay']);

// The fields the form submits for the repeat section, by element id, with the
// value each holds when the page loads.
const fields = {
    recurrence: [prefix + '-recurrence', ''],
    recurrence_interval: [prefix + '-recurrence-interval', '1'],
    recurrence_end_type: [prefix + '-recurrence-end', 'never'],
    recurrence_count: [prefix + '-recurrence-count', '10'],
    recurrence_until: [prefix + '-recurrence-until', ''],
    recurrence_bymonthday: [prefix + (aggregate ? '-recurrence-monthday' : '-recurrence-bymonthday'), ''],
    recurrence_bymonth: [prefix + (aggregate ? '-recurrence-month' : '-recurrence-bymonth'), ''],
};
const elements = {};
function element(id) {
    const initial = Object.values(fields).find(([field]) => field === id);
    return {value: initial ? initial[1] : '', style: {}, classList: {add() {}, remove() {}}, focus() {}, setAttribute() {}, hidden: false, textContent: '',
        querySelector() { return {}; }};
}
const weekdays = ['MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'].map(value => ({value, checked: false}));
const document = {
    getElementById(id) { return elements[id] ||= element(id); },
    querySelectorAll(selector) { return selector.includes('byday') || selector.includes('recurrence_byday') ? weekdays : []; },
    querySelector() { return element(''); }
};
const editorForm = {
    reset() {
        for (const [id, initial] of Object.values(fields)) document.getElementById(id).value = initial;
        weekdays.forEach(day => { day.checked = false; });
    }
};
const source = names.map(extract).join('\n').replace('{{.Calendar.ID}}', '1');
const stubs = `
    var window = {}, currentEditorEvent, modal = document.getElementById('modal'), editorModal = modal;
    var editorCalendar = {value: '1'};
    function populateEditorCalendars() {}
    function addReminderRow() {}
    function setReminderList() {}
    function currentReturnPath() { return '/calendars'; }
    function toggleRecurrenceOptions() {}
    function toggleRecurrenceEnd() {}
    function selectStoredValue(select, value) { select.value = value || ''; }
    window.selectStoredValue = selectStoredValue;
    window.formatDateTimeForInput = formatDateTimeForInput;
    window.timeZoneClock = timeZoneClock;
    window.editorTimeZone = editorTimeZone;
    window.editorRecurrenceTimeZone = editorRecurrenceTimeZone;
    window.editorRecurrenceFloating = editorRecurrenceFloating;
    window.editorClock = editorClock;
    window.editorRecurrenceClock = editorRecurrenceClock;
    window.browserTimeZone = browserTimeZone;
    window.showPartialRecurrence = showPartialRecurrence;
    window.refreshPartialRecurrence = refreshPartialRecurrence;
`;
const view = new Function('document', 'editorForm', stubs + source + '\nreturn {open: ' +
    (aggregate ? 'openEditEditor' : 'showEditEventModal') + ', refresh: refreshPartialRecurrence};')(document, editorForm);
const open = view.open;

const filled = forms.map(form => {
    open({uid: 'ruled', calendarId: 1, canEdit: true, canDelete: true, allDay: false, timezone: 'UTC',
        dtstart: new Date('2026-06-15T10:00:00Z'), dtend: new Date('2026-06-15T11:00:00Z'),
        rrule: 'FREQ=TEST', recurrenceForm: form});
    const submitted = {};
    for (const [name, [id]] of Object.entries(fields)) submitted[name] = [document.getElementById(id).value];
    const checked = weekdays.filter(day => day.checked).map(day => day.value);
    if (checked.length) submitted.recurrence_byday = checked;
    const note = document.getElementById(prefix + '-recurrence-partial');
    return {Fields: submitted, Partial: note.hidden ? '' : note.textContent};
});

// The note holds only while the fields still repeat at the rule's frequency
// and the edit is of the series.
const partial = forms.find(form => form.partial);
if (partial) {
    open({uid: 'ruled', calendarId: 1, canEdit: true, canDelete: true, allDay: false, timezone: 'UTC',
        dtstart: new Date('2026-06-15T10:00:00Z'), dtend: new Date('2026-06-15T11:00:00Z'),
        rrule: 'FREQ=TEST', recurrenceForm: partial});
    const note = document.getElementById(prefix + '-recurrence-partial');
    const select = document.getElementById(prefix + '-recurrence');
    const check = (want, what) => { if (note.hidden !== want) throw new Error('the note is ' + (note.hidden ? 'hidden' : 'shown') + ' ' + what); };
    check(false, 'for the series as filled');
    view.refresh(note, select, true);
    check(true, 'for one occurrence');
    select.value = '';
    view.refresh(note, select, false);
    check(true, 'once the series is set not to repeat');
    select.value = 'DAILY';
    view.refresh(note, select, false);
    check(true, 'once the series repeats at another frequency');
    select.value = partial.frequency;
    view.refresh(note, select, false);
    check(false, 'once the frequency is set back');
    open({uid: 'ruled', calendarId: 1, canEdit: true, canDelete: true, allDay: false, timezone: 'UTC',
        dtstart: new Date('2026-06-22T10:00:00Z'), dtend: new Date('2026-06-22T11:00:00Z'),
        recurrenceId: new Date('2026-06-22T10:00:00Z'), recurrenceIdAllDay: false, isOccurrence: true,
        rrule: 'FREQ=TEST', recurrenceForm: partial});
    check(true, 'when the editor opens on one occurrence');
}
// The calendar view hides the fields a frequency does not offer. They submit
// what they were filled with while the frequency is the stored rule's, and
// nothing at another frequency or on the create form.
if (!aggregate) {
    const fieldsOf = () => [{disabled: false}, {disabled: false}];
    const groups = {byday: fieldsOf(), monthly: fieldsOf(), bymonth: fieldsOf()};
    const group = name => ({style: {}, querySelectorAll() { return groups[name]; }});
    const select = {value: '', attributes: {}, getAttribute(name) { return name in this.attributes ? this.attributes[name] : null; }};
    const details = new Function('document', extract('updateRecurrenceDetails') + '\nreturn updateRecurrenceDetails;')({
        getElementById(id) {
            if (id.endsWith('-recurrence')) return select;
            const name = id.replace(/^(edit|create)-recurrence-/, '').replace(/-group$/, '');
            return group(name);
        },
    });
    const submitted = () => Object.fromEntries(Object.entries(groups).map(([name, fields]) => [name, !fields[0].disabled]));
    const expect = (want, what) => {
        const got = submitted();
        if (JSON.stringify(got) !== JSON.stringify(want)) throw new Error(what + ': submitted ' + JSON.stringify(got));
    };
    select.attributes['data-filled-frequency'] = 'MONTHLY';
    select.value = 'MONTHLY';
    details('edit');
    expect({byday: true, monthly: true, bymonth: true}, 'the stored frequency');
    select.value = 'WEEKLY';
    details('edit');
    expect({byday: true, monthly: false, bymonth: false}, 'a new weekly rule');
    select.value = 'YEARLY';
    details('edit');
    expect({byday: false, monthly: true, bymonth: true}, 'a new yearly rule');
    select.attributes = {};
    select.value = 'MONTHLY';
    details('create');
    expect({byday: false, monthly: true, bymonth: false}, 'a created monthly rule');
}
console.log(JSON.stringify(filled));
