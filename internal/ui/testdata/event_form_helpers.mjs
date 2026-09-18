// Runs the calendar templates' own attendee formatting and create-form date
// check. Source-level assertions in templates_test.go only prove the helpers
// are present; these prove they behave.
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
vm.createContext(context);
vm.runInContext(extract('formatEmailValue'), context);

const email = 'James@jameswilliams.business';
const emailCases = [
  // A CN repeating the address is what rendered it twice on the "Trek" event.
  [`mailto:${email}`, {CN: email}, email],
  // The repeat is still a repeat when the two differ only in case.
  [`mailto:${email}`, {CN: email.toLowerCase()}, email],
  [`MAILTO:${email}`, {CN: email}, email],
  // A CN that is genuinely a name is still shown.
  [`mailto:${email}`, {CN: 'James Williams'}, `James Williams <${email}>`],
  [`mailto:${email}`, {}, email],
  [`mailto:${email}`, null, email],
  // Whitespace around the CN does not make it a distinct name.
  [`mailto:${email}`, {CN: `  ${email}  `}, email],
  ['', {CN: 'James Williams'}, ''],
];
for (const [value, params, want] of emailCases) {
  const got = context.formatEmailValue(value, params);
  assert.equal(got, want, `formatEmailValue(${JSON.stringify(value)}, ${JSON.stringify(params)}) = ${JSON.stringify(got)}, want ${JSON.stringify(want)}`);
}

// createEventDateError only exists on the single-calendar page, which is the
// one carrying the create modal.
if (source.includes('function createEventDateError(')) {
  vm.runInContext(extract('createEventDateError'), context);
  const dateCases = [
    // The reported case: an end before the start, rejected before submitting.
    ['2026-09-17T10:00', '2026-09-17T09:00', false, true],
    ['2026-09-18T10:00', '2026-09-17T10:00', false, true],
    // A timed event of zero length is what the server rejects too.
    ['2026-09-17T10:00', '2026-09-17T10:00', false, true],
    ['2026-09-17T10:00', '2026-09-17T11:00', false, false],
    // An all-day event may start and end on the same day; the form's end date
    // is inclusive.
    ['2026-09-17', '2026-09-17', true, false],
    ['2026-09-17', '2026-09-16', true, true],
    ['2026-09-17', '2026-09-18', true, false],
    // An incomplete form is the browser's required-field job, not this check's.
    ['', '2026-09-17T11:00', false, false],
    ['2026-09-17T10:00', '', false, false],
  ];
  for (const [start, end, allDay, wantError] of dateCases) {
    const got = context.createEventDateError(start, end, allDay);
    assert.equal(
      Boolean(got), wantError,
      `createEventDateError(${JSON.stringify(start)}, ${JSON.stringify(end)}, ${allDay}) = ${JSON.stringify(got)}, want ${wantError ? 'an error' : 'null'}`,
    );
  }
}
