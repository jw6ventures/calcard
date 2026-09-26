// Runs birthdays.html's own date helpers over a 29 February birthday. Built as
// new Date(year, 1, 29), it is 1 March in three years out of four: shown as
// "March 1" in the modal, missing from February's grid, and counted down to
// the wrong day. It is kept on 28 February in those years instead.
//
// Usage: node birthdays_leap_day.mjs birthdays.html safe_html_js.tmpl
import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';

const source = process.argv.slice(2).map((path) => fs.readFileSync(path, 'utf8')).join('\n');

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

const byId = {};
const context = {
  document: {
    getElementById(id) {
      if (!byId[id]) byId[id] = { id, textContent: '', innerHTML: '', classList: { add() {} } };
      return byId[id];
    },
  },
  modal: { classList: { add() {} } },
};
context.globalThis = context;
vm.runInNewContext(
  [
    extract('escapeHtml'),
    extract('observedBirthdayDay'),
    extract('birthdayDate'),
    extract('nextBirthday'),
    extract('daysUntil'),
    extract('birthdaysOn'),
    extract('birthdayDateLabel'),
    extract('showBirthdayModal'),
    'Object.assign(globalThis, { birthdayDateLabel, birthdaysOn, nextBirthday, daysUntil, showBirthdayModal });',
  ].join('\n'),
  context,
);

const leapling = { displayName: 'Ada <Lovelace>', month: 2, day: 29, birthYear: null };
const march1 = { displayName: 'Bea', month: 3, day: 1, birthYear: null };
const monthDay = (year, month, day) => new Date(year, month - 1, day).toLocaleDateString(undefined, { month: 'long', day: 'numeric' });

// The modal names the day it is marked and, in a year without 29 February,
// the day it really is -- never 1 March.
context.showBirthdayModal(leapling, 2027);
const modalHtml = byId['modal-body'].innerHTML;
assert.ok(modalHtml.includes(monthDay(2027, 2, 28)), `2027 modal does not show 28 February: ${modalHtml}`);
assert.ok(modalHtml.includes(monthDay(2000, 2, 29)), `2027 modal does not name the real birthday: ${modalHtml}`);
assert.ok(!modalHtml.includes(monthDay(2027, 3, 1)), `2027 modal shows 1 March: ${modalHtml}`);
assert.ok(modalHtml.includes('Ada &lt;Lovelace&gt;'), `the name was not escaped: ${modalHtml}`);

const leapLabel = context.birthdayDateLabel(leapling, 2028);
assert.ok(leapLabel.includes(monthDay(2028, 2, 29)), `2028 label is ${leapLabel}`);
assert.ok(!leapLabel.includes('('), `a leap year needs no note: ${leapLabel}`);

// The grid shows it on 28 February in a common year and 29 February in a leap
// year, and never on 1 March.
const on = (year, month, day) => [...context.birthdaysOn([leapling, march1], new Date(year, month - 1, day))].map((b) => b.displayName);
assert.deepEqual(on(2027, 2, 28), ['Ada <Lovelace>']);
assert.deepEqual(on(2027, 3, 1), ['Bea']);
assert.deepEqual(on(2028, 2, 28), []);
assert.deepEqual(on(2028, 2, 29), ['Ada <Lovelace>']);
assert.deepEqual(on(2028, 3, 1), ['Bea']);

// The countdown targets the same day, and a birthday today is today's.
const next = context.nextBirthday(leapling, new Date(2027, 0, 10, 15, 30));
assert.deepEqual([next.getFullYear(), next.getMonth() + 1, next.getDate()], [2027, 2, 28]);
assert.equal(context.daysUntil(leapling, new Date(2027, 1, 28, 18, 0)), 0, 'a birthday today counted as next year');
assert.equal(context.daysUntil(leapling, new Date(2027, 2, 1, 9, 0)), 365, 'the next leap-day birthday after 1 March 2027');
const after = context.nextBirthday(leapling, new Date(2027, 2, 1));
assert.deepEqual([after.getFullYear(), after.getMonth() + 1, after.getDate()], [2028, 2, 29]);

// The age shown is the one reached in the year shown, worked out from the
// birth year alone: it does not depend on the year the server or the browser
// is in when the page is read.
context.showBirthdayModal({ displayName: 'Cy', month: 1, day: 1, birthYear: 1990 }, 2027);
assert.ok(byId['modal-body'].innerHTML.includes('Turns 37 years old'), `2027 modal age: ${byId['modal-body'].innerHTML}`);
context.showBirthdayModal({ displayName: 'Cy', month: 1, day: 1, birthYear: 1990 }, 2031);
assert.ok(byId['modal-body'].innerHTML.includes('Turns 41 years old'), `2031 modal age: ${byId['modal-body'].innerHTML}`);
context.showBirthdayModal({ displayName: 'Dee', month: 1, day: 1, birthYear: null }, 2027);
assert.ok(!byId['modal-body'].innerHTML.includes('Turns'), `an unknown birth year showed an age: ${byId['modal-body'].innerHTML}`);
assert.ok(!source.includes('.age +'), 'the page still adds to an age fixed to one year');
