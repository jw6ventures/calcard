// Runs addressbook_view.html's own birthday helpers and checks that the hidden
// field can never carry a date the server refuses, and that a half-chosen date
// never reaches the server at all.
//
// The day list is built by the page rather than by the markup, so it is the only
// thing standing between "31" chosen under January and a February birthday the
// vCard grammar has no day for. An empty hidden field is how the form asks the
// server to remove a birthday, so a date the form declines to spell must stop
// the submit rather than go out empty.
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

// Constraint validation as far as the form needs it: a message set with
// setCustomValidity makes the control invalid until it is cleared.
function validatable(node) {
  node.validationMessage = '';
  node.reported = 0;
  node.setCustomValidity = (message) => { node.validationMessage = String(message); };
  node.reportValidity = () => { node.reported++; return node.validationMessage === ''; };
  return node;
}

// A select refuses a value it has no option for, which is what makes an
// out-of-range day observable here rather than silently retained.
function selectElement(id, options) {
  return validatable({
    id,
    _options: options,
    _value: '',
    _listeners: {},
    addEventListener(type, handler) { (this._listeners[type] ||= []).push(handler); },
    set innerHTML(html) {
      this._options = [...String(html).matchAll(/<option value="([^"]*)"/g)].map((m) => m[1]);
      if (!this._options.includes(this._value)) this._value = '';
    },
    set value(next) {
      this._value = this._options.includes(String(next)) ? String(next) : '';
    },
    get value() { return this._value; },
  });
}

function freeInput(id, form) {
  return validatable({
    id,
    value: '',
    form,
    _listeners: {},
    addEventListener(type, handler) { (this._listeners[type] ||= []).push(handler); },
  });
}

const months = ['', ...Array.from({ length: 12 }, (_, i) => String(i + 1).padStart(2, '0'))];

function newForm(prefix) {
  const form = {
    _listeners: {},
    addEventListener(type, handler) { (this._listeners[type] ||= []).push(handler); },
  };
  const byId = {
    [`${prefix}-birthday-month`]: selectElement(`${prefix}-birthday-month`, months),
    [`${prefix}-birthday-day`]: selectElement(`${prefix}-birthday-day`, ['']),
    [`${prefix}-birthday-year`]: freeInput(`${prefix}-birthday-year`, form),
    [`${prefix}-birthday`]: freeInput(`${prefix}-birthday`, form),
  };
  const other = prefix === 'create' ? 'edit' : 'create';
  const context = {
    document: {
      getElementById(id) {
        // initBirthdaySelects wires both forms; the other one gets a throwaway
        // set of controls so this test can hold one form at a time.
        if (!byId[id] && id.startsWith(other + '-birthday')) {
          const suffix = id.slice(other.length);
          byId[id] = suffix === '-birthday-month' ? selectElement(id, months)
            : suffix === '-birthday-day' ? selectElement(id, [''])
            : freeInput(id, { addEventListener() {} });
        }
        assert.ok(byId[id], `the page looked up an element the birthday form does not have: ${id}`);
        return byId[id];
      },
    },
  };
  context.globalThis = context;
  vm.runInNewContext(
    [
      extract('birthdayDaysInMonth'),
      extract('syncBirthdayDayOptions'),
      extract('updateBirthdayHidden'),
      extract('setBirthdayFields'),
      extract('initBirthdaySelects'),
      'globalThis._sync = syncBirthdayDayOptions;',
      'globalThis._update = updateBirthdayHidden;',
      'globalThis._setFields = setBirthdayFields;',
      'globalThis._days = birthdayDaysInMonth;',
      'globalThis._init = initBirthdaySelects;',
    ].join('\n'),
    context,
  );
  context._init();

  // What a person does: pick a value, which fires change.
  const choose = (suffix, value) => {
    const el = byId[`${prefix}-birthday${suffix}`];
    el.value = value;
    for (const handler of el._listeners.change || []) handler.call(el, { target: el });
  };
  // What the browser does on submit: refuse while any control is invalid, and
  // otherwise dispatch submit, which a listener may still cancel.
  const submit = () => {
    const controls = ['-month', '-day', '-year'].map((s) => byId[`${prefix}-birthday${s}`]);
    if (controls.some((el) => el.validationMessage !== '')) return false;
    let prevented = false;
    const event = { preventDefault() { prevented = true; } };
    for (const handler of form._listeners.submit || []) handler(event);
    return !prevented;
  };
  const problem = () => ['-month', '-day', '-year']
    .map((s) => byId[`${prefix}-birthday${s}`].validationMessage)
    .filter(Boolean)
    .join(' ');
  return { byId, context, choose, submit, problem };
}

// The day count follows the month, and February follows the year. Without a
// year the value is stored as --MM-DD, which names a day in every year, so the
// leap day stays offered until a year rules it out.
for (const [month, year, want] of [
  ['01', '', 31],
  ['02', '', 29],
  ['02', '1900', 28],
  ['02', '2000', 29],
  ['02', '2001', 28],
  ['02', '4', 29],
  ['02', '1', 28],
  ['04', '', 30],
  ['12', '1988', 31],
]) {
  const { context } = newForm('create');
  assert.equal(
    context._days(month, year),
    want,
    `month ${month} of year ${year || '(none)'} = ${context._days(month, year)} days, want ${want}`,
  );
}

// Choosing 31 under a month that has it and then switching to one that does not
// must not leave the shorter month holding it -- and must not let the form go
// out with an empty birthday, which the server reads as "remove the birthday".
{
  const { byId, choose, submit, problem } = newForm('create');
  choose('-month', '01');
  choose('-day', '31');
  assert.equal(byId['create-birthday'].value, '--01-31', 'a January 31 birthday was not spelled');
  assert.equal(problem(), '', 'a whole date was flagged');

  choose('-month', '04');
  assert.equal(byId['create-birthday-day'].value, '', 'April kept day 31, which no April has');
  assert.equal(
    byId['create-birthday'].value,
    '',
    `the hidden field spelled ${byId['create-birthday'].value}, a date the server refuses`,
  );
  assert.notEqual(byId['create-birthday-day'].validationMessage, '', 'the emptied day is not flagged');
  assert.equal(submit(), false, 'a half-chosen birthday was submitted, which removes the stored one');

  choose('-day', '30');
  assert.equal(byId['create-birthday'].value, '--04-30');
  assert.equal(problem(), '', 'the flag outlived the day that cleared it');
  assert.equal(submit(), true, 'a whole date was refused');
}

// A day the shorter month does have survives the switch, so narrowing the list
// does not cost a selection it can still honour.
{
  const { byId, choose } = newForm('create');
  choose('-month', '01');
  choose('-day', '15');
  choose('-month', '02');
  assert.equal(byId['create-birthday'].value, '--02-15', 'a day February has was discarded');
}

// Typing a year that shortens February clears a 29 already chosen under it and
// holds the form until a day is chosen again, rather than submitting 29
// February of a non-leap year or an empty field that deletes the birthday.
{
  const { byId, context, choose, submit } = newForm('edit');
  context._setFields('edit', { year: null, month: 2, day: 29 });
  assert.equal(byId['edit-birthday'].value, '--02-29', 'a year-less leap-day birthday was refused');

  choose('-year', '1999');
  assert.equal(
    byId['edit-birthday'].value,
    '',
    `the hidden field spelled ${byId['edit-birthday'].value}, which 1999 has no day for`,
  );
  assert.equal(submit(), false, 'the leap-day birthday was submitted as a removal');
}

// A submit arriving before any change event -- a year typed and Enter pressed
// -- is still checked against the day list the year implies.
{
  const { byId, context, submit } = newForm('edit');
  context._setFields('edit', { year: null, month: 2, day: 29 });
  byId['edit-birthday-year'].value = '2001';
  assert.equal(submit(), false, 'a stale day list let 29 February 2001 through');
  assert.equal(byId['edit-birthday'].value, '');
}

// A day or a year with no month is as half-chosen as a month with no day.
for (const [suffix, value] of [['-day', '12'], ['-year', '1990']]) {
  const { choose, submit, problem } = newForm('create');
  choose(suffix, value);
  assert.notEqual(problem(), '', `a birthday of only ${suffix.slice(1)} ${value} is not flagged`);
  assert.equal(submit(), false, `a birthday of only ${suffix.slice(1)} ${value} was submitted`);
}

// Clearing every birthday control is the one way to remove a birthday, and it
// submits.
{
  const { byId, context, choose, submit, problem } = newForm('edit');
  context._setFields('edit', { year: 1990, month: 4, day: 15 });
  assert.equal(byId['edit-birthday'].value, '1990-04-15');
  choose('-month', '');
  choose('-day', '');
  choose('-year', '');
  assert.equal(byId['edit-birthday'].value, '');
  assert.equal(problem(), '');
  assert.equal(submit(), true, 'an explicit removal was refused');
}

// Opening the editor on a stored leap-day birthday has to select it, which it
// can only do once the list has been rebuilt for February.
{
  const { byId, context } = newForm('edit');
  context._setFields('edit', { year: 2000, month: 2, day: 29 });
  assert.equal(byId['edit-birthday-day'].value, '29', 'the stored leap day was not selected');
  assert.equal(byId['edit-birthday'].value, '2000-02-29', 'the stored leap day was not spelled back');
}

// A year below 1000 is still four digits in the value the server parses.
{
  const { byId, context } = newForm('edit');
  context._setFields('edit', { year: 415, month: 4, day: 15 });
  assert.equal(byId['edit-birthday'].value, '0415-04-15');
}

// Resetting the form leaves a day list as long as the longest month, not the
// one the last birthday was chosen under.
{
  const { byId, context, choose } = newForm('create');
  choose('-month', '02');
  choose('-year', '2001');
  assert.equal(byId['create-birthday-day']._options.length, 29, 'February 2001 is not 28 days plus the placeholder');
  context._setFields('create', null);
  assert.equal(byId['create-birthday-day']._options.length, 32, 'the reset form still offers only 28 days');
  assert.equal(byId['create-birthday'].value, '');
}

// Closing the create modal resets the birthday through the same path, so the
// next contact starts from a full day list and no leftover flag.
{
  const month = selectElement('create-birthday-month', months);
  const day = selectElement('create-birthday-day', ['']);
  const year = freeInput('create-birthday-year', null);
  const hidden = freeInput('create-birthday', null);
  const byId = {
    'create-birthday-month': month,
    'create-birthday-day': day,
    'create-birthday-year': year,
    'create-birthday': hidden,
  };
  const context = {
    document: {
      getElementById(id) {
        if (!byId[id]) byId[id] = { value: 'left over', classList: { remove() {} } };
        return byId[id];
      },
    },
  };
  context.globalThis = context;
  vm.runInNewContext(
    [
      extract('birthdayDaysInMonth'),
      extract('syncBirthdayDayOptions'),
      extract('updateBirthdayHidden'),
      extract('setBirthdayFields'),
      extract('closeCreateContactModal'),
      'globalThis._sync = syncBirthdayDayOptions;',
      'globalThis._update = updateBirthdayHidden;',
      'globalThis._close = closeCreateContactModal;',
    ].join('\n'),
    context,
  );
  month.value = '02';
  year.value = '2001';
  context._sync('create');
  context._update('create');
  assert.notEqual(day.validationMessage, '', 'February without a day is not flagged');

  context._close();
  assert.equal(month.value, '');
  assert.equal(year.value, '');
  assert.equal(hidden.value, '');
  assert.equal(day._options.length, 32, 'closing the modal left the February 2001 day list behind');
  assert.equal(day.validationMessage, '', 'closing the modal left the birthday flagged');
}
