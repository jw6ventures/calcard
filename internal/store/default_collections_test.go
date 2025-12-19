package store

import (
	"context"
	"regexp"
	"testing"
)

func TestEnsureDefaultCalendarCreatesWhenMissing(t *testing.T) {
	tx := &mockTx{
		execs: []execExpectation{
			{expect: regexp.MustCompile("pg_advisory_xact_lock"), args: []any{int64(1)}},
			{expect: regexp.MustCompile("INSERT INTO calendars"), args: []any{int64(1)}},
		},
		queries: []queryExpectation{
			{expect: regexp.MustCompile("SELECT EXISTS \\(SELECT 1 FROM calendars"), args: []any{int64(1)}, value: false},
		},
	}
	pool := &mockPool{t: t, txs: []*mockTx{tx}}

	store := &Store{pool: pool}
	if err := store.ensureDefaultCalendar(context.Background(), 1); err != nil {
		t.Fatalf("ensureDefaultCalendar returned error: %v", err)
	}

	pool.assertDone()
	tx.assertDone()
}

func TestEnsureDefaultCalendarSkipsWhenPresent(t *testing.T) {
	tx := &mockTx{
		execs: []execExpectation{
			{expect: regexp.MustCompile("pg_advisory_xact_lock"), args: []any{int64(2)}},
		},
		queries: []queryExpectation{
			{expect: regexp.MustCompile("SELECT EXISTS \\(SELECT 1 FROM calendars"), args: []any{int64(2)}, value: true},
		},
	}
	pool := &mockPool{t: t, txs: []*mockTx{tx}}

	store := &Store{pool: pool}
	if err := store.ensureDefaultCalendar(context.Background(), 2); err != nil {
		t.Fatalf("ensureDefaultCalendar returned error: %v", err)
	}

	pool.assertDone()
	tx.assertDone()
}

func TestEnsureDefaultAddressBookCreatesWhenMissing(t *testing.T) {
	tx := &mockTx{
		execs: []execExpectation{
			{expect: regexp.MustCompile("pg_advisory_xact_lock"), args: []any{int64(3)}},
			{expect: regexp.MustCompile("INSERT INTO address_books"), args: []any{int64(3)}},
		},
		queries: []queryExpectation{
			{expect: regexp.MustCompile("SELECT EXISTS \\(SELECT 1 FROM address_books"), args: []any{int64(3)}, value: false},
		},
	}
	pool := &mockPool{t: t, txs: []*mockTx{tx}}

	store := &Store{pool: pool}
	if err := store.ensureDefaultAddressBook(context.Background(), 3); err != nil {
		t.Fatalf("ensureDefaultAddressBook returned error: %v", err)
	}

	pool.assertDone()
	tx.assertDone()
}

func TestEnsureDefaultAddressBookSkipsWhenPresent(t *testing.T) {
	tx := &mockTx{
		execs: []execExpectation{
			{expect: regexp.MustCompile("pg_advisory_xact_lock"), args: []any{int64(4)}},
		},
		queries: []queryExpectation{
			{expect: regexp.MustCompile("SELECT EXISTS \\(SELECT 1 FROM address_books"), args: []any{int64(4)}, value: true},
		},
	}
	pool := &mockPool{t: t, txs: []*mockTx{tx}}

	store := &Store{pool: pool}
	if err := store.ensureDefaultAddressBook(context.Background(), 4); err != nil {
		t.Fatalf("ensureDefaultAddressBook returned error: %v", err)
	}

	pool.assertDone()
	tx.assertDone()
}
