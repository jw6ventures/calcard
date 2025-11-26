package store

import "context"

// UserRepository defines persistence operations for users.
type UserRepository interface {
	UpsertOAuthUser(ctx context.Context, subject, email string) (*User, error)
	GetByID(ctx context.Context, id int64) (*User, error)
	GetByEmail(ctx context.Context, email string) (*User, error)
}

// CalendarRepository handles calendars lifecycle.
type CalendarRepository interface {
	GetByID(ctx context.Context, id int64) (*Calendar, error)
	ListByUser(ctx context.Context, userID int64) ([]Calendar, error)
	Create(ctx context.Context, cal Calendar) (*Calendar, error)
	Rename(ctx context.Context, userID, id int64, name string) error
	Delete(ctx context.Context, userID, id int64) error
}

// EventRepository handles event storage.
type EventRepository interface {
	Upsert(ctx context.Context, event Event) (*Event, error)
	DeleteByUID(ctx context.Context, calendarID int64, uid string) error
	GetByUID(ctx context.Context, calendarID int64, uid string) (*Event, error)
	ListForCalendar(ctx context.Context, calendarID int64) ([]Event, error)
	ListByUIDs(ctx context.Context, calendarID int64, uids []string) ([]Event, error)
}

// AddressBookRepository manages address books.
type AddressBookRepository interface {
	GetByID(ctx context.Context, id int64) (*AddressBook, error)
	ListByUser(ctx context.Context, userID int64) ([]AddressBook, error)
	Create(ctx context.Context, book AddressBook) (*AddressBook, error)
	Rename(ctx context.Context, userID, id int64, name string) error
	Delete(ctx context.Context, userID, id int64) error
}

// ContactRepository handles vCard storage.
type ContactRepository interface {
	Upsert(ctx context.Context, contact Contact) (*Contact, error)
	DeleteByUID(ctx context.Context, addressBookID int64, uid string) error
	GetByUID(ctx context.Context, addressBookID int64, uid string) (*Contact, error)
	ListForBook(ctx context.Context, addressBookID int64) ([]Contact, error)
	ListByUIDs(ctx context.Context, addressBookID int64, uids []string) ([]Contact, error)
}

// AppPasswordRepository handles Basic Auth token storage.
type AppPasswordRepository interface {
	Create(ctx context.Context, token AppPassword) (*AppPassword, error)
	FindValidByUser(ctx context.Context, userID int64) ([]AppPassword, error)
	ListByUser(ctx context.Context, userID int64) ([]AppPassword, error)
	GetByID(ctx context.Context, id int64) (*AppPassword, error)
	Revoke(ctx context.Context, id int64) error
	TouchLastUsed(ctx context.Context, id int64) error
}

// TODO: add search/indexing helpers for REPORT support.
