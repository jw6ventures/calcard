package store

import (
	"context"
	"time"
)

// UserRepository defines persistence operations for users.
type UserRepository interface {
	UpsertOAuthUser(ctx context.Context, subject, email string) (*User, error)
	GetByID(ctx context.Context, id int64) (*User, error)
	GetByEmail(ctx context.Context, email string) (*User, error)
	ListActive(ctx context.Context) ([]User, error)
}

// CalendarRepository handles calendars lifecycle.
type CalendarRepository interface {
	GetByID(ctx context.Context, id int64) (*Calendar, error)
	ListByUser(ctx context.Context, userID int64) ([]Calendar, error)
	ListAccessible(ctx context.Context, userID int64) ([]CalendarAccess, error)
	GetAccessible(ctx context.Context, calendarID, userID int64) (*CalendarAccess, error)
	Create(ctx context.Context, cal Calendar) (*Calendar, error)
	Update(ctx context.Context, userID, id int64, name string, description, timezone, color *string) error
	UpdateProperties(ctx context.Context, id int64, name string, description, timezone, color *string) error
	Rename(ctx context.Context, userID, id int64, name string) error
	Delete(ctx context.Context, userID, id int64) error
}

// EventRepository handles event storage.
type EventRepository interface {
	Upsert(ctx context.Context, event Event) (*Event, error)
	DeleteByUID(ctx context.Context, calendarID int64, uid string) error
	GetByUID(ctx context.Context, calendarID int64, uid string) (*Event, error)
	GetByResourceName(ctx context.Context, calendarID int64, resourceName string) (*Event, error)
	ListForCalendar(ctx context.Context, calendarID int64) ([]Event, error)
	ListForCalendarPaginated(ctx context.Context, calendarID int64, limit, offset int) (*PaginatedResult[Event], error)
	ListByUIDs(ctx context.Context, calendarID int64, uids []string) ([]Event, error)
	ListModifiedSince(ctx context.Context, calendarID int64, since time.Time) ([]Event, error)
	ListRecentByUser(ctx context.Context, userID int64, limit int) ([]Event, error)
	MaxLastModified(ctx context.Context, calendarID int64) (time.Time, error)
	MoveToCalendar(ctx context.Context, fromCalendarID, toCalendarID int64, uid, destResourceName string) error
	CopyToCalendar(ctx context.Context, fromCalendarID, toCalendarID int64, uid, destResourceName, newETag string) (*Event, error)
}

// AddressBookRepository manages address books.
type AddressBookRepository interface {
	GetByID(ctx context.Context, id int64) (*AddressBook, error)
	ListByUser(ctx context.Context, userID int64) ([]AddressBook, error)
	Create(ctx context.Context, book AddressBook) (*AddressBook, error)
	Update(ctx context.Context, userID, id int64, name string, description *string) error
	UpdateProperties(ctx context.Context, id int64, name string, description *string) error
	Rename(ctx context.Context, userID, id int64, name string) error
	Delete(ctx context.Context, userID, id int64) error
}

// ContactRepository handles vCard storage.
type ContactRepository interface {
	Upsert(ctx context.Context, contact Contact) (*Contact, error)
	DeleteByUID(ctx context.Context, addressBookID int64, uid string) error
	GetByUID(ctx context.Context, addressBookID int64, uid string) (*Contact, error)
	ListForBook(ctx context.Context, addressBookID int64) ([]Contact, error)
	ListForBookPaginated(ctx context.Context, addressBookID int64, limit, offset int) (*PaginatedResult[Contact], error)
	ListByUIDs(ctx context.Context, addressBookID int64, uids []string) ([]Contact, error)
	ListModifiedSince(ctx context.Context, addressBookID int64, since time.Time) ([]Contact, error)
	ListRecentByUser(ctx context.Context, userID int64, limit int) ([]Contact, error)
	MaxLastModified(ctx context.Context, addressBookID int64) (time.Time, error)
	ListWithBirthdaysByUser(ctx context.Context, userID int64) ([]Contact, error)
	MoveToAddressBook(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid, destResourceName string) error
	GetByResourceName(ctx context.Context, addressBookID int64, resourceName string) (*Contact, error)
	CopyToAddressBook(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid, destResourceName, newETag string) (*Contact, error)
}

// AppPasswordRepository handles Basic Auth token storage.
type AppPasswordRepository interface {
	Create(ctx context.Context, token AppPassword) (*AppPassword, error)
	FindValidByUser(ctx context.Context, userID int64) ([]AppPassword, error)
	ListByUser(ctx context.Context, userID int64) ([]AppPassword, error)
	GetByID(ctx context.Context, id int64) (*AppPassword, error)
	Revoke(ctx context.Context, id int64) error
	DeleteRevoked(ctx context.Context, id int64) error
	TouchLastUsed(ctx context.Context, id int64) error
}

// DeletedResourceRepository handles tombstone tracking for sync.
type DeletedResourceRepository interface {
	ListDeletedSince(ctx context.Context, resourceType string, collectionID int64, since time.Time) ([]DeletedResource, error)
	DeleteByIdentity(ctx context.Context, resourceType string, collectionID int64, uid, resourceName string) error
	Cleanup(ctx context.Context, olderThan time.Duration) (int64, error)
}

// SessionRepository handles database-backed sessions.
type SessionRepository interface {
	Create(ctx context.Context, session Session) (*Session, error)
	GetByID(ctx context.Context, id string) (*Session, error)
	ListByUser(ctx context.Context, userID int64) ([]Session, error)
	TouchLastSeen(ctx context.Context, id string) error
	Delete(ctx context.Context, id string) error
	DeleteByUser(ctx context.Context, userID int64) error
	DeleteExpired(ctx context.Context) (int64, error)
}

// LockRepository handles WebDAV lock storage.
type LockRepository interface {
	Create(ctx context.Context, lock Lock) (*Lock, error)
	GetByToken(ctx context.Context, token string) (*Lock, error)
	ListByResource(ctx context.Context, resourcePath string) ([]Lock, error)
	ListByResources(ctx context.Context, paths []string) ([]Lock, error)
	ListByResourcePrefix(ctx context.Context, prefix string) ([]Lock, error)
	MoveResourcePath(ctx context.Context, fromPath, toPath string) error
	DeleteByResourcePath(ctx context.Context, resourcePath string) error
	Delete(ctx context.Context, token string) error
	DeleteExpired(ctx context.Context) (int64, error)
	Refresh(ctx context.Context, token string, newTimeout int, newExpiry time.Time) (*Lock, error)
}

// ACLRepository handles WebDAV access control entries.
type ACLRepository interface {
	SetACL(ctx context.Context, resourcePath string, entries []ACLEntry) error
	ListByResource(ctx context.Context, resourcePath string) ([]ACLEntry, error)
	ListByPrincipal(ctx context.Context, principalHref string) ([]ACLEntry, error)
	HasPrivilege(ctx context.Context, resourcePath, principalHref, privilege string) (bool, error)
	DeletePrincipalEntriesByResourcePrefix(ctx context.Context, principalHref, resourcePathPrefix string) error
	MoveResourcePath(ctx context.Context, fromPath, toPath string) error
	Delete(ctx context.Context, resourcePath string) error
}
