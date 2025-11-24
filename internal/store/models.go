package store

import "time"

// User represents a person authenticated via OAuth.
type User struct {
	ID           int64
	OAuthSubject string
	PrimaryEmail string
	CreatedAt    time.Time
	LastLoginAt  time.Time
}

// Calendar is a CalDAV calendar belonging to a user.
type Calendar struct {
	ID        int64
	UserID    int64
	Name      string
	Color     *string
	CreatedAt time.Time
}

// Event stores raw iCalendar payload and metadata.
type Event struct {
	ID           int64
	CalendarID   int64
	UID          string
	RawICAL      string
	ETag         string
	LastModified time.Time
}

// AddressBook belongs to a user for CardDAV.
type AddressBook struct {
	ID        int64
	UserID    int64
	Name      string
	CreatedAt time.Time
}

// Contact stores raw vCard payload and metadata.
type Contact struct {
	ID            int64
	AddressBookID int64
	UID           string
	RawVCard      string
	ETag          string
	LastModified  time.Time
}

// AppPassword is a per-client credential for DAV access.
type AppPassword struct {
	ID         int64
	UserID     int64
	Label      string
	TokenHash  string
	CreatedAt  time.Time
	ExpiresAt  *time.Time
	RevokedAt  *time.Time
	LastUsedAt *time.Time
}
