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
	ID          int64
	UserID      int64
	Name        string
	Description *string
	Timezone    *string
	Color       *string
	CTag        int64
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// CalendarShare grants another user access to a calendar.
type CalendarShare struct {
	CalendarID int64
	UserID     int64
	GrantedBy  int64
	Editor     bool
	CreatedAt  time.Time
}

// CalendarAccess wraps a calendar with context about how the current user can access it.
type CalendarAccess struct {
	Calendar
	OwnerEmail string
	Shared     bool
	Editor     bool
}

// Event stores raw iCalendar payload and metadata.
type Event struct {
	ID           int64
	CalendarID   int64
	UID          string
	RawICAL      string
	ETag         string
	Summary      *string
	DTStart      *time.Time
	DTEnd        *time.Time
	AllDay       bool
	LastModified time.Time
}

// AddressBook belongs to a user for CardDAV.
type AddressBook struct {
	ID          int64
	UserID      int64
	Name        string
	Description *string
	CTag        int64
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// Contact stores raw vCard payload and metadata.
type Contact struct {
	ID            int64
	AddressBookID int64
	UID           string
	RawVCard      string
	ETag          string
	DisplayName   *string
	PrimaryEmail  *string
	Birthday      *time.Time
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

// DeletedResource tracks tombstones for sync reporting.
type DeletedResource struct {
	ID           int64
	ResourceType string // "event" or "contact"
	CollectionID int64
	UID          string
	DeletedAt    time.Time
}

// Session represents a database-backed user session.
type Session struct {
	ID         string
	UserID     int64
	UserAgent  *string
	IPAddress  *string
	CreatedAt  time.Time
	ExpiresAt  time.Time
	LastSeenAt time.Time
}

// PaginatedResult wraps a paginated query result.
type PaginatedResult[T any] struct {
	Items      []T
	TotalCount int
	Limit      int
	Offset     int
}
