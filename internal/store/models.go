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
	Slug        *string
	Description *string
	Timezone    *string
	Color       *string
	CTag        int64
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// CalendarPrivileges captures the effective collection privileges available to the current user.
type CalendarPrivileges struct {
	Read            bool `json:"read"`
	ReadFreeBusy    bool `json:"readFreeBusy"`
	Write           bool `json:"write"`
	WriteContent    bool `json:"writeContent"`
	WriteProperties bool `json:"writeProperties"`
	Bind            bool `json:"bind"`
	Unbind          bool `json:"unbind"`
}

func (p CalendarPrivileges) Normalized() CalendarPrivileges {
	p.Write = p.WriteContent && p.WriteProperties && p.Bind && p.Unbind
	return p
}

func (p CalendarPrivileges) HasAny() bool {
	return p.Read || p.ReadFreeBusy || p.Write || p.WriteContent || p.WriteProperties || p.Bind || p.Unbind
}

func (p CalendarPrivileges) Allows(privilege string) bool {
	p = p.Normalized()
	switch privilege {
	case "read":
		return p.Read
	case "read-free-busy":
		return p.ReadFreeBusy
	case "write":
		return p.Write
	case "write-content":
		return p.WriteContent
	case "write-properties":
		return p.WriteProperties
	case "bind":
		return p.Bind
	case "unbind":
		return p.Unbind
	default:
		return false
	}
}

func (p CalendarPrivileges) AllowsEventEditing() bool {
	p = p.Normalized()
	return p.Write || (p.WriteContent && p.Bind && p.Unbind)
}

func (p CalendarPrivileges) AllowsAnyWrite() bool {
	p = p.Normalized()
	return p.Write || p.WriteContent || p.WriteProperties || p.Bind || p.Unbind
}

func FullCalendarPrivileges() CalendarPrivileges {
	return CalendarPrivileges{
		Read:            true,
		ReadFreeBusy:    true,
		Write:           true,
		WriteContent:    true,
		WriteProperties: true,
		Bind:            true,
		Unbind:          true,
	}
}

// CalendarAccess wraps a calendar with context about how the current user can access it.
type CalendarAccess struct {
	Calendar
	OwnerEmail         string
	Shared             bool
	Editor             bool
	Privileges         CalendarPrivileges
	PrivilegesResolved bool
}

func (c CalendarAccess) EffectivePrivileges() CalendarPrivileges {
	if c.PrivilegesResolved || c.Privileges.HasAny() {
		return c.Privileges.Normalized()
	}
	if !c.Shared {
		return FullCalendarPrivileges()
	}
	if c.Editor {
		return FullCalendarPrivileges()
	}
	return CalendarPrivileges{Read: true, ReadFreeBusy: true}
}

// Event stores raw iCalendar payload and metadata.
type Event struct {
	ID           int64
	CalendarID   int64
	UID          string
	ResourceName string
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
	ResourceName  string
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
	ResourceName string
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

// Lock represents a WebDAV lock on a resource (RFC 4918).
type Lock struct {
	ID             int64
	Token          string
	ResourcePath   string
	UserID         int64
	LockScope      string
	LockType       string
	Depth          string
	OwnerInfo      string
	TimeoutSeconds int
	CreatedAt      time.Time
	ExpiresAt      time.Time
}

// ACLEntry represents a single access control entry (RFC 3744).
type ACLEntry struct {
	ID            int64
	ResourcePath  string
	PrincipalHref string
	IsGrant       bool
	Privilege     string
	CreatedAt     time.Time
}

// PaginatedResult wraps a paginated query result.
type PaginatedResult[T any] struct {
	Items      []T
	TotalCount int
	Limit      int
	Offset     int
}
