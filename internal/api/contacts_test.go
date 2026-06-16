package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
)

type fakeAddressBookRepo struct {
	books map[int64]*store.AddressBook
}

func (f *fakeAddressBookRepo) GetByID(ctx context.Context, id int64) (*store.AddressBook, error) {
	b, ok := f.books[id]
	if !ok {
		return nil, nil
	}
	copy := *b
	return &copy, nil
}
func (f *fakeAddressBookRepo) ListByUser(ctx context.Context, userID int64) ([]store.AddressBook, error) {
	var out []store.AddressBook
	for _, b := range f.books {
		if b.UserID == userID {
			out = append(out, *b)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func (f *fakeAddressBookRepo) Create(ctx context.Context, book store.AddressBook) (*store.AddressBook, error) {
	return nil, nil
}
func (f *fakeAddressBookRepo) Update(ctx context.Context, userID, id int64, name string, description *string) error {
	return nil
}
func (f *fakeAddressBookRepo) UpdateProperties(ctx context.Context, id int64, name string, description *string) error {
	return nil
}
func (f *fakeAddressBookRepo) Rename(ctx context.Context, userID, id int64, name string) error {
	return nil
}
func (f *fakeAddressBookRepo) Delete(ctx context.Context, userID, id int64) error { return nil }

type fakeContactRepo struct {
	contacts map[string]store.Contact // key: "bookID:uid"
}

func contactKey(bookID int64, uid string) string {
	return strconv.FormatInt(bookID, 10) + ":" + uid
}

func (f *fakeContactRepo) Upsert(ctx context.Context, c store.Contact) (*store.Contact, error) {
	c.LastModified = time.Now().UTC()
	if c.ResourceName == "" {
		c.ResourceName = c.UID
	}
	// Mimic the store: derive display name / email from the vCard.
	if dn := vcardField(c.RawVCard, "FN:"); dn != "" {
		c.DisplayName = &dn
	}
	if em := vcardField(c.RawVCard, "EMAIL"); em != "" {
		c.PrimaryEmail = &em
	}
	f.contacts[contactKey(c.AddressBookID, c.UID)] = c
	copy := c
	return &copy, nil
}
func (f *fakeContactRepo) DeleteByUID(ctx context.Context, bookID int64, uid string) error {
	delete(f.contacts, contactKey(bookID, uid))
	return nil
}
func (f *fakeContactRepo) GetByUID(ctx context.Context, bookID int64, uid string) (*store.Contact, error) {
	c, ok := f.contacts[contactKey(bookID, uid)]
	if !ok {
		return nil, nil
	}
	copy := c
	return &copy, nil
}
func (f *fakeContactRepo) GetByResourceName(ctx context.Context, bookID int64, resourceName string) (*store.Contact, error) {
	for _, c := range f.contacts {
		if c.AddressBookID == bookID && c.ResourceName == resourceName {
			copy := c
			return &copy, nil
		}
	}
	return nil, nil
}
func (f *fakeContactRepo) ListForBook(ctx context.Context, bookID int64) ([]store.Contact, error) {
	var out []store.Contact
	for _, c := range f.contacts {
		if c.AddressBookID == bookID {
			out = append(out, c)
		}
	}
	return out, nil
}
func (f *fakeContactRepo) ListForBookFiltered(ctx context.Context, bookID int64, filter store.ContactFilter) ([]store.Contact, error) {
	all, _ := f.ListForBook(ctx, bookID)
	contains := func(p *string, term string) bool {
		return p != nil && strings.Contains(strings.ToLower(*p), strings.ToLower(term))
	}
	var out []store.Contact
	for _, c := range all {
		if filter.Name != "" && !contains(c.DisplayName, filter.Name) {
			continue
		}
		if filter.Email != "" && !contains(c.PrimaryEmail, filter.Email) {
			continue
		}
		if filter.Query != "" && !contains(c.DisplayName, filter.Query) && !contains(c.PrimaryEmail, filter.Query) {
			continue
		}
		out = append(out, c)
	}
	name := func(c store.Contact) string {
		if c.DisplayName != nil {
			return strings.ToLower(*c.DisplayName)
		}
		return ""
	}
	sort.SliceStable(out, func(i, j int) bool { return name(out[i]) < name(out[j]) })
	if filter.Offset > 0 {
		if filter.Offset >= len(out) {
			return []store.Contact{}, nil
		}
		out = out[filter.Offset:]
	}
	if filter.Limit > 0 && filter.Limit < len(out) {
		out = out[:filter.Limit]
	}
	return out, nil
}
func (f *fakeContactRepo) ListForBookPaginated(ctx context.Context, bookID int64, limit, offset int) (*store.PaginatedResult[store.Contact], error) {
	return nil, nil
}
func (f *fakeContactRepo) ListByUIDs(ctx context.Context, bookID int64, uids []string) ([]store.Contact, error) {
	return nil, nil
}
func (f *fakeContactRepo) ListModifiedSince(ctx context.Context, bookID int64, since time.Time) ([]store.Contact, error) {
	return nil, nil
}
func (f *fakeContactRepo) ListRecentByUser(ctx context.Context, userID int64, limit int) ([]store.Contact, error) {
	return nil, nil
}
func (f *fakeContactRepo) MaxLastModified(ctx context.Context, bookID int64) (time.Time, error) {
	return time.Time{}, nil
}
func (f *fakeContactRepo) ListWithBirthdaysByUser(ctx context.Context, userID int64) ([]store.Contact, error) {
	return nil, nil
}
func (f *fakeContactRepo) MoveToAddressBook(ctx context.Context, from, to int64, uid, destResourceName string) error {
	return nil
}
func (f *fakeContactRepo) CopyToAddressBook(ctx context.Context, from, to int64, uid, destResourceName, newETag string) (*store.Contact, error) {
	return nil, nil
}

// vcardField returns the value of the first line beginning with prefix.
func vcardField(vcard, prefix string) string {
	for _, line := range strings.Split(vcard, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.HasPrefix(line, prefix) {
			if i := strings.Index(line, ":"); i >= 0 {
				return strings.TrimSpace(line[i+1:])
			}
		}
	}
	return ""
}

func newContactsHandler(books map[int64]*store.AddressBook, contacts map[string]store.Contact) *Handler {
	return NewHandler(&config.Config{}, &store.Store{
		AddressBooks: &fakeAddressBookRepo{books: books},
		Contacts:     &fakeContactRepo{contacts: contacts},
	})
}

func strptr(s string) *string { return &s }

func TestListAddressBooks(t *testing.T) {
	h := newContactsHandler(map[int64]*store.AddressBook{
		1: {ID: 1, UserID: 1, Name: "Personal"},
		2: {ID: 2, UserID: 1, Name: "Work", Description: strptr("colleagues")},
		3: {ID: 3, UserID: 99, Name: "Someone else"},
	}, map[string]store.Contact{})

	req := httptest.NewRequest(http.MethodGet, "/api/addressbooks", nil)
	req = withUserAndRoute(req, "", "")
	rec := httptest.NewRecorder()
	h.ListAddressBooks(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	var out []addressBookResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Fatalf("expected only owned books, got %d: %+v", len(out), out)
	}
}

func TestGetAddressBookOwnership(t *testing.T) {
	h := newContactsHandler(map[int64]*store.AddressBook{
		1: {ID: 1, UserID: 1, Name: "Personal"},
		3: {ID: 3, UserID: 99, Name: "Someone else"},
	}, map[string]store.Contact{})

	req := withUserAndRoute(httptest.NewRequest(http.MethodGet, "/api/addressbooks/1", nil), "1", "")
	rec := httptest.NewRecorder()
	h.GetAddressBook(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("owned status=%d", rec.Code)
	}
	// Not owned -> 404 (not 403, to avoid leaking existence)
	req = withUserAndRoute(httptest.NewRequest(http.MethodGet, "/api/addressbooks/3", nil), "3", "")
	rec = httptest.NewRecorder()
	h.GetAddressBook(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unowned status=%d, want 404", rec.Code)
	}
}

func TestListContactsFilter(t *testing.T) {
	h := newContactsHandler(
		map[int64]*store.AddressBook{1: {ID: 1, UserID: 1, Name: "Personal"}},
		map[string]store.Contact{
			"1:a": {AddressBookID: 1, UID: "a", ResourceName: "a", ETag: "e", DisplayName: strptr("Alice Smith"), PrimaryEmail: strptr("alice@example.com")},
			"1:b": {AddressBookID: 1, UID: "b", ResourceName: "b", ETag: "e", DisplayName: strptr("Bob Jones"), PrimaryEmail: strptr("bob@work.com")},
			"1:c": {AddressBookID: 1, UID: "c", ResourceName: "c", ETag: "e", DisplayName: strptr("Carol Smith")},
		},
	)

	listUIDs := func(query string) []string {
		req := withUserAndRoute(httptest.NewRequest(http.MethodGet, "/api/addressbooks/1/contacts?"+query, nil), "1", "")
		rec := httptest.NewRecorder()
		h.ListContacts(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("query %q status=%d body=%s", query, rec.Code, rec.Body.String())
		}
		var out []contactResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		uids := make([]string, len(out))
		for i, c := range out {
			uids[i] = c.UID
		}
		return uids
	}

	tests := []struct {
		name, query string
		want        []string
	}{
		{"all ordered by name", "", []string{"a", "b", "c"}},
		{"name", "name=smith", []string{"a", "c"}},
		{"email", "email=work.com", []string{"b"}},
		{"q matches email", "q=example.com", []string{"a"}},
		{"q matches name", "q=carol", []string{"c"}},
		{"limit", "limit=2", []string{"a", "b"}},
		{"offset", "offset=2", []string{"c"}},
		{"no match", "name=zzz", []string{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := listUIDs(tc.query); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("query %q = %v, want %v", tc.query, got, tc.want)
			}
		})
	}
}

func TestContactCreateGetUpdateDelete(t *testing.T) {
	h := newContactsHandler(
		map[int64]*store.AddressBook{1: {ID: 1, UserID: 1, Name: "Personal"}},
		map[string]store.Contact{},
	)

	body := `{"structured":{"uid":"u1","displayName":"Dana Lee","email":"dana@example.com"}}`
	req := withUserAndRoute(httptest.NewRequest(http.MethodPost, "/api/addressbooks/1/contacts", strings.NewReader(body)), "1", "")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.CreateContact(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", rec.Code, rec.Body.String())
	}
	var created contactResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.UID != "u1" || created.DisplayName == nil || *created.DisplayName != "Dana Lee" {
		t.Fatalf("unexpected created contact: %+v", created)
	}
	if !strings.Contains(created.RawVCard, "BEGIN:VCARD") {
		t.Fatalf("expected vCard body, got %q", created.RawVCard)
	}

	req = withUserAndRoute(httptest.NewRequest(http.MethodPost, "/api/addressbooks/1/contacts", strings.NewReader(body)), "1", "")
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	h.CreateContact(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("duplicate create status=%d, want 409", rec.Code)
	}

	req = withUserAndRoute(httptest.NewRequest(http.MethodGet, "/api/addressbooks/1/contacts/u1", nil), "1", "u1")
	rec = httptest.NewRecorder()
	h.GetContact(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("get status=%d", rec.Code)
	}
	if etag := rec.Header().Get("ETag"); etag == "" {
		t.Fatal("expected ETag header on get")
	}

	upd := `{"structured":{"uid":"u1","displayName":"Dana M. Lee","email":"dana@example.com"}}`
	req = withUserAndRoute(httptest.NewRequest(http.MethodPut, "/api/addressbooks/1/contacts/u1", strings.NewReader(upd)), "1", "u1")
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	h.UpdateContact(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("update status=%d body=%s", rec.Code, rec.Body.String())
	}
	var updated contactResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &updated)
	if updated.DisplayName == nil || *updated.DisplayName != "Dana M. Lee" {
		t.Fatalf("update did not apply: %+v", updated)
	}

	req = withUserAndRoute(httptest.NewRequest(http.MethodPut, "/api/addressbooks/1/contacts/missing", strings.NewReader(`{"structured":{"displayName":"x"}}`)), "1", "missing")
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	h.UpdateContact(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("update missing status=%d, want 404", rec.Code)
	}

	req = withUserAndRoute(httptest.NewRequest(http.MethodDelete, "/api/addressbooks/1/contacts/u1", nil), "1", "u1")
	rec = httptest.NewRecorder()
	h.DeleteContact(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete status=%d, want 204", rec.Code)
	}
	req = withUserAndRoute(httptest.NewRequest(http.MethodGet, "/api/addressbooks/1/contacts/u1", nil), "1", "u1")
	rec = httptest.NewRecorder()
	h.GetContact(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("get after delete status=%d, want 404", rec.Code)
	}
}

func TestCreateContactValidation(t *testing.T) {
	h := newContactsHandler(
		map[int64]*store.AddressBook{1: {ID: 1, UserID: 1, Name: "Personal"}},
		map[string]store.Contact{},
	)
	req := withUserAndRoute(httptest.NewRequest(http.MethodPost, "/api/addressbooks/1/contacts", strings.NewReader(`{"structured":{"email":"x@y.com"}}`)), "1", "")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.CreateContact(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing displayName status=%d, want 400", rec.Code)
	}

	req = withUserAndRoute(httptest.NewRequest(http.MethodPost, "/api/addressbooks/2/contacts", strings.NewReader(`{"structured":{"displayName":"x"}}`)), "2", "")
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	h.CreateContact(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unowned book create status=%d, want 404", rec.Code)
	}
}

func TestCreateContactRawVCard(t *testing.T) {
	h := newContactsHandler(
		map[int64]*store.AddressBook{1: {ID: 1, UserID: 1, Name: "Personal"}},
		map[string]store.Contact{},
	)
	raw := "BEGIN:VCARD\nVERSION:3.0\nUID:raw1\nFN:Raw Person\nEMAIL:raw@example.com\nEND:VCARD\n"
	req := withUserAndRoute(httptest.NewRequest(http.MethodPost, "/api/addressbooks/1/contacts", strings.NewReader(raw)), "1", "")
	req.Header.Set("Content-Type", "text/vcard")
	rec := httptest.NewRecorder()
	h.CreateContact(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("raw create status=%d body=%s", rec.Code, rec.Body.String())
	}
	var created contactResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &created)
	if created.UID != "raw1" {
		t.Fatalf("expected uid from vcard, got %q", created.UID)
	}
	if !strings.Contains(created.RawVCard, "\r\n") {
		t.Fatal("expected CRLF-normalized vCard body")
	}
}
