package dav

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// DAVx5 reads CARDDAV:supported-address-data, sees vCard 4.0 advertised, and
// asks for version="4.0" in every addressbook-multiget. Contacts created by the
// CalCard UI are stored as vCard 3.0, so refusing the conversion hides them
// from the client for good: the sync token still advances, so the addition is
// never replayed.
func TestAddressbookMultigetConvertsStoredVCardToRequestedVersion(t *testing.T) {
	user := &store.User{ID: 1}
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now, CTag: 1},
		},
	}
	h := &DavServer{store: &store.Store{
		AddressBooks: bookRepo,
		Contacts: &fakeContactRepo{contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", ETag: "etag-3", RawVCard: buildVCard(
				"3.0",
				"UID:alice",
				"FN:Alice Adams",
				"N:Adams;Alice;;;",
				"EMAIL;TYPE=INTERNET,PREF:alice@example.com",
				"BDAY:1985-04-12",
			)},
		}},
	}}

	body := `<?xml version='1.0' encoding='UTF-8' ?><CARD:addressbook-multiget xmlns="DAV:" xmlns:CARD="urn:ietf:params:xml:ns:carddav"><prop><getcontenttype /><getetag /><CARD:address-data content-type="text/vcard" version="4.0" /></prop><href>/dav/addressbooks/5/alice.vcf</href></CARD:addressbook-multiget>`

	req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207 multistatus, got %d", rr.Code)
	}
	respBody := rr.Body.String()
	if strings.Contains(respBody, "supported-address-data-conversion") {
		t.Fatalf("a stored vCard 3.0 must be converted for a vCard 4.0 multiget, not refused: %s", respBody)
	}
	if !strings.Contains(respBody, "VERSION:4.0") {
		t.Fatalf("multiget response must carry the requested vCard version, got %s", respBody)
	}
	if !strings.Contains(respBody, "FN:Alice Adams") {
		t.Fatalf("converted card must retain contact data, got %s", respBody)
	}
}

// The other paths a client can read address data through have to convert as
// well, or the same contact stays invisible to a client that reads it with GET
// or an addressbook-query instead of a multiget.
func TestAddressDataReadPathsConvertStoredVCard(t *testing.T) {
	user := &store.User{ID: 1}
	now := store.Now()
	newServer := func() *DavServer {
		return &DavServer{store: &store.Store{
			AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now, CTag: 1},
			}},
			Contacts: &fakeContactRepo{contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", ETag: "etag-3", RawVCard: buildVCard(
					"3.0", "UID:alice", "FN:Alice Adams", "EMAIL;TYPE=INTERNET:alice@example.com",
				)},
			}},
		}}
	}

	t.Run("GET honours the Accept version", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.vcf", nil)
		req.Header.Set("Accept", `text/vcard; version="4.0"`)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		newServer().Get(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "VERSION:4.0") {
			t.Fatalf("GET must serve the accepted vCard version, got %s", rr.Body.String())
		}
	})

	t.Run("addressbook-query honours the requested version", func(t *testing.T) {
		body := `<?xml version="1.0"?><card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:d="DAV:"><d:prop><d:getetag/><card:address-data content-type="text/vcard" version="4.0"/></d:prop><card:filter><card:prop-filter name="FN"/></card:filter></card:addressbook-query>`
		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		newServer().Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "VERSION:4.0") {
			t.Fatalf("addressbook-query must serve the requested vCard version, got %s", rr.Body.String())
		}
	})
}

func TestConvertVCardVersion(t *testing.T) {
	tests := []struct {
		name   string
		target string
		source []string
		want   []string
	}{
		{
			name:   "vcard3 preferred email loses the INTERNET type and gains PREF",
			target: "4.0",
			source: []string{"EMAIL;TYPE=INTERNET,HOME,PREF:alice@example.com"},
			want:   []string{"EMAIL;TYPE=HOME;PREF=1:alice@example.com"},
		},
		{
			name:   "vcard4 preference parameter becomes a vcard3 type",
			target: "3.0",
			source: []string{"EMAIL;TYPE=HOME;PREF=1:alice@example.com"},
			want:   []string{"EMAIL;TYPE=HOME,PREF:alice@example.com"},
		},
		{
			name:   "vcard3 dates compact to the basic ISO 8601 form",
			target: "4.0",
			source: []string{"BDAY:1985-04-12", "REV:2026-09-13T20:04:58Z"},
			want:   []string{"BDAY:19850412", "REV:20260913T200458Z"},
		},
		{
			name:   "vcard3 birthdays without a year keep their reduced accuracy",
			target: "4.0",
			source: []string{"BDAY:--04-12"},
			want:   []string{"BDAY:--0412"},
		},
		{
			name:   "vcard4 dates expand to the extended ISO 8601 form",
			target: "3.0",
			source: []string{"BDAY:19850412", "REV:20260913T200458Z"},
			want:   []string{"BDAY:1985-04-12", "REV:2026-09-13T20:04:58Z"},
		},
		{
			name:   "textual birthdays are left alone",
			target: "4.0",
			source: []string{"BDAY;VALUE=TEXT:circa 1800"},
			want:   []string{"BDAY;VALUE=TEXT:circa 1800"},
		},
		{
			name:   "inline vcard3 photos become data URIs",
			target: "4.0",
			source: []string{"PHOTO;ENCODING=b;TYPE=JPEG:/9j/4AAQ"},
			want:   []string{"PHOTO:data:image/jpeg;base64,/9j/4AAQ"},
		},
		{
			name:   "vcard4 data URIs become inline base64 values",
			target: "3.0",
			source: []string{"PHOTO:data:image/png;base64,iVBORw0K"},
			want:   []string{"PHOTO;ENCODING=b;TYPE=PNG:iVBORw0K"},
		},
		{
			// RFC 2426 Section 4 defaults PHOTO to an inline binary value, so
			// a 3.0 client reading an unlabelled "https://..." reads it as a
			// corrupt image. RFC 6350 made the URI the only form, which is why
			// the 4.0 card does not have to say so and the 3.0 one does.
			name:   "photo URI references survive both ways, labelled as URIs",
			target: "3.0",
			source: []string{"PHOTO;MEDIATYPE=image/jpeg:https://example.com/a.jpg"},
			want:   []string{"PHOTO;VALUE=uri:https://example.com/a.jpg"},
		},
		{
			name:   "vcard3 group members use the address book server names",
			target: "4.0",
			source: []string{"X-ADDRESSBOOKSERVER-KIND:group", "X-ADDRESSBOOKSERVER-MEMBER:urn:uuid:alice"},
			want:   []string{"KIND:group", "MEMBER:urn:uuid:alice"},
		},
		{
			name:   "vcard4 group members fall back to the address book server names",
			target: "3.0",
			source: []string{"KIND:group", "MEMBER:urn:uuid:alice"},
			want:   []string{"X-ADDRESSBOOKSERVER-KIND:group", "X-ADDRESSBOOKSERVER-MEMBER:urn:uuid:alice"},
		},
		{
			name:   "vcard4 anniversary and gender fall back to extension properties",
			target: "3.0",
			source: []string{"ANNIVERSARY:20100615", "GENDER:F"},
			want:   []string{"X-ANNIVERSARY:2010-06-15", "X-GENDER:F"},
		},
		{
			name:   "properties vcard4 removed are dropped",
			target: "4.0",
			source: []string{"FN:Alice Adams", "LABEL;TYPE=HOME:1 Main St", "CLASS:PUBLIC", "MAILER:Mutt", "SORT-STRING:Adams"},
			want:   []string{"FN:Alice Adams"},
		},
		{
			// The N is derived rather than dropped: RFC 2426 Section 3.1.2
			// requires it and RFC 6350 Section 6.2.2 made it optional, so a
			// valid 4.0 card can arrive without one.
			name:   "properties vcard3 cannot express are dropped",
			target: "3.0",
			source: []string{"FN:Alice Adams", "LANG:en", "XML:<x/>", "CLIENTPIDMAP:1;urn:uuid:1", "RELATED;TYPE=friend:urn:uuid:bob"},
			want:   []string{"FN:Alice Adams", "N:Adams;Alice;;;"},
		},
		{
			name:   "parameters vcard3 cannot express are dropped",
			target: "3.0",
			source: []string{"EMAIL;ALTID=1;PID=1.1;TYPE=WORK:alice@example.com", "N;SORT-AS=Adams:Adams;Alice;;;"},
			want:   []string{"EMAIL;TYPE=WORK:alice@example.com", "N:Adams;Alice;;;"},
		},
		{
			name:   "the vcard3 charset parameter is dropped",
			target: "4.0",
			source: []string{"FN;CHARSET=UTF-8:Alice Adams"},
			want:   []string{"FN:Alice Adams"},
		},
		{
			name:   "vcard4 telephone URIs become plain vcard3 numbers",
			target: "3.0",
			source: []string{"TEL;VALUE=uri;TYPE=cell:tel:+15555550123"},
			want:   []string{"TEL;TYPE=cell:+15555550123"},
		},
		{
			name:   "geographic positions change syntax in both versions",
			target: "4.0",
			source: []string{"GEO:37.386013;-122.082932"},
			want:   []string{"GEO:geo:37.386013,-122.082932"},
		},
		{
			name:   "geographic positions come back from the vcard4 URI form",
			target: "3.0",
			source: []string{"GEO:geo:37.386013,-122.082932"},
			want:   []string{"GEO:37.386013;-122.082932"},
		},
		{
			name:   "property groups and extensions are preserved",
			target: "4.0",
			source: []string{"item1.TEL;TYPE=VOICE:+15555550123", "item1.X-ABLabel:Direct", "X-CUSTOM:kept"},
			want:   []string{"item1.TEL;TYPE=VOICE:+15555550123", "item1.X-ABLabel:Direct", "X-CUSTOM:kept"},
		},
		{
			name:   "quoted parameter values keep their delimiters",
			target: "4.0",
			source: []string{`TEL;TYPE="voice,work";X-NOTE="a;b":+15555550123`},
			want:   []string{`TEL;TYPE="voice,work";X-NOTE="a;b":+15555550123`},
		},
		{
			name:   "legacy bare type parameters are normalised",
			target: "4.0",
			source: []string{"TEL;WORK;VOICE:+15555550123"},
			want:   []string{"TEL;TYPE=WORK,VOICE:+15555550123"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			from := "3.0"
			if tc.target == "3.0" {
				from = "4.0"
			}
			got, ok := convertVCardVersion(buildVCard(from, tc.source...), tc.target)
			if !ok {
				t.Fatalf("convertVCardVersion(%q) refused to convert", tc.source)
			}
			want := buildVCard(tc.target, tc.want...)
			if got != want {
				t.Errorf("convertVCardVersion() =\n%q\nwant\n%q", got, want)
			}
		})
	}
}

func TestConvertVCardVersionRefusals(t *testing.T) {
	tests := []struct {
		name   string
		raw    string
		target string
	}{
		{name: "unsupported target version", raw: buildVCard("3.0", "FN:Alice"), target: "2.1"},
		{name: "unsupported stored version", raw: buildVCard("2.1", "FN:Alice"), target: "4.0"},
		{name: "no version property", raw: "BEGIN:VCARD\r\nFN:Alice\r\nEND:VCARD\r\n", target: "4.0"},
		{name: "ambiguous version", raw: "BEGIN:VCARD\r\nVERSION:3.0\r\nVERSION:4.0\r\nFN:Alice\r\nEND:VCARD\r\n", target: "4.0"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := convertVCardVersion(tc.raw, tc.target); ok {
				t.Fatalf("convertVCardVersion(%q, %q) should report the conversion precondition", tc.raw, tc.target)
			}
		})
	}
}

func TestConvertVCardVersionKeepsMatchingVersionVerbatim(t *testing.T) {
	// Folding, ordering and escaping are the writing client's; an unconverted
	// read must hand back exactly what was stored.
	raw := "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:alice\r\nFN:Alice Adams\r\nNOTE:long\r\n  folded\r\nEND:VCARD\r\n"
	got, ok := convertVCardVersion(raw, "3.0")
	if !ok {
		t.Fatal("converting to the stored version must succeed")
	}
	if got != raw {
		t.Errorf("stored data must be returned verbatim, got %q", got)
	}
}

func TestConvertVCardVersionRoundTripPreservesContactData(t *testing.T) {
	original := buildVCard("3.0",
		"UID:alice",
		"FN:Alice Adams",
		"N:Adams;Alice;;;",
		"ORG:Example Inc.",
		"EMAIL;TYPE=INTERNET,HOME:alice@example.com",
		"TEL;TYPE=CELL:+15555550123",
		"BDAY:1985-04-12",
		`NOTE:Escaped\, value\; here`,
	)

	upgraded, ok := convertVCardVersion(original, "4.0")
	if !ok {
		t.Fatal("vCard 3.0 must upgrade")
	}
	roundTripped, ok := convertVCardVersion(upgraded, "3.0")
	if !ok {
		t.Fatal("vCard 4.0 must downgrade")
	}

	for _, want := range []string{
		"UID:alice",
		"FN:Alice Adams",
		"N:Adams;Alice;;;",
		"ORG:Example Inc.",
		"alice@example.com",
		"+15555550123",
		"BDAY:1985-04-12",
		`NOTE:Escaped\, value\; here`,
	} {
		if !strings.Contains(roundTripped, want) {
			t.Errorf("round trip lost %q:\n%s", want, roundTripped)
		}
	}
}

// Conversion rewrote content lines one at a time and never asked whether the
// result was a valid card of the version it claimed. RFC 2426 Section 3.1.2
// requires N, which RFC 6350 made optional, and Section 4 defaults PHOTO, LOGO,
// SOUND and KEY to a binary value -- so a 4.0 card with no N and a remote URI
// photo downgraded into a 3.0 card that clients reject or read as a corrupt
// inline image.
func TestDowngradedVCardIsValidForItsVersion(t *testing.T) {
	tests := []struct {
		name        string
		raw         string
		wantLines   []string
		absentLines []string
	}{
		{
			name: "synthesizes the N vCard 3.0 requires",
			raw: buildVCard("4.0",
				"UID:alice",
				"FN:Alice Adams",
			),
			wantLines: []string{"N:Adams;Alice;;;"},
		},
		{
			name: "single-word FN becomes the family name alone",
			raw: buildVCard("4.0",
				"UID:cher",
				"FN:Cher",
			),
			wantLines: []string{"N:Cher;;;;"},
		},
		{
			name: "an existing N is left alone",
			raw: buildVCard("4.0",
				"UID:alice",
				"FN:Alice Adams",
				"N:Adams;Alice;Q;Dr.;PhD",
			),
			wantLines: []string{"N:Adams;Alice;Q;Dr.;PhD"},
		},
		{
			name: "a remote photo URI is marked as one",
			raw: buildVCard("4.0",
				"UID:alice",
				"FN:Alice Adams",
				"N:Adams;Alice;;;",
				"PHOTO:https://example.com/alice.jpg",
			),
			wantLines: []string{"PHOTO;VALUE=uri:https://example.com/alice.jpg"},
		},
		{
			name: "an inline photo still unpacks to base64",
			raw: buildVCard("4.0",
				"UID:alice",
				"FN:Alice Adams",
				"N:Adams;Alice;;;",
				"PHOTO:data:image/jpeg;base64,QUJD",
			),
			wantLines:   []string{"ENCODING=b", "TYPE=JPEG", "QUJD"},
			absentLines: []string{"VALUE=uri"},
		},
		{
			name: "every binary property carrying a URI is marked",
			raw: buildVCard("4.0",
				"UID:alice",
				"FN:Alice Adams",
				"N:Adams;Alice;;;",
				"LOGO:https://example.com/logo.png",
				"SOUND:https://example.com/name.ogg",
				"KEY:https://example.com/alice.asc",
			),
			wantLines: []string{
				"LOGO;VALUE=uri:https://example.com/logo.png",
				"SOUND;VALUE=uri:https://example.com/name.ogg",
				"KEY;VALUE=uri:https://example.com/alice.asc",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := convertVCardVersion(tc.raw, "3.0")
			if !ok {
				t.Fatalf("convertVCardVersion(_, %q) refused a convertible card", "3.0")
			}
			if err := validateVCardForVersion(got, "3.0"); err != nil {
				t.Fatalf("converted card is not valid vCard 3.0: %v\n%s", err, got)
			}
			for _, want := range tc.wantLines {
				if !strings.Contains(got, want) {
					t.Errorf("converted card is missing %q:\n%s", want, got)
				}
			}
			for _, absent := range tc.absentLines {
				if strings.Contains(got, absent) {
					t.Errorf("converted card should not carry %q:\n%s", absent, got)
				}
			}
		})
	}
}

// RFC 6350 Section 6.2.1 makes FN mandatory, and RFC 2426 does not require a
// 3.0 card to carry one. The upgrade owes the same structural repair the
// downgrade does.
func TestUpgradedVCardIsValidForItsVersion(t *testing.T) {
	raw := buildVCard("3.0",
		"UID:alice",
		"N:Adams;Alice;;;",
	)
	got, ok := convertVCardVersion(raw, "4.0")
	if !ok {
		t.Fatal("vCard 3.0 without FN must still upgrade")
	}
	if err := validateVCardForVersion(got, "4.0"); err != nil {
		t.Fatalf("converted card is not valid vCard 4.0: %v\n%s", err, got)
	}
	if !strings.Contains(got, "FN:Alice Adams") {
		t.Errorf("upgrade did not derive FN from N:\n%s", got)
	}
}

// Refusing a card makes the resource invisible to the client -- the sync token
// advances either way, so it is never asked for again. A card that was already
// invalid in its stored version is therefore converted as far as it goes rather
// than refused: the conversion is not what made it invalid, and handing back
// something a client can read beats handing back nothing at all.
func TestConvertVCardVersionDoesNotRefuseAnAlreadyInvalidStoredCard(t *testing.T) {
	// Neither FN nor N, so neither can be derived from the other, and RFC 6350
	// Section 6.2.1 already made this invalid as a 4.0 card.
	raw := buildVCard("4.0", "UID:alice", "EMAIL:alice@example.com")
	got, ok := convertVCardVersion(raw, "3.0")
	if !ok {
		t.Fatal("an already-invalid stored card must still be handed back converted")
	}
	for _, want := range []string{"VERSION:3.0", "UID:alice", "EMAIL:alice@example.com"} {
		if !strings.Contains(got, want) {
			t.Errorf("converted card is missing %q:\n%s", want, got)
		}
	}
}
