package contacts

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/jw6ventures/calcard/internal/store"
)

func newContactsPostgresStore(t *testing.T) *store.Store {
	t.Helper()
	dsn := os.Getenv("CALCARD_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("CALCARD_TEST_POSTGRES_DSN is unset; set it to run the PostgreSQL import test")
	}
	schema := fmt.Sprintf("calcard_contacts_test_%d", time.Now().UnixNano())
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		admin.Close()
		t.Fatalf("create schema %s: %v", schema, err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		admin.Close()
		t.Fatalf("parse CALCARD_TEST_POSTGRES_DSN: %v", err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	pool, err := sql.Open("postgres", parsed.String())
	if err != nil {
		admin.Close()
		t.Fatalf("open schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		pool.Close()
		if _, err := admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); err != nil {
			t.Errorf("drop schema %s: %v", schema, err)
		}
		admin.Close()
	})
	schemaSQL, err := os.ReadFile(filepath.Join("..", "..", "db.sql"))
	if err != nil {
		t.Fatalf("read db.sql: %v", err)
	}
	if _, err := pool.Exec(string(schemaSQL)); err != nil {
		t.Fatalf("apply db.sql: %v", err)
	}
	pool.SetMaxOpenConns(5)
	return store.New(pool)
}

// incompressibleText is n octets PostgreSQL cannot compress below its btree
// entry limit, as a real value of that length would be.
func incompressibleText(n int) string {
	var b strings.Builder
	seed := sha256.Sum256([]byte("calcard"))
	for b.Len() < n {
		b.WriteString(hex.EncodeToString(seed[:]))
		seed = sha256.Sum256(seed[:])
	}
	return b.String()[:n]
}

// A card whose values PostgreSQL's indexes cannot hold costs that card, never
// the rest of the import.
func TestPostgres_ImportContinuesPastOversizedValues(t *testing.T) {
	database := newContactsPostgresStore(t)
	ctx := t.Context()
	user, err := database.Users.UpsertOAuthUser(ctx, "import-limits", "import@example.test", "Import", "Test")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	book, err := database.AddressBooks.Create(ctx, store.AddressBook{UserID: user.ID, Name: "Import"})
	if err != nil {
		t.Fatalf("create address book: %v", err)
	}
	long := incompressibleText(4096)
	card := func(uid, fn string) string {
		return "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:" + uid + "\r\nFN:" + fn + "\r\nEND:VCARD\r\n"
	}
	file := card("a", "A") + card("long-fn", long) + card(long, "Long UID") + card("c", "C")
	result, err := NewService(database).ImportVCards(ctx, user, book.ID, file)
	if err != nil {
		t.Fatalf("ImportVCards aborted: %v", err)
	}
	if result.Imported != 3 || len(result.Skipped) != 1 || result.Skipped[0].Card != 3 {
		t.Fatalf("result = %+v, want a, the long FN and c imported and the long UID skipped", result)
	}
	for _, uid := range []string{"a", "long-fn", "c"} {
		c, err := database.Contacts.GetByUID(ctx, book.ID, uid)
		if err != nil || c == nil {
			t.Fatalf("contact %q not stored: %v", uid, err)
		}
		if uid == "long-fn" && !strings.Contains(strings.ReplaceAll(c.RawVCard, "\r\n ", ""), long) {
			t.Fatal("the long FN was not kept whole in the card")
		}
	}
}
