package contacts

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/vcard"
)

// ImportResult reports a VCF import: how many cards were stored and why each
// other card was not.
type ImportResult struct {
	Imported int
	Skipped  []ImportSkip
}

// ImportSkip is one card an import did not store. Card is its 1-based position
// in the file; UID is empty when the card carries none.
type ImportSkip struct {
	Card   int
	UID    string
	Code   ImportSkipCode
	Reason string
}

// ImportSkipCode classifies why a card was skipped. Reason is the detail, and
// may quote the file; Code never does.
type ImportSkipCode string

const (
	ImportSkipMissingFN          ImportSkipCode = "missing-fn"
	ImportSkipUnsupportedCharset ImportSkipCode = "unsupported-charset"
	ImportSkipMalformed          ImportSkipCode = "malformed"
	ImportSkipDuplicateUID       ImportSkipCode = "duplicate-uid"
	ImportSkipForbidden          ImportSkipCode = "forbidden"
	ImportSkipChanged            ImportSkipCode = "changed"
	ImportSkipLocked             ImportSkipCode = "locked"
	ImportSkipInvalid            ImportSkipCode = "invalid"
)

// importBatchSize is how many cards one transaction stores: the most the store
// takes in a batch.
const importBatchSize = store.MaxContactObjectBatch

// ImportVCards stores every card in a VCF file. A card whose UID the book
// already holds replaces that contact; any other card is created. vCard 2.1
// cards are converted to vCard 3.0. A card that cannot be stored is skipped
// with its reason; an error is returned only when the book cannot be written
// at all or the store fails.
//
// Cards are stored a batch to a transaction, each as a CardDAV PUT of it would
// be: checked against the ACL and the locks, and given its own ETag and sync
// change. A card the store refuses costs that card alone. A card whose
// contact changed while the batch was being stored is decided and stored again
// by itself.
func (s *Service) ImportVCards(ctx context.Context, user *store.User, bookID int64, content string) (ImportResult, error) {
	var result ImportResult
	book, err := s.loadAddressBookWithPrivilege(ctx, user, bookID, "", "bind")
	if err != nil {
		return result, err
	}
	cards := vcard.SplitCards(content)
	if len(cards) == 0 {
		return result, fmt.Errorf("%w: no contacts found in file", ErrBadRequest)
	}
	batch := importBatch{service: s, user: user, book: book, result: &result}
	for i, card := range cards {
		card, err := importableCard(card)
		uid := vcard.UID(card)
		if err == nil && batch.holds(uid) {
			if err := batch.flush(ctx); err != nil {
				return result, err
			}
		}
		var write store.ContactObjectWrite
		if err == nil {
			write, err = s.importWrite(ctx, user, book, uid, card)
		}
		if err != nil {
			if err := batch.skip(i+1, uid, err); err != nil {
				return result, err
			}
			continue
		}
		batch.add(importedCard{position: i + 1, uid: uid, card: card, write: write})
		if len(batch.cards) == importBatchSize {
			if err := batch.flush(ctx); err != nil {
				return result, err
			}
		}
	}
	if err := batch.flush(ctx); err != nil {
		return result, err
	}
	slices.SortStableFunc(result.Skipped, func(a, b ImportSkip) int { return a.Card - b.Card })
	return result, nil
}

// importedCard is a card of the file decided into the write that stores it.
type importedCard struct {
	position int
	uid      string
	card     string
	write    store.ContactObjectWrite
}

// importBatch collects the decided cards of an import until they are stored in
// one transaction.
type importBatch struct {
	service *Service
	user    *store.User
	book    *store.AddressBook
	result  *ImportResult
	cards   []importedCard
	uids    map[string]struct{}
}

func (b *importBatch) add(card importedCard) {
	if b.uids == nil {
		b.uids = map[string]struct{}{}
	}
	b.uids[card.write.UID] = struct{}{}
	b.cards = append(b.cards, card)
}

// holds reports whether a card of the batch has the UID. A later card with it
// is decided only after the batch is stored, so that it is decided against the
// contact the earlier card made and replaces it, as when cards are stored one
// at a time.
func (b *importBatch) holds(uid string) bool {
	_, held := b.uids[uid]
	return held
}

// skip records the card as skipped for err, or returns err when it is the
// store failing rather than a refusal of the card.
func (b *importBatch) skip(position int, uid string, err error) error {
	if !isImportSkip(err) {
		return err
	}
	b.result.Skipped = append(b.result.Skipped, ImportSkip{Card: position, UID: uid, Code: importSkipCode(err), Reason: importSkipReason(err)})
	return nil
}

// flush stores the collected cards in one transaction. A card the store
// refuses for want of a lock or because it cannot hold its values is skipped;
// one that lost a race with another writer is decided and stored again alone.
func (b *importBatch) flush(ctx context.Context) error {
	cards := b.cards
	b.cards, b.uids = nil, nil
	if len(cards) == 0 {
		return nil
	}
	writes := make([]store.ContactObjectWrite, len(cards))
	for i, card := range cards {
		writes[i] = card.write
	}
	outcomes, err := b.service.store.PutContactObjects(ctx, writes)
	if err != nil && !errors.Is(err, store.ErrResourceStateChanged) {
		return err
	}
	for i, card := range cards {
		refusal := store.ErrResourceStateChanged
		if outcomes != nil {
			refusal = outcomes[i].Err
		}
		refusal = contactWriteError(refusal)
		if refusal != nil && !errors.Is(refusal, ErrLocked) && !store.IsDataError(refusal) {
			refusal = b.service.importCard(ctx, b.user, b.book.ID, card.uid, card.card)
			if errors.Is(refusal, ErrChangedConcurrently) {
				refusal = b.service.settleChangedImport(ctx, b.user, b.book, card.uid, card.card)
			}
		}
		if refusal == nil {
			b.result.Imported++
			continue
		}
		if err := b.skip(card.position, card.uid, refusal); err != nil {
			return err
		}
	}
	return nil
}

// importWrite decides what storing the card does, as importCard does: it
// replaces the contact holding the card's UID, or creates one.
func (s *Service) importWrite(ctx context.Context, user *store.User, book *store.AddressBook, uid, card string) (store.ContactObjectWrite, error) {
	input := UpsertInput{RawVCard: card}
	if uid != "" {
		write, err := s.updateWrite(ctx, user, book, uid, input)
		if !errors.Is(err, ErrNotFound) {
			return write, err
		}
	}
	body, createdUID, err := normalizeVCardPayload(input, "", "")
	if err != nil {
		if accessErr := s.requireBookPrivilege(ctx, user, book, "", "bind"); accessErr != nil {
			return store.ContactObjectWrite{}, accessErr
		}
		return store.ContactObjectWrite{}, err
	}
	return s.createWrite(ctx, user, book, createdUID, body, input)
}

// importableCard converts a vCard 2.1 card first, so the card is identified
// by the UID it will be stored under.
func importableCard(card string) (string, error) {
	if err := vcard.CheckOctets(card); err != nil {
		return card, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}
	if _, err := vcard.Inspect(card, false); err == nil || vcard.Version(card) != "2.1" {
		return card, nil
	}
	converted, err := vcard.Convert21To30(card)
	if err != nil {
		return card, fmt.Errorf("%w: vCard 2.1: %w", ErrBadRequest, err)
	}
	return converted, nil
}

// importCard replaces the contact holding the card's UID, or creates one. A
// contact created concurrently under that UID is then replaced as well.
func (s *Service) importCard(ctx context.Context, user *store.User, bookID int64, uid, card string) error {
	input := UpsertInput{RawVCard: card}
	if uid != "" {
		_, _, err := s.UpdateContact(ctx, user, bookID, uid, input)
		if !errors.Is(err, ErrNotFound) {
			return err
		}
	}
	_, _, err := s.CreateContact(ctx, user, bookID, input)
	if uid != "" && errors.Is(err, ErrConflict) {
		if _, _, updateErr := s.UpdateContact(ctx, user, bookID, uid, input); !errors.Is(updateErr, ErrNotFound) {
			return updateErr
		}
	}
	return err
}

// settleChangedImport answers for a card whose write kept losing while the
// import retried it, as the calendar import answers for an event another
// writer stored first: nil when the contact now holds what storing the card
// would leave and the user may write it, the refusal a fresh decision of the
// card meets, ErrConflict when the name a new contact would take is held by
// one of another UID, which refuses every create of the card, or else
// ErrPreconditionFailed.
func (s *Service) settleChangedImport(ctx context.Context, user *store.User, book *store.AddressBook, uid, card string) error {
	if uid == "" {
		return ErrPreconditionFailed
	}
	write, err := s.importWrite(ctx, user, book, uid, card)
	if err != nil {
		return err
	}
	stored, err := s.store.Contacts.GetByUID(ctx, book.ID, uid)
	if err != nil {
		return err
	}
	if stored != nil && stored.RawVCard == write.RawVCard {
		return nil
	}
	holder, err := s.store.Contacts.GetByResourceName(ctx, book.ID, write.ResourceName)
	if err != nil {
		return err
	}
	if holder != nil && holder.UID != uid {
		return ErrConflict
	}
	return ErrPreconditionFailed
}

func isImportSkip(err error) bool {
	return errors.Is(err, ErrBadRequest) || errors.Is(err, ErrConflict) ||
		errors.Is(err, ErrForbidden) || errors.Is(err, ErrPreconditionFailed) ||
		errors.Is(err, ErrLocked) || store.IsDataError(err)
}

func importSkipReason(err error) string {
	switch {
	case errors.Is(err, ErrConflict):
		return "another contact already uses this UID or resource name"
	case errors.Is(err, ErrForbidden):
		return "you may not change this contact"
	case errors.Is(err, ErrPreconditionFailed):
		return "the contact changed while it was being imported"
	case errors.Is(err, ErrLocked):
		return "a WebDAV client has locked this contact or the address book"
	case store.IsDataError(err):
		return "the card holds a value the server cannot store"
	}
	return strings.TrimPrefix(err.Error(), ErrBadRequest.Error()+": ")
}

func importSkipCode(err error) ImportSkipCode {
	switch {
	case errors.Is(err, ErrConflict):
		return ImportSkipDuplicateUID
	case errors.Is(err, ErrForbidden):
		return ImportSkipForbidden
	case errors.Is(err, ErrPreconditionFailed):
		return ImportSkipChanged
	case errors.Is(err, ErrLocked):
		return ImportSkipLocked
	case errors.Is(err, vcard.ErrMissingFN):
		return ImportSkipMissingFN
	case errors.Is(err, vcard.ErrUnsupportedCharset):
		return ImportSkipUnsupportedCharset
	case errors.Is(err, vcard.ErrMalformed), store.IsDataError(err):
		return ImportSkipMalformed
	}
	return ImportSkipInvalid
}
