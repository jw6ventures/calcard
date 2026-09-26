package contacts

import (
	"context"
	"errors"
	"fmt"
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
	ImportSkipInvalid            ImportSkipCode = "invalid"
)

// ImportVCards stores every card in a VCF file. A card whose UID the book
// already holds replaces that contact; any other card is created. vCard 2.1
// cards are converted to vCard 3.0. A card that cannot be stored is skipped
// with its reason; an error is returned only when the book cannot be written
// at all or the store fails.
func (s *Service) ImportVCards(ctx context.Context, user *store.User, bookID int64, content string) (ImportResult, error) {
	var result ImportResult
	if _, err := s.loadAddressBookWithPrivilege(ctx, user, bookID, "", "bind"); err != nil {
		return result, err
	}
	cards := vcard.SplitCards(content)
	if len(cards) == 0 {
		return result, fmt.Errorf("%w: no contacts found in file", ErrBadRequest)
	}
	for i, card := range cards {
		card, err := importableCard(card)
		uid := vcard.UID(card)
		if err == nil {
			err = s.importCard(ctx, user, bookID, uid, card)
		}
		switch {
		case err == nil:
			result.Imported++
		case isImportSkip(err):
			result.Skipped = append(result.Skipped, ImportSkip{Card: i + 1, UID: uid, Code: importSkipCode(err), Reason: importSkipReason(err)})
		default:
			return result, err
		}
	}
	return result, nil
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

func isImportSkip(err error) bool {
	return errors.Is(err, ErrBadRequest) || errors.Is(err, ErrConflict) ||
		errors.Is(err, ErrForbidden) || errors.Is(err, ErrPreconditionFailed) ||
		store.IsDataError(err)
}

func importSkipReason(err error) string {
	switch {
	case errors.Is(err, ErrConflict):
		return "another contact already uses this UID or resource name"
	case errors.Is(err, ErrForbidden):
		return "you may not change this contact"
	case errors.Is(err, ErrPreconditionFailed):
		return "the contact changed while it was being imported"
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
	case errors.Is(err, vcard.ErrMissingFN):
		return ImportSkipMissingFN
	case errors.Is(err, vcard.ErrUnsupportedCharset):
		return ImportSkipUnsupportedCharset
	case errors.Is(err, vcard.ErrMalformed), store.IsDataError(err):
		return ImportSkipMalformed
	}
	return ImportSkipInvalid
}
