package dav

import (
	"fmt"

	"github.com/jw6ventures/calcard/internal/vcard"
)

// extractUIDFromVCard returns the card's UID, parameters and group allowed.
func extractUIDFromVCard(vcardData string) (string, error) {
	structure := vcard.ReadStructure(vcardData)
	if structure.UIDCount == 0 {
		return "", fmt.Errorf("no UID property found in vCard data")
	}
	uid := vcard.UID(vcardData)
	if uid == "" {
		return "", fmt.Errorf("empty UID property")
	}
	return uid, nil
}

func validateVCardEnvelope(data string) error {
	return vcard.ValidateEnvelope(data)
}

func (h *DavServer) validateVCard(data string) error {
	return vcard.Validate(data, true)
}

// validateVCardForVersion reports whether data is a valid card of exactly the
// named version. It checks the version conversion's own output and is stricter
// than validateVCard: a card this server produces has no author to blame.
//
// RFC 2426 Section 3.1.2 makes N mandatory in vCard 3.0; RFC 6350 Section 6.2.2
// makes it optional in 4.0, while Section 6.2.1 keeps FN mandatory.
func validateVCardForVersion(data, version string) error {
	if err := vcard.ValidateEnvelope(data); err != nil {
		return err
	}

	structure := vcard.ReadStructure(data)
	if len(structure.Versions) != 1 {
		return fmt.Errorf("VCARD must contain exactly one VERSION")
	}
	if structure.Versions[0] != version {
		return fmt.Errorf("VCARD declares version %q, want %q", structure.Versions[0], version)
	}
	if !structure.HasFN {
		return fmt.Errorf("vCard %s requires FN", version)
	}
	if version == "3.0" && !structure.HasN {
		return fmt.Errorf("vCard 3.0 requires N")
	}
	return nil
}
