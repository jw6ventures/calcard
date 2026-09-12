package dav

import (
	"context"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

// collectBoundedPages drains a keyset-paged repository read into one slice
// under a report's candidate-row budget: a page at a time, so the resident cost
// is a page rather than the whole collection, and past the budget the report
// cannot answer over the complete set, which is errTooManyCandidateRows. It is
// for the reports that need the complete set before they can answer;
// calendarQuery pages against the same budget inline instead, since it stops on
// the response count as it builds.
//
// page returns at most multistatusPageSize rows following afterID, and id reads
// the keyset column off one row. A short page ends the read, as does one whose
// last id fails to advance -- a repository answering with rows it has already
// returned would otherwise loop forever.
func collectBoundedPages[T any](ctx context.Context, rowLimit int, page func(ctx context.Context, afterID int64) ([]T, error), id func(T) int64) ([]T, error) {
	var rows []T
	afterID := int64(0)
	for {
		batch, err := page(ctx, afterID)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			return rows, nil
		}
		if len(rows)+len(batch) > rowLimit {
			return nil, errTooManyCandidateRows
		}
		rows = append(rows, batch...)

		lastID := id(batch[len(batch)-1])
		if lastID <= afterID || len(batch) < multistatusPageSize {
			return rows, nil
		}
		afterID = lastID
	}
}

// listBoundedDeletedResources reads the tombstones an incremental sync reports
// as removals, under a budget of its own, equal to the one the collection's own
// rows are read with. The client's sync token is the only narrowing and nothing
// prunes the table it selects from, so without this the resident cost of a sync
// grows for the life of the deployment. The budget bounds the rows the read
// returns, not the rows the database examines reaching them.
func (h *DavServer) listBoundedDeletedResources(ctx context.Context, resourceType string, collectionID int64, since time.Time) ([]store.DeletedResource, error) {
	return collectBoundedPages(ctx, h.reportCandidateRowLimit(),
		func(ctx context.Context, afterID int64) ([]store.DeletedResource, error) {
			return h.store.DeletedResources.ListDeletedSincePageAfter(ctx, resourceType, collectionID, afterID, since, multistatusPageSize)
		}, deletedResourceID)
}

func eventID(event store.Event) int64 { return event.ID }

func contactID(contact store.Contact) int64 { return contact.ID }

func deletedResourceID(deleted store.DeletedResource) int64 { return deleted.ID }
