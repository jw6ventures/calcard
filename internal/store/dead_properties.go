package store

import (
	"context"
	"database/sql"
	"strings"

	"github.com/lib/pq"
)

type deadPropertyRepo struct {
	pool *sql.DB
}

func (r *deadPropertyRepo) ListByResources(ctx context.Context, resourcePaths []string) ([]DeadProperty, error) {
	if len(resourcePaths) == 0 {
		return []DeadProperty{}, nil
	}
	const q = `
SELECT resource_path, namespace_uri, local_name, inner_xml, created_at, updated_at
FROM dav_dead_properties
WHERE resource_path = ANY($1)
ORDER BY resource_path, namespace_uri, local_name`
	defer observeDB(ctx, "dead_properties.list_by_resources")()
	rows, err := r.pool.QueryContext(ctx, q, pq.Array(resourcePaths))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []DeadProperty
	for rows.Next() {
		var property DeadProperty
		if err := rows.Scan(
			&property.ResourcePath,
			&property.NamespaceURI,
			&property.LocalName,
			&property.InnerXML,
			&property.CreatedAt,
			&property.UpdatedAt,
		); err != nil {
			return nil, err
		}
		result = append(result, property)
	}
	return result, rows.Err()
}

func (r *deadPropertyRepo) Apply(ctx context.Context, resourcePath string, mutations []DeadPropertyMutation) error {
	if len(mutations) == 0 {
		return nil
	}
	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := applyDeadPropertyMutationsTx(ctx, tx, resourcePath, mutations); err != nil {
		return err
	}
	return tx.Commit()
}

func applyDeadPropertyMutationsTx(ctx context.Context, tx execContext, resourcePath string, mutations []DeadPropertyMutation) error {
	resourcePath = strings.TrimSpace(resourcePath)
	if resourcePath == "" {
		return ErrNotFound
	}
	for _, mutation := range mutations {
		if mutation.LocalName == "" {
			return ErrConflict
		}
		if mutation.Remove {
			if _, err := tx.ExecContext(ctx, `DELETE FROM dav_dead_properties WHERE resource_path=$1 AND namespace_uri=$2 AND local_name=$3`, resourcePath, mutation.NamespaceURI, mutation.LocalName); err != nil {
				return err
			}
			continue
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO dav_dead_properties (resource_path, namespace_uri, local_name, inner_xml)
VALUES ($1, $2, $3, $4)
ON CONFLICT (resource_path, namespace_uri, local_name) DO UPDATE SET
    inner_xml=EXCLUDED.inner_xml,
    updated_at=NOW()`, resourcePath, mutation.NamespaceURI, mutation.LocalName, mutation.InnerXML); err != nil {
			return err
		}
	}
	return nil
}
