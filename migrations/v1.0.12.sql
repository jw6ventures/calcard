-- v1.0.12: migrate legacy calendar shares to ACL-backed permissions

INSERT INTO acl_entries (resource_path, principal_href, is_grant, privilege, created_at)
SELECT path.resource_path, path.principal_href, TRUE, privilege_map.privilege, path.created_at
FROM (
    SELECT '/dav/calendars/' || cs.calendar_id::TEXT AS resource_path,
           '/dav/principals/' || cs.user_id::TEXT || '/' AS principal_href,
           cs.editor,
           cs.created_at
    FROM calendar_shares cs
) path
JOIN LATERAL (
    SELECT 'read' AS privilege
    UNION ALL
    SELECT 'read-free-busy'
    UNION ALL
    SELECT 'write' WHERE path.editor
) privilege_map ON TRUE
ON CONFLICT (resource_path, principal_href, privilege, is_grant) DO NOTHING;

DROP INDEX IF EXISTS idx_calendar_shares_user_id;
DROP TABLE IF EXISTS calendar_shares;

UPDATE application SET value = 'v1.0.12' WHERE key = 'version';
