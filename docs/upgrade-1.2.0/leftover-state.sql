-- State the v1.2.0 resource identity repair left for an operator to review.
--
-- Lists, by finding and path:
--   acl_entries, dav_dead_properties: rows stored at a calendar object's or
--     contact's path where no member is stored. No request reads them, and
--     creating a member under that name removes them.
--   shared state path: paths more than one member owns, one stored without a
--     name, at its UID, and one stored under that name. The two share their
--     ACL entries, locks and dead properties.
--   name resolves elsewhere: members whose stored names hold a "." or ".."
--     segment, an empty one, a trailing "/", a "?", a "#" or a
--     percent-encoded octet, which no request reaches as themselves.
--
-- Entries the repair removed, or withheld from a member that read them at
-- another spelling of its name, are kept in acl_entries_v120_removed when
-- there were any.
SELECT 'acl_entries' AS finding, e.resource_path, count(*) AS rows
FROM acl_entries e
WHERE e.resource_path ~ '^/dav/(calendars|addressbooks)/[0-9]+/.'
  AND NOT EXISTS (SELECT 1 FROM events WHERE object_acl_path = e.resource_path)
  AND NOT EXISTS (SELECT 1 FROM contacts WHERE object_acl_path = e.resource_path)
GROUP BY e.resource_path
UNION ALL
SELECT 'dav_dead_properties', p.resource_path, count(*)
FROM dav_dead_properties p
WHERE p.resource_path ~ '^/dav/(calendars|addressbooks)/[0-9]+/.'
  AND NOT EXISTS (SELECT 1 FROM events WHERE object_acl_path = p.resource_path)
  AND NOT EXISTS (SELECT 1 FROM contacts WHERE object_acl_path = p.resource_path)
GROUP BY p.resource_path
UNION ALL
SELECT 'shared state path', member.object_acl_path, count(*)
FROM (SELECT object_acl_path FROM events UNION ALL SELECT object_acl_path FROM contacts) AS member
GROUP BY member.object_acl_path
HAVING count(*) > 1
UNION ALL
SELECT 'name resolves elsewhere', member.object_acl_path, count(*)
FROM (
    SELECT object_acl_path, COALESCE(NULLIF(resource_name, ''), uid) AS name FROM events
    UNION ALL
    SELECT object_acl_path, COALESCE(NULLIF(resource_name, ''), uid) FROM contacts
) AS member
WHERE member.name ~ '[?#]|%[0-9A-Fa-f]{2}'
   OR ('/' || regexp_replace(member.name, '[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]+$', '') || '/') ~ '//|/\.\.?/'
GROUP BY member.object_acl_path
ORDER BY 1, 2;
