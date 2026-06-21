// Package dav implements the WebDAV, CalDAV (RFC 4791) and CardDAV (RFC 6352)
// server for calcard, including scheduling free/busy (RFC 6638) and WebDAV ACL.
//
// # Entry point
//
// [DavServer] is the package's HTTP entry point. Construct it with
// [NewDavServer] and mount it as an http.Handler; its ServeHTTP dispatches each
// WebDAV/CalDAV/CardDAV method to the matching handler. Core methods are
// implemented as methods on *DavServer.
//
// # Extensibility
//
// Beyond the built-in modules, behaviour is pluggable through a [Registry]:
// callers pass [Extension] values in [Options], and each extension registers
// additional methods, REPORTs, PUT validators, collection contributors and
// PROPFIND decorators (see registry.go). ServeHTTP falls back to
// handleRegisteredMethod for any method it does not handle natively.
//
// # File layout
//
// Request handling is split by HTTP method / concern:
//
//	server.go             DavServer, ServeHTTP dispatch, construction
//	registry.go           Extension/Registry plugin model
//	options.go            OPTIONS, HEAD
//	get.go                GET
//	put.go                PUT
//	delete.go             DELETE
//	mkcol.go              MKCOL, MKCALENDAR
//	proppatch.go          PROPPATCH
//	propfind.go           PROPFIND
//	propfind_props.go     PROPFIND property serialization
//	propfind_responses.go PROPFIND/principal response building
//	report.go             REPORT dispatch
//	report_calendar.go    calendar REPORTs, filters, free/busy
//	report_addressbook.go address-book REPORTs
//	copymove.go           COPY, MOVE
//	lock.go               LOCK, UNLOCK
//	acl.go                ACL method and decision helpers
//
// Domain logic lives in parallel calendar/address-book files that share a set of
// primitives. calendar_access.go and addressbook_access.go hold loading,
// privilege and ACL-prefetch logic; the shared building blocks they delegate to
// (collection/object path builders, the ACL prefetch sweep and the privilege
// decision core) live in dav_paths.go and the cross-domain helpers in
// calendar_access.go.
package dav
