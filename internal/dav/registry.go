package dav

import (
	"net/http"
	"path"
	"strings"
)

// Extension registers DAV behavior into a server registry.
type Extension interface {
	RegisterDAV(*Registry)
}

type MethodHandler func(http.ResponseWriter, *http.Request)
type ReportHandler func(http.ResponseWriter, *http.Request, RequestContext) bool
type PutValidator func(PutValidation) error
type CollectionContributor func(RequestContext) ([]Collection, error)
type PropfindDecorator func(RequestContext, *PropfindProperties) error

type MethodAuthPolicy int

const (
	MethodAuthRequired MethodAuthPolicy = iota
	MethodAuthNone
)

type MethodOptions struct {
	Auth MethodAuthPolicy
}

// Collection is a lightweight collection response that extensions can append
// to PROPFIND discovery results.
type Collection struct {
	Href string
	Name string
}

type routeRegistration struct {
	method  string
	prefix  string
	options MethodOptions
	handler MethodHandler
}

type reportRegistration struct {
	prefix       string
	name         string
	handler      ReportHandler
	overrideCore bool
}

type putValidatorRegistration struct {
	prefix    string
	validator PutValidator
}

type collectionContributorRegistration struct {
	prefix      string
	contributor CollectionContributor
}

type propfindDecoratorRegistration struct {
	prefix    string
	decorator PropfindDecorator
}

// Registry stores default DAV modules and caller-provided extensions.
type Registry struct {
	collections            []string
	methodHandlers         []routeRegistration
	reportHandlers         []reportRegistration
	putValidators          []putValidatorRegistration
	collectionContributors []collectionContributorRegistration
	propfindDecorators     []propfindDecoratorRegistration
}

func NewRegistry() *Registry {
	return &Registry{}
}

func (r *Registry) RegisterCollection(prefix string) {
	prefix = normalizeRegistryPrefix(prefix)
	for _, existing := range r.collections {
		if existing == prefix {
			return
		}
	}
	r.collections = append(r.collections, prefix)
}

func (r *Registry) RegisterMethod(method, prefix string, opts MethodOptions, handler MethodHandler) {
	if handler == nil {
		return
	}
	method = strings.ToUpper(strings.TrimSpace(method))
	if method == "" {
		return
	}
	r.methodHandlers = append(r.methodHandlers, routeRegistration{
		method:  method,
		prefix:  normalizeRegistryPrefix(prefix),
		options: opts,
		handler: handler,
	})
}

func (r *Registry) RegisterReport(prefix, reportName string, handler ReportHandler) {
	r.registerReport(prefix, reportName, handler, false)
}

func (r *Registry) RegisterReportOverride(prefix, reportName string, handler ReportHandler) {
	r.registerReport(prefix, reportName, handler, true)
}

func (r *Registry) registerReport(prefix, reportName string, handler ReportHandler, overrideCore bool) {
	if handler == nil {
		return
	}
	r.reportHandlers = append(r.reportHandlers, reportRegistration{
		prefix:       normalizeRegistryPrefix(prefix),
		name:         strings.TrimSpace(reportName),
		handler:      handler,
		overrideCore: overrideCore,
	})
}

func (r *Registry) RegisterPutValidator(prefix string, validator PutValidator) {
	if validator == nil {
		return
	}
	r.putValidators = append(r.putValidators, putValidatorRegistration{
		prefix:    normalizeRegistryPrefix(prefix),
		validator: validator,
	})
}

func (r *Registry) RegisterCollectionContributor(prefix string, contributor CollectionContributor) {
	if contributor == nil {
		return
	}
	r.collectionContributors = append(r.collectionContributors, collectionContributorRegistration{
		prefix:      normalizeRegistryPrefix(prefix),
		contributor: contributor,
	})
}

func (r *Registry) RegisterPropfindDecorator(prefix string, decorator PropfindDecorator) {
	if decorator == nil {
		return
	}
	r.propfindDecorators = append(r.propfindDecorators, propfindDecoratorRegistration{
		prefix:    normalizeRegistryPrefix(prefix),
		decorator: decorator,
	})
}

func (r *Registry) HasCollection(prefix string) bool {
	prefix = normalizeRegistryPrefix(prefix)
	for _, existing := range r.collections {
		if existing == prefix {
			return true
		}
	}
	return false
}

func (r *Registry) methodHandler(method, requestPath string) (MethodHandler, bool) {
	route, ok := r.methodRoute(method, requestPath)
	if !ok {
		return nil, false
	}
	return route.handler, true
}

func (r *Registry) methodRoute(method, requestPath string) (routeRegistration, bool) {
	method = strings.ToUpper(strings.TrimSpace(method))
	cleanPath := normalizeRegistryPrefix(requestPath)
	defaultPath := isDefaultDAVPath(cleanPath)
	var selected routeRegistration
	found := false
	for _, candidate := range r.methodHandlers {
		if candidate.method != method || !registryPrefixMatch(cleanPath, candidate.prefix) {
			continue
		}
		if defaultPath && isCoreDAVMethod(candidate.method) {
			continue
		}
		if !found || len(candidate.prefix) >= len(selected.prefix) {
			selected = candidate
			found = true
		}
	}
	return selected, found
}

func (r *Registry) RegisteredMethods() []string {
	seen := make(map[string]struct{})
	var methods []string
	for _, candidate := range r.methodHandlers {
		if _, ok := seen[candidate.method]; ok {
			continue
		}
		seen[candidate.method] = struct{}{}
		methods = append(methods, candidate.method)
	}
	return methods
}

func (r *Registry) reportHandler(requestPath, reportName string) (ReportHandler, bool) {
	cleanPath := normalizeRegistryPrefix(requestPath)
	reportName = strings.TrimSpace(reportName)
	requiresOverride := isDefaultDAVPath(cleanPath) && isCoreReportName(reportName)
	var selected reportRegistration
	found := false
	for _, candidate := range r.reportHandlers {
		if requiresOverride && !candidate.overrideCore {
			continue
		}
		if candidate.name != reportName || !registryPrefixMatch(cleanPath, candidate.prefix) {
			continue
		}
		if !found || len(candidate.prefix) >= len(selected.prefix) {
			selected = candidate
			found = true
		}
	}
	return selected.handler, found
}

func (r *Registry) registeredMethodsForPath(requestPath string) []string {
	cleanPath := normalizeRegistryPrefix(requestPath)
	seen := make(map[string]struct{})
	var methods []string
	for _, candidate := range r.methodHandlers {
		if !registryPrefixMatch(cleanPath, candidate.prefix) {
			continue
		}
		if isDefaultDAVPath(cleanPath) && isCoreDAVMethod(candidate.method) {
			continue
		}
		if _, ok := seen[candidate.method]; ok {
			continue
		}
		seen[candidate.method] = struct{}{}
		methods = append(methods, candidate.method)
	}
	return methods
}

func (r *Registry) validatePut(v PutValidation) error {
	cleanPath := normalizeRegistryPrefix(v.Path)
	for _, candidate := range r.putValidators {
		if registryPrefixMatch(cleanPath, candidate.prefix) {
			if err := candidate.validator(v); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Registry) contributeCollections(ctx RequestContext) ([]Collection, error) {
	cleanPath := normalizeRegistryPrefix(ctx.Path)
	collections := r.staticChildCollections(cleanPath)
	for _, candidate := range r.collectionContributors {
		if cleanPath != candidate.prefix {
			continue
		}
		added, err := candidate.contributor(ctx)
		if err != nil {
			return nil, err
		}
		collections = append(collections, added...)
	}
	return collections, nil
}

func (r *Registry) staticChildCollections(parent string) []Collection {
	parent = normalizeRegistryPrefix(parent)
	var collections []Collection
	for _, collection := range r.collections {
		if isDefaultDAVPath(collection) {
			continue
		}
		if normalizeRegistryPrefix(path.Dir(collection)) != parent {
			continue
		}
		href := collection
		if !strings.HasSuffix(href, "/") {
			href += "/"
		}
		collections = append(collections, Collection{
			Href: href,
			Name: path.Base(collection),
		})
	}
	return collections
}

func (r *Registry) registeredExtensionCollection(requestPath string) (Collection, bool) {
	cleanPath := normalizeRegistryPrefix(requestPath)
	for _, collection := range r.collections {
		if isDefaultDAVPath(collection) {
			continue
		}
		if cleanPath != collection {
			continue
		}
		return Collection{
			Href: cleanPath + "/",
			Name: path.Base(cleanPath),
		}, true
	}
	return Collection{}, false
}

func (r *Registry) isExtensionPath(requestPath string) bool {
	cleanPath := normalizeRegistryPrefix(requestPath)
	if isDefaultDAVPath(cleanPath) {
		return false
	}
	for _, collection := range r.collections {
		if isDefaultDAVPath(collection) {
			continue
		}
		if registryPrefixMatch(cleanPath, collection) {
			return true
		}
	}
	return false
}

func (r *Registry) decoratePropfind(ctx RequestContext, props *PropfindProperties) error {
	cleanPath := normalizeRegistryPrefix(ctx.Path)
	for _, candidate := range r.propfindDecorators {
		if !registryPrefixMatch(cleanPath, candidate.prefix) {
			continue
		}
		if err := candidate.decorator(ctx, props); err != nil {
			return err
		}
	}
	return nil
}

func normalizeRegistryPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return "/"
	}
	clean := path.Clean(prefix)
	if !strings.HasPrefix(clean, "/") {
		clean = "/" + clean
	}
	return clean
}

func registryPrefixMatch(requestPath, prefix string) bool {
	if requestPath == prefix {
		return true
	}
	if prefix == "/" {
		return true
	}
	return strings.HasPrefix(requestPath, strings.TrimSuffix(prefix, "/")+"/")
}

func isDefaultDAVPath(requestPath string) bool {
	requestPath = normalizeRegistryPrefix(requestPath)
	switch {
	case requestPath == "/dav":
		return true
	case registryPrefixMatch(requestPath, "/dav/principals"):
		return true
	case registryPrefixMatch(requestPath, "/dav/calendars"):
		return true
	case registryPrefixMatch(requestPath, "/dav/addressbooks"):
		return true
	default:
		return false
	}
}

func isCoreReportName(reportName string) bool {
	switch strings.TrimSpace(reportName) {
	case "calendar-query", "calendar-multiget", "free-busy-query",
		"addressbook-query", "addressbook-multiget",
		"sync-collection", "expand-property":
		return true
	default:
		return false
	}
}

func isCoreDAVMethod(method string) bool {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case http.MethodHead, http.MethodGet, http.MethodOptions, http.MethodPut, http.MethodDelete,
		"PROPFIND", "PROPPATCH", "MKCOL", "MKCALENDAR", "REPORT", "COPY", "MOVE", "LOCK", "UNLOCK", "ACL":
		return true
	default:
		return false
	}
}
