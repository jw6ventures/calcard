// Package deploy holds the guards on this repository's deployment assets. The
// chart is rendered by helm rather than modelled in Go, so the only way to
// assert what a release actually creates is to run the renderer and read what
// it produced.
package deploy

import (
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// chartPath is the chart these tests render, relative to this package.
const chartPath = "../../deploy/helm/calcard"

// baseURLHost is the host the ingress must derive from app.baseUrl when
// ingress.host is left empty. It is deliberately not the host the CI install
// uses, so a template that hard-coded one could not pass.
const baseURLHost = "calcard.example.test"

// ciValues is the value set the deployment job installs with. The tests render
// the same set so that a chart change which only breaks the CI install is
// caught here, before the cluster is built.
func ciValues() []string {
	return []string{
		"--set", "image.repository=calcard",
		"--set", "image.tag=test",
		"--set", "image.pullPolicy=IfNotPresent",
		"--set", "app.baseUrl=https://" + baseURLHost,
		"--set", "app.trustedProxies=10.244.0.0/16",
		"--set", "app.oauth.clientId=test-client",
		"--set", "app.oauth.clientSecret=test-client-secret",
		"--set", "app.oauth.discoveryUrl=http://oidc-mock.default.svc.cluster.local/.well-known/openid-configuration",
		"--set", "app.sessionSecret=this-is-a-very-long-session-secret-32chars",
		"--set", "app.db.user=postgres",
		"--set", "app.db.password=postgres",
		"--set", "app.db.name=app",
		"--set", "postgres.enabled=true",
		"--set", "postgres.persistence.enabled=false",
	}
}

// helmBinary returns the helm executable, skipping when the host has none. The
// chart is verified by the real renderer or not at all; CI installs helm, so
// the skip is a developer-machine convenience rather than a way for the gate to
// pass without running.
func helmBinary(t *testing.T) string {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed; install it to run the chart render tests")
	}
	return helm
}

func runHelm(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(helmBinary(t), args...)
	// helm writes coverage of missing required values to stderr, so both
	// streams are needed to explain a failure.
	output, err := cmd.CombinedOutput()
	return string(output), err
}

// renderChart runs helm template over the CI value set and returns each
// rendered document keyed by the template that produced it.
func renderChart(t *testing.T, extra ...string) map[string]string {
	t.Helper()
	args := append([]string{"template", "calcard-test", chartPath}, ciValues()...)
	args = append(args, extra...)
	output, err := runHelm(t, args...)
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}
	return splitRenderedDocuments(output)
}

// splitRenderedDocuments indexes a helm render by the "# Source:" comment helm
// writes ahead of each document. A template rendering nothing contributes no
// document, which is what lets a test assert that a block is absent.
func splitRenderedDocuments(rendered string) map[string]string {
	documents := make(map[string]string)
	source := ""
	var body strings.Builder
	flush := func() {
		if source != "" {
			documents[source] = body.String()
		}
		source = ""
		body.Reset()
	}
	for _, line := range strings.Split(rendered, "\n") {
		if strings.TrimSpace(line) == "---" {
			flush()
			continue
		}
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "# Source:"); ok {
			source = strings.TrimSpace(rest)
			continue
		}
		body.WriteString(line)
		body.WriteString("\n")
	}
	flush()
	return documents
}

func requireDocument(t *testing.T, documents map[string]string, template string) string {
	t.Helper()
	document, ok := documents["calcard/templates/"+template]
	if !ok {
		t.Fatalf("chart rendered no %s; it rendered %v", template, renderedTemplates(documents))
	}
	return document
}

// renderedTemplates names what the chart did render, which is what makes a
// missing template's failure readable.
func renderedTemplates(documents map[string]string) []string {
	names := make([]string, 0, len(documents))
	for name := range documents {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// hasLine reports whether the document carries want as a line of its own,
// ignoring indentation. Comparing the trimmed line rather than a substring
// keeps "type: ClusterIP" from being satisfied by a comment mentioning it, and
// keeps the test from breaking when a template's indentation moves.
func hasLine(document, want string) bool {
	for _, line := range strings.Split(document, "\n") {
		if strings.TrimSpace(line) == want {
			return true
		}
	}
	return false
}

// blockUnder returns the lines nested beneath the first line whose trimmed form
// is key, so an assertion can be scoped to one part of a document rather than
// matching anywhere in it.
func blockUnder(document, key string) (string, bool) {
	lines := strings.Split(document, "\n")
	for i, line := range lines {
		if strings.TrimSpace(line) != key {
			continue
		}
		indent := len(line) - len(strings.TrimLeft(line, " "))
		var block strings.Builder
		for _, nested := range lines[i+1:] {
			if strings.TrimSpace(nested) == "" {
				continue
			}
			if len(nested)-len(strings.TrimLeft(nested, " ")) <= indent {
				break
			}
			block.WriteString(nested)
			block.WriteString("\n")
		}
		return block.String(), true
	}
	return "", false
}

func TestHelmChartLints(t *testing.T) {
	args := append([]string{"lint", chartPath}, ciValues()...)
	if output, err := runHelm(t, args...); err != nil {
		t.Fatalf("helm lint failed: %v\n%s", err, output)
	}
}

// TestHelmChartRendersEveryTemplate pins the set of objects a release creates,
// so a template that stops rendering -- an `if` whose value moved, say -- fails
// here rather than in a cluster that silently lost an object.
func TestHelmChartRendersEveryTemplate(t *testing.T) {
	documents := renderChart(t)
	for _, template := range []string{
		"app-deployment.yaml",
		"app-service.yaml",
		"configmap.yaml",
		"db-secret.yaml",
		"ingress.yaml",
		"postgres-configmap.yaml",
		"postgres-service.yaml",
		"postgres-statefulset.yaml",
		"secret.yaml",
	} {
		requireDocument(t, documents, template)
	}
}

// TestHelmChartRequiresABaseURL guards the derive path below: the ingress host
// falls back to app.baseUrl, so a release without one must fail to render
// rather than publish an ingress routing an empty host.
func TestHelmChartRequiresABaseURL(t *testing.T) {
	args := append([]string{"template", "calcard-test", chartPath}, ciValues()...)
	args = append(args, "--set", "app.baseUrl=")
	output, err := runHelm(t, args...)
	if err == nil {
		t.Fatalf("chart rendered without app.baseUrl:\n%s", output)
	}
	if !strings.Contains(output, "app.baseUrl is required") {
		t.Fatalf("expected the render to name app.baseUrl, got:\n%s", output)
	}
}

// TestHelmChartKeepsTheApplicationServiceInternal is the deployment half of
// RFC 4791 §11: the application answers HTTP Basic only over a secure
// transport, which holds only while every route to it runs through the
// TLS-terminating ingress. A NodePort, a LoadBalancer or a hostPort would
// publish the cleartext container port beside that ingress.
func TestHelmChartKeepsTheApplicationServiceInternal(t *testing.T) {
	documents := renderChart(t)
	service := requireDocument(t, documents, "app-service.yaml")
	if !hasLine(service, "type: ClusterIP") {
		t.Fatalf("application Service is not ClusterIP:\n%s", service)
	}
	for template, document := range documents {
		for _, forbidden := range []string{"NodePort", "LoadBalancer", "nodePort:", "hostPort:", "externalIPs:"} {
			if strings.Contains(document, forbidden) {
				t.Errorf("%s exposes the application outside the cluster (%q):\n%s", template, forbidden, document)
			}
		}
	}
}

// TestHelmChartIngressTerminatesTLSOnTheBaseURLHost covers the other half: the
// one published route carries a TLS block for exactly the host the application
// is addressed by, so the transport the Basic gate requires is the transport
// clients actually reach.
func TestHelmChartIngressTerminatesTLSOnTheBaseURLHost(t *testing.T) {
	documents := renderChart(t)
	ingress := requireDocument(t, documents, "ingress.yaml")

	rules, ok := blockUnder(ingress, "rules:")
	if !ok {
		t.Fatalf("ingress declares no rules:\n%s", ingress)
	}
	if !hasLine(rules, `- host: "`+baseURLHost+`"`) {
		t.Fatalf("ingress does not route the app.baseUrl host %q:\n%s", baseURLHost, ingress)
	}

	tls, ok := blockUnder(ingress, "tls:")
	if !ok {
		t.Fatalf("ingress declares no TLS block:\n%s", ingress)
	}
	if !hasLine(tls, `- "`+baseURLHost+`"`) {
		t.Fatalf("ingress TLS block does not cover the app.baseUrl host %q:\n%s", baseURLHost, ingress)
	}
}

// TestHelmChartIngressPrefersAnExplicitHost keeps the derive above from being
// the only path: an operator whose public host differs from app.baseUrl sets
// ingress.host, and both the rule and the TLS block have to follow it.
func TestHelmChartIngressPrefersAnExplicitHost(t *testing.T) {
	const explicit = "dav.example.test"
	documents := renderChart(t, "--set", "ingress.host="+explicit, "--set", "ingress.tls.secretName=calcard-tls")
	ingress := requireDocument(t, documents, "ingress.yaml")

	if strings.Contains(ingress, baseURLHost) {
		t.Fatalf("ingress kept the app.baseUrl host after ingress.host was set:\n%s", ingress)
	}
	rules, _ := blockUnder(ingress, "rules:")
	if !hasLine(rules, `- host: "`+explicit+`"`) {
		t.Fatalf("ingress does not route ingress.host %q:\n%s", explicit, ingress)
	}
	tls, ok := blockUnder(ingress, "tls:")
	if !ok {
		t.Fatalf("ingress declares no TLS block:\n%s", ingress)
	}
	if !hasLine(tls, `- "`+explicit+`"`) {
		t.Fatalf("ingress TLS block does not cover ingress.host %q:\n%s", explicit, ingress)
	}
	if !hasLine(tls, `secretName: "calcard-tls"`) {
		t.Fatalf("ingress TLS block does not name the configured secret:\n%s", ingress)
	}
}

// TestHelmChartIngressCanBeDisabled covers the deployment that fronts CalCard
// with an ingress of its own: the chart must then create none rather than a
// second route to the same Service.
func TestHelmChartIngressCanBeDisabled(t *testing.T) {
	documents := renderChart(t, "--set", "ingress.enabled=false")
	if document, ok := documents["calcard/templates/ingress.yaml"]; ok {
		t.Fatalf("ingress.enabled=false still rendered an Ingress:\n%s", document)
	}
}

// TestHelmChartCarriesTrustedProxies pins the setting the transport check
// depends on behind a terminating proxy: without it every peer's
// X-Forwarded-Proto is believed, and a cleartext client can claim https.
func TestHelmChartCarriesTrustedProxies(t *testing.T) {
	documents := renderChart(t)
	configmap := requireDocument(t, documents, "configmap.yaml")
	if !hasLine(configmap, `APP_TRUSTED_PROXIES: "10.244.0.0/16"`) {
		t.Fatalf("configmap does not carry app.trustedProxies:\n%s", configmap)
	}
}

// TestHelmChartDAVLimitsReachTheConfigMap pins the wiring of the resource
// bounds an operator tunes, including that an explicit 0 -- which turns a limit
// off -- reaches the server rather than being dropped as an empty value.
func TestHelmChartDAVLimitsReachTheConfigMap(t *testing.T) {
	documents := renderChart(t,
		"--set", "app.dav.maxFilterElements=25",
		"--set", "app.dav.maxReportCandidateRows=0",
	)
	configmap := requireDocument(t, documents, "configmap.yaml")
	for _, want := range []string{
		`APP_DAV_MAX_FILTER_ELEMENTS: "25"`,
		`APP_DAV_MAX_REPORT_CANDIDATE_ROWS: "0"`,
	} {
		if !hasLine(configmap, want) {
			t.Errorf("configmap is missing %s:\n%s", want, configmap)
		}
	}
	if hasLine(configmap, `APP_DAV_MAX_MULTIGET_HREFS: ""`) {
		t.Errorf("an unset limit reached the server as an empty value:\n%s", configmap)
	}
}

// TestChartPathExists keeps a moved chart from turning every test above into a
// helm error that reads like a chart defect.
func TestChartPathExists(t *testing.T) {
	if _, err := os.Stat(filepath.Join(chartPath, "Chart.yaml")); err != nil {
		t.Fatalf("chart not found at %s: %v", chartPath, err)
	}
}

// TestHelmChartRefusesAPublishedServiceBesideTheIngress is what makes the check
// above a guard rather than a restatement of the default: ClusterIP being the
// default value is not the same as the chart declining to publish the cleartext
// port, and RFC 4791 §11 needs the second.
func TestHelmChartRefusesAPublishedServiceBesideTheIngress(t *testing.T) {
	for _, published := range []string{"NodePort", "LoadBalancer"} {
		t.Run(published, func(t *testing.T) {
			args := append([]string{"template", "calcard-test", chartPath}, ciValues()...)
			args = append(args, "--set", "service.type="+published)
			output, err := runHelm(t, args...)
			if err == nil {
				t.Fatalf("chart rendered a %s Service beside the ingress:\n%s", published, output)
			}
			if !strings.Contains(output, "service.type must be ClusterIP") {
				t.Fatalf("expected the render to name the constraint, got:\n%s", output)
			}
		})
	}
}

// An operator terminating TLS in front of CalCard turns the chart's ingress
// off, and the Service type is then theirs to choose: the rule is about a
// cleartext route published beside the chart's own TLS one, not about the
// Service type on its own.
func TestHelmChartAllowsAPublishedServiceWithoutTheIngress(t *testing.T) {
	documents := renderChart(t, "--set", "ingress.enabled=false", "--set", "service.type=LoadBalancer")
	service := requireDocument(t, documents, "app-service.yaml")
	if !hasLine(service, "type: LoadBalancer") {
		t.Fatalf("service.type was not honoured with the ingress disabled:\n%s", service)
	}
}
