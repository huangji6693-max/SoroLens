package buildinfo_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/sorotrail/sorolens/internal/buildinfo"
)

// ldflagTargetRe extracts -X <importpath>.<symbol>=... targets from build files.
var ldflagTargetRe = regexp.MustCompile(`-X\s+(github\.com/sorotrail/sorolens/internal/buildinfo\.[A-Za-z0-9_]+)=`)

// TestReleasePipelineLdflagsMatchBuildinfoSymbols verifies that the -X symbol
// paths declared in .goreleaser.yaml, Dockerfile, and Makefile match the actual
// exported package variables in internal/buildinfo. Go's linker silently
// ignores a -X flag whose target symbol does not exist, so a typo such as
// BuildDate instead of Date would ship binaries stuck at "unknown".
func TestReleasePipelineLdflagsMatchBuildinfoSymbols(t *testing.T) {
	// Ensure the package variables exist and have their expected defaults.
	if buildinfo.Version == "" || buildinfo.Commit == "" || buildinfo.Date == "" {
		t.Fatalf("unexpected empty buildinfo defaults: Version=%q Commit=%q Date=%q",
			buildinfo.Version, buildinfo.Commit, buildinfo.Date)
	}

	wantTargets := map[string]bool{
		"github.com/sorotrail/sorolens/internal/buildinfo.Version": true,
		"github.com/sorotrail/sorolens/internal/buildinfo.Commit":  true,
		"github.com/sorotrail/sorolens/internal/buildinfo.Date":    true,
	}

	root := filepath.Join("..", "..")
	for _, rel := range []string{".goreleaser.yaml", "Dockerfile", "Makefile"} {
		data, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("reading %s: %v", rel, err)
		}
		matches := ldflagTargetRe.FindAllStringSubmatch(string(data), -1)
		if len(matches) != len(wantTargets) {
			t.Fatalf("%s: found %d buildinfo -X ldflags, want %d", rel, len(matches), len(wantTargets))
		}
		seen := make(map[string]bool, len(matches))
		for _, m := range matches {
			target := m[1]
			if !wantTargets[target] {
				t.Errorf("%s: unknown buildinfo ldflag target %q", rel, target)
			}
			seen[target] = true
		}
		for target := range wantTargets {
			if !seen[target] {
				t.Errorf("%s: missing buildinfo ldflag target %q", rel, target)
			}
		}
	}

	// Verify .goreleaser.yaml keeps CGO disabled for static single-binary builds.
	goreleaserRaw, err := os.ReadFile(filepath.Join(root, ".goreleaser.yaml"))
	if err != nil {
		t.Fatalf("reading .goreleaser.yaml: %v", err)
	}
	if !strings.Contains(string(goreleaserRaw), "CGO_ENABLED=0") {
		t.Error(".goreleaser.yaml must set CGO_ENABLED=0")
	}
}
