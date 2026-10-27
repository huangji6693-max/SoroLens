# Releasing SoroLens

## Overview

Pushing a `v*` tag triggers the release jobs in `.github/workflows/release.yml`:

1. **GoReleaser (`goreleaser` job)** builds static (`CGO_ENABLED=0`) binaries for Linux, macOS, and Windows (`amd64` + `arm64` each), computes SHA-256 checksums (`checksums.txt`), generates changelog notes, and publishes a GitHub Release with the archives attached.
2. **Docker publish (`docker` job)** builds and pushes a multi-arch container image (`linux/amd64`, `linux/arm64`) to `ghcr.io/sorotrail/sorolens` with `VERSION`, `COMMIT`, and `DATE` stamped into the binary via `Dockerfile` build arguments.

On pull requests that touch release configuration or build metadata, the `snapshot` job runs `goreleaser check` and `goreleaser release --snapshot --clean` without requiring secrets or publishing artefacts, then starts the compiled binary and verifies that `GET /api/version` reports the stamped values rather than the `dev`/`none`/`unknown` defaults.

## How to release

1. Ensure `main` is at the commit you want to release and CI is green.

2. Tag and push:

   ```bash
   git checkout main
   git pull origin main
   git tag -a v0.1.0 -m "v0.1.0"
   git push origin v0.1.0
   ```

   Use [semver](https://semver.org/) for the tag name. Pre-release tags (e.g. `v0.1.0-rc.1`) automatically mark the GitHub Release as a pre-release (`prerelease: auto`).

3. Monitor the workflow at:

   - `https://github.com/sorotrail/SoroLens/actions/workflows/release.yml`

4. Once complete, the GitHub Release page carries the platform archives and `checksums.txt`, and `ghcr.io/sorotrail/sorolens:<tag>` (plus `:latest` for stable tags) is published.

## Verifying a release

### Download and check stamped version

```bash
# Linux amd64
curl -LO https://github.com/sorotrail/SoroLens/releases/download/v0.1.0/sorolens_0.1.0_linux_amd64.tar.gz
tar -xzf sorolens_0.1.0_linux_amd64.tar.gz

# Run in upstream mode briefly and query /api/version
SOURCE_MODE=sorotrail SOROTRAIL_URL=http://127.0.0.1:19999 HTTP_ADDR=127.0.0.1:18080 ./sorolens_0.1.0_linux_amd64/sorolens &
PID=$!
sleep 1
curl -s http://127.0.0.1:18080/api/version
kill $PID
```

### Verify checksums

```bash
curl -LO https://github.com/sorotrail/SoroLens/releases/download/v0.1.0/checksums.txt
sha256sum -c checksums.txt --ignore-missing
```

### Verify container image

```bash
docker run --rm -d --name sorolens-verify \
  -p 18080:8080 \
  -e SOURCE_MODE=sorotrail \
  -e SOROTRAIL_URL=http://127.0.0.1:19999 \
  ghcr.io/sorotrail/sorolens:v0.1.0
curl -s http://127.0.0.1:18080/api/version
docker stop sorolens-verify
```

## Local snapshot builds

Validate configuration and build binaries locally without creating or pushing a tag:

```bash
goreleaser check
goreleaser release --snapshot --clean
```

Generated archives and binaries are placed in `dist/`.

## Release notes

GoReleaser groups commit messages into **Features** (`feat:`), **Bug fixes** (`fix:`), and **Maintenance**, excluding `docs:`, `test:`, `chore:`, `ci:`, and merge commits as configured in `.goreleaser.yaml`.
