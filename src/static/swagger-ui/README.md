# Vendored Swagger UI assets

These files are the static assets for the Swagger UI page served by the EventGate `GET /docs` endpoint. They are
vendored (committed into the repository and baked into the Docker image) and inlined directly into the `/docs`
HTML response so that the page is fully self-hosted and makes **no external requests** at runtime — see issue #197.

## Provenance

| Property | Value                                                              |
|----------|--------------------------------------------------------------------|
| Package  | [`swagger-ui-dist`](https://www.npmjs.com/package/swagger-ui-dist) |
| Version  | **5.32.9** (pinned — do not use a floating `@5` tag)               |
| Upstream | https://github.com/swagger-api/swagger-ui                          |
| Source   | `https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.32.9/<file>`       |
| License  | Apache-2.0 (see `LICENSE`)                                         |

## Files

| File                   | SHA-256                                                            |
|------------------------|--------------------------------------------------------------------|
| `swagger-ui.css`       | `ca238f7d7c2cf4480c1e77a9c3b9da915ab216e96ffd354e69076560c650c6de` |
| `swagger-ui-bundle.js` | `303f48967313bee56f30c651b6b90467482a99d2cf1571cde4e44527912eceea` |

`swagger-ui-bundle.js` is self-contained (bundles the standalone presets); no
additional Swagger UI scripts are required.

## Upgrading

1. Pick an exact `swagger-ui-dist` version (e.g. `5.x.y`).
2. Re-download both files from
   `https://cdn.jsdelivr.net/npm/swagger-ui-dist@<version>/`:

   ```sh
   BASE="https://cdn.jsdelivr.net/npm/swagger-ui-dist@<version>"
   curl -sSo swagger-ui.css       "$BASE/swagger-ui.css"
   curl -sSo swagger-ui-bundle.js "$BASE/swagger-ui-bundle.js"
   curl -sSo LICENSE              "$BASE/LICENSE"
   ```

3. Update the version and SHA-256 checksums in this file
   (`shasum -a 256 swagger-ui.css swagger-ui-bundle.js`).
