# CHANGELOG

## Emoji Cheatsheet
- :pencil2: doc updates
- :bug: when fixing a bug
- :rocket: when making general improvements
- :white_check_mark: when adding tests
- :arrow_up: when upgrading dependencies
- :tada: when adding new features

## Version History

### v1.3.2
- :arrow_up: Update `@tak-ps/etl` to `^11.4.0`. CloudTAK v14 requires ETLs to be on ETL-Base v11 or higher. v11 publishes Output schemas as named schemas, so this task needed no change for that, and the release also works with CloudTAK v13
- :arrow_up: Update the other dependencies within their existing ranges, which clears the `npm audit --production` findings that the deploy workflow checks
### v1.3.1
- :bug: Detection footprints now expire. They were sent as `u-d-f` drawings, which ATAK never expires. They are now sent with the detection point's marker type (`a-f-X-i`), so CloudTAK (`@tak-ps/node-cot` >= 14.58.0) converts them into a marker CoT with a `<shape><polyline>` and ATAK removes the footprint together with the marker at `stale`
- :bug: The footprint marker is invisible (`marker-color` with `marker-opacity: 0`, written as `<color argb>` with alpha 0), otherwise ATAK draws an unknown-type "?" on it. It must not carry an `icon`: a valid `usericon` makes ATAK move the marker to another map group, which deletes the polygon it owns. The fire icon stays on the detection point at the footprint's centre
- :rocket: The footprint sets no `stale` itself, like the detection point, so both take the layer's stale setting. Check that the layer applies it to polygons as well as points, otherwise the footprint gets node-cot's 20 second default
- :white_check_mark: Test the footprint feature and its conversion through `CoTParser.from_geojson` (adds `@tak-ps/node-cot` as a devDependency)
- :arrow_up: `npm audit fix`, `npm audit` reports 0 vulnerabilities
### v1.3.0
- :bug: Add `.dockerignore` so locally built images no longer bundle git-ignored local files. A stale `dist/.env` (`ETL_API=http://localhost:5001`) was copied into the image by `deploy-etl.sh` and overrode the Lambda's `ETL_API`/`ETL_LAYER` at runtime, causing `fetch failed`
- :tada: Add `capabilities.json` manifest (validated against `@tak-ps/etl`'s `StaticCapabilitiesSchema`) declaring the `feature:*` permission, compute (1024 MB / 300 s) and a `rate(1 minute)` schedule, embedded in the pushed image as the `com.cloudtak.capabilities` OCI annotation via `docker buildx` in CI. The existing CloudFormation-export ECR lookup is kept; the `cloudtak-etl` CLI is not used because it hardcodes `tak-vpc-<Environment>-cloudtak-tasks`
- :white_check_mark: Add basic test suite (`npm test`) covering static config, Input/Output schemas and the capabilities manifest
- :rocket: Switch to `Task.init()` for local-dev `ETL_TOKEN` auto-generation (no behavior change in Lambda)
- :arrow_up: Require Node 24 in CI and `engines`, matching the Dockerfile runtime and `@tak-ps/etl`
- :arrow_up: Update dependencies (`@tak-ps/etl` 10.8.0 -> 10.22.2, `adm-zip` 0.5.x -> 0.6.1, `tsx` 4.23.15 added) and resolve all `npm audit` advisories (0 remaining). `typescript` stays on `^6.0.3` until `typescript-eslint` supports 7.x
- :arrow_up: Update GitHub Actions to releases that run on Node.js 24, clearing the Node.js 20 deprecation warnings: `actions/checkout` v7, `actions/setup-node` v7, `aws-actions/configure-aws-credentials` v6 and `docker/setup-buildx-action` v4. `aws-actions/amazon-ecr-login` v2 already runs on Node.js 24. Not yet run in CI on these versions
- :rocket: Pin the workflow runners to `ubuntu-24.04` instead of `ubuntu-latest`, so the `ubuntu-latest` migration to Ubuntu 26 (starting October 19, 2026) does not change the build environment unannounced

### v1.0.0

- :tada: Initial Commit
