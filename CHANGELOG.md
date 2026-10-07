# CHANGELOG

## Emoji Cheatsheet
- :pencil2: doc updates
- :bug: when fixing a bug
- :rocket: when making general improvements
- :white_check_mark: when adding tests
- :arrow_up: when upgrading dependencies
- :tada: when adding new features

## Version History

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
