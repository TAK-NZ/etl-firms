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
- :tada: Add `capabilities.json` manifest (validated against `@tak-ps/etl`'s `StaticCapabilitiesSchema`) declaring the `feature:*` permission, compute (1024 MB / 300 s) and a `rate(1 minute)` schedule, embedded in the pushed image as the `com.cloudtak.capabilities` OCI annotation via `docker buildx` in CI. The existing CloudFormation-export ECR lookup is kept; the `cloudtak-etl` CLI is not used because it hardcodes `tak-vpc-<Environment>-cloudtak-tasks`
- :white_check_mark: Add basic test suite (`npm test`) covering static config, Input/Output schemas and the capabilities manifest
- :rocket: Switch to `Task.init()` for local-dev `ETL_TOKEN` auto-generation (no behavior change in Lambda)
- :arrow_up: Require Node 24 in CI and `engines`, matching the Dockerfile runtime and `@tak-ps/etl`
- :arrow_up: Update dependencies (`@tak-ps/etl` 10.8.0 -> 10.22.2, `adm-zip` 0.5.x -> 0.6.1, `tsx` 4.23.15 added) and resolve all `npm audit` advisories (0 remaining). `typescript` stays on `^6.0.3` until `typescript-eslint` supports 7.x

### v1.0.0

- :tada: Initial Commit
