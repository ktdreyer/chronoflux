# Fixture provenance

These fixtures are synthetic and redacted, but their annotation and label keys
now mirror the real p02 `ai-tenant` conventions verified in October 2026
(RHAI-10822): `appstudio.openshift.io/component` / `application` labels,
`build.appstudio.redhat.com/commit_sha` / `target_branch` /
`pull_request_number` annotations, `pipelinesascode.tekton.dev/*` keys, and the
`build.appstudio.redhat.com/target-platform` label plus `PLATFORM` param on
build-container TaskRuns. URLs use `example.invalid` and IDs/timestamps are
fabricated; they are not verbatim captures from MR !1458.
