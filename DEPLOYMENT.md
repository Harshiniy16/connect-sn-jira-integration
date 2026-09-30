# Deployment

## Why Terraform

I'd use Terraform. The `plan` step lets someone review exactly what will change before it happens, `terraform import` lets me adopt existing resources without recreating them, and it isn't tied to AWS if we end up managing something else alongside. CloudFormation or CDK would also work and would be my pick if the team already standardizes on them. I wouldn't use plain scripts beyond thin glue, since they're hard to review and repeat.

Since there's already a custom deployment system, I'd treat it as the orchestrator and make Terraform one more step in it: produce a plan, get approval, apply from the saved plan. Before writing anything I'd inventory what that system already deploys (Connect flows, Lambdas, ServiceNow update sets, Jira automations) and agree on who owns what, so Terraform never manages something another system owns.

The included `terraform/main.tf` is a skeleton. It shows the resources that matter for correctness (idempotency table, queues, DLQ and alarm, Kinesis stream, reserved Lambda concurrency) but it isn't deployable as-is. Packaging, IAM, event source mappings, networking, KMS, AppConfig and per-environment values are left out. In the real environment the existing deployment system would supply those.

## Not breaking what already runs

- Everything new is prefixed `cc-int-` and tagged with an owner. For the first releases I don't modify any existing Lambda, flow or ServiceNow rule.
- Phase 1 starts in shadow mode. It reads CTRs and logs the ticket it would have created, without writing anything. I compare that against what agents log manually.
- I never edit a live contact flow. I clone it, add the Lambda block with an error branch that goes to the existing default path, and point one number or queue at the clone.
- ServiceNow changes go in as a scoped app or update set with its own fields, rules and service account. Every business rule checks an `integration_enabled` property first, so turning it off makes it a no-op.
- In Jira I use a dedicated automation user and webhooks filtered by JQL to the relevant issue types.
- Before Phase 3, I list the existing ServiceNow rules and Jira automations that touch the same tables and issue types and either add the actor filter or exclude the service account.
- CI runs contract tests against a ServiceNow dev instance and a Jira sandbox project.

## Environments

- **dev**: a test Connect instance, ServiceNow dev instance, Jira sandbox project. Per-branch stacks.
- **staging**: production-like, with anonymized CTRs replayed into it. This is where I load test and rehearse the rollout.
- **prod**: the three real instances.

Prod and non-prod live in separate AWS accounts, with Terraform state in S3 and a DynamoDB lock per environment. The same modules run everywhere with different variable files, and the same commit gets promoted through all three.

## Pipeline

Lint and test, package the Lambdas, `terraform validate` and plan, static checks (tflint, checkov), approval, apply, then a smoke test with a synthetic CTR. Lambdas deploy behind a `live` alias with weighted shifting (10% then 100%) and roll back automatically if an alarm fires.

## Secrets

Credentials live in Secrets Manager. Terraform creates the empty secret and the value is set separately, so it never ends up in code, environment variables or state. Lambdas cache the value for a few minutes. I'd use OAuth client credentials where ServiceNow and Jira support them, otherwise a least-privilege service account, rotated every 90 days with both old and new valid briefly. Each Lambda gets its own IAM role scoped to its own resources, and webhooks are authenticated with a signature plus timestamp to block replays.

## Rollout plan

0. Deploy everything to prod with all flags off.
1. Turn on Phase 1 shadow mode for all three instances. Move on once the logged payloads match the manual tickets for a few days (I'd aim for 99% or better).
2. Turn on Phase 1 for Sales. I picked Sales first because it has the fewest downstream automations. Watch for a week: no duplicates, empty DLQ, call-to-ticket time under about two minutes.
3. Then TAM, then Support. Support goes last because it has the biggest blast radius.
4. Phase 2: first only set the attributes without changing routing, then route on one Support queue. Watch latency and how often it falls back to defaults.
5. Phase 3: Support to Jira only, one Jira project, then bidirectional once the loop alarm has stayed quiet and drift is near zero.
6. Extend to Sales and TAM and retire manual logging once the team leads sign off.

I'd avoid all three teams at once because each has its own queues, SLAs and automations. A bug that would hit 3,000 calls a day gets caught at 700 instead. Flags are per instance in AppConfig, so each step is a config change and not a deploy.
