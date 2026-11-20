# Healthcare11-16

CMS nursing-home staffing pipeline on AWS (S3 and Glue), as approved in the architecture diagram.
Shared Google Drive folder -> Glue ingest -> S3 Raw -> Glue -> S3 Silver -> Glue -> S3 Gold -> Streamlit (hosted outside AWS).

| Path | What it is |
|---|---|
| `glue/ingest_drive.py` | Downloads only new or changed Drive files into Raw (manifest of Drive checksums) |
| `glue/raw_to_silver.py` | Cleans, runs the data-quality checks, writes Parquet (staffing by month, CMS tables) |
| `glue/silver_to_gold.py` | Builds `fact_staffing_daily`, `dim_facility`, `dim_date`, `mart_state_month`, `mart_facility_quarter` |
| `infra/template.yaml` | CloudFormation: 3 buckets, 3 Glue jobs, workflow, 11:00 AM ET trigger, deploy role |
| `.github/workflows/deploy.yml` | On push to `main`: updates the stack and uploads scripts (no AWS keys stored in GitHub) |

No data or credentials are ever committed (see `.gitignore`).

## One-time setup (AWS CloudShell, region us-east-1)
1. Create the GitHub sign-in provider if the account does not have one yet:
   `aws iam create-open-id-connect-provider --url https://token.actions.githubusercontent.com --client-id-list sts.amazonaws.com`
   (an "already exists" error is fine.)
2. Clone this repo in CloudShell, then deploy the stack:
   `aws cloudformation deploy --stack-name healthcare-metrics --template-file infra/template.yaml --capabilities CAPABILITY_NAMED_IAM --parameter-overrides DriveFolderId=<DRIVE_FOLDER_ID>`
3. Share the Drive folder as "Anyone with the link" (viewer). No Google key or secret is used.
4. Upload the scripts once: `aws s3 sync glue s3://healthcare-metrics-raw-<ACCOUNT_ID>/_scripts/ --exclude "*" --include "*.py"`
5. In GitHub, repo Settings -> Variables, add `AWS_DEPLOY_ROLE_ARN` (stack output `GitHubDeployRoleArn`) and `DRIVE_FOLDER_ID`.
6. Test: `aws glue start-workflow-run --name healthcare-metrics-daily`.

Turn the schedule on after a successful manual test run: redeploy with `ScheduleEnabled=true`.

After that, merging to `main` deploys changes automatically and Glue runs daily.

## Drive ingest notes
Only Drive files not seen before are downloaded (tracked by Drive file id). If a file is replaced in place under the same id, run the ingest job with `--FORCE_REFRESH true`. Google can throttle repeated link downloads; the job fails loudly and can be rerun later.

## Schedule
`cron(0 16 * * ? *)` = 11:00 AM Eastern Standard Time. Glue schedules are UTC and ignore daylight saving, so from March 8, 2027 change the `ScheduleCron` parameter to `cron(0 15 * * ? *)` to stay at 11:00 AM (and back to 16:00 in November).

## Status
Written but not yet run on AWS. The Spark jobs have not been executed anywhere yet, so expect a fix or two on the first real run.
