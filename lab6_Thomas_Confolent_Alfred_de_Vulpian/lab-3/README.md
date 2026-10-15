# Lab 6.3 - Job and CronJob

This lab use Kubernetes Jobs for execute a task one time and CronJobs for execute tasks with a schedule.

## Simple Job

The `data-import-job` started one pod, processed the example records and finished with the `Complete` status. A Job keep the pod after completion, so we can read its logs and status.

## Failure and retries

The transformation Job always return exit code 1. With `backoffLimit: 3`, Kubernetes make one first pod and three retry pods. The failed pods stay for show logs and help to understand the problem. The Job failure reason is `BackoffLimitExceeded`.

When `activeDeadlineSeconds` is changed to 10, Kubernetes stop the Job before all retries. The failure reason become `DeadlineExceeded`.

## Parallel Job

The export Job use four pods in parallel. Each pod receive a different `JOB_COMPLETION_INDEX` from 0 to 3. Like this, every pod process one different shard and the four completions are required.

## CronJobs

The pipeline CronJob create one Job every two minutes. `concurrencyPolicy: Forbid` prevent two executions at the same time. The backup CronJob run every day at 02:00 with the `Europe/Paris` time zone.

A CronJob can be suspended and resumed without delete it:

```bash
kubectl -n lab-job-cronjob patch cronjob bronze-to-silver-pipeline -p '{"spec":{"suspend":true}}'
kubectl -n lab-job-cronjob patch cronjob bronze-to-silver-pipeline -p '{"spec":{"suspend":false}}'
```

## ETL scenario

The ETL CronJob read `BATCH_SIZE`, `SOURCE_LAYER` and `TARGET_LAYER` from a ConfigMap. I created a manual Job from the CronJob template for test it without wait five minutes. The logs confirmed the transformation from bronze to silver with a batch size of 1000.

## Files

- `job-simple.yaml`: one task who finish successfully.
- `job-with-retry.yaml`: task who fail and use retries.
- `job-parallel.yaml`: four indexed tasks in parallel.
- `cronjob-pipeline.yaml`: pipeline every two minutes.
- `cronjob-backup.yaml`: daily backup at 02:00 in Paris time.
- `cronjob-etl.yaml`: ETL pipeline configured by ConfigMap.

At the end, all Jobs, CronJobs and the namespace are deleted.
