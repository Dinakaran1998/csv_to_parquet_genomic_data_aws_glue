# AWS Glue ETL Job: CSV to Parquet by Chromosome

---

**Description:**
The script(make_parquet.py) reads multiple CSV files from S3, splits the data by chromosome, and writes each chromosome’s data as Parquet files back to S3.

---

## Steps to Create AWS Glue Job

1. **Upload the Script to S3**

   * Upload make_parquet.py Python ETL script to an S3 bucket.

2. **Create IAM Role for Glue Job**

   * Role name: `AWSGlueServiceRole`
   * Attach policies:

     * `AmazonS3FullAccess` (for reading/writing data to S3)
     * `AWSGlueServiceRole` (Glue service execution permissions)

3. **Submit Job via AWS CLI**

You can submit the job using the following AWS CLI command. Replace the input and output paths as needed:

aws glue start-job-run \
    --job-name csv_to_parquet_genomic_data \
    --arguments '{ \
        "--INPUT_PATH":"s3://aws-batch-input-bioinformatics/csv/*.csv", \
        "--OUTPUT_PATH":"s3://aws-batch-input-bioinformatics/output/" \
    }'

---

## About AWS Glue

### Spark Cluster Terms

| Concept   | Explanation                                           |
| --------- | ----------------------------------------------------- |
| Cluster   | The whole Spark environment (driver + executors).     |
| Driver    | The “master brain” that coordinates tasks.            |
| Executors | The “workers” that actually process data in parallel. |

### Glue Job Configuration

* **Worker Type:** e.g., `G.1X` = 4 vCPU + 16 GB RAM
* **Number of Workers:** e.g., 5

Glue will spin up a Spark cluster:

* 1 Driver (managed by AWS, not counted in your worker setting)
* N Executors (equal to the number of workers specified)

**Summary:**

* Worker = one executor node/container
* Cluster = driver + all workers

---

## Automatically Scale the Number of Workers (FLEX)

* You set a **maximum number of workers** (`--number-of-workers`)
* Glue starts small and scales up/down dynamically based on:

  * Dataset size
  * Stage of Spark job (reading, shuffling, writing)
  * Parallelism available in the DAG

**Example:**

* Worker type: `G.1X`
* Max workers: 10
* Auto-scaling: ON

➝ Glue might start with 2 workers, scale up to 6–8 during a heavy stage, then scale down before writing.

---

### Cost Effects

* **Billing**: Only for workers actually in use per minute
* **Auto-scaling vs Fixed**:

  * Fixed workers run full job duration (idle workers cost money)
  * Auto-scaling adds/removes workers as needed → saves cost

**Rule of Thumb:**

| Job Size / Pattern              | Best Choice                      | Reason                                |
| ------------------------------- | -------------------------------- | ------------------------------------- |
| Tiny (<1 GB, predictable)       | 1 worker, STANDARD               | Minimal cost, auto-scaling not needed |
| Medium (1–50 GB, variable load) | FLEX, auto-scale, reasonable max | Scales up only when needed            |
| Large (>50 GB, heavy shuffles)  | FLEX, higher max workers         | Saves cost by removing idle workers   |

---

### When Auto-Scaling May NOT Be Cost-Efficient

* Very small jobs (<1 GB) → job likely never scales above 1–2 workers
* Long-running jobs with constant high load → almost same cost as fixed workers
* Frequent worker spin-up/down for tiny stages → minimal savings

---

### Recommendations for Your Dataset

* Dataset: 2 CSVs, 250 MB
* Auto-scaling won’t help much; 1 fixed worker (STANDARD) is simpler and cheapest.
* For larger/unpredictable datasets → FLEX with auto-scaling is recommended.
