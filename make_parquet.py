import sys
import boto3
import os
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col

# Glue boilerplate
args = getResolvedOptions(sys.argv, ["JOB_NAME", "INPUT_PATH", "OUTPUT_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Chromosomes
chromosomes = [f"chr{i}" for i in range(1, 23)] + ["chrX", "chrY", "chrM"]

# Input S3 path (all CSVs)
input_path =  args["INPUT_PATH"]       #"s3://aws-batch-input-bioinformatics/*.csv"  # e.g. s3://my-genomics-data/csv_new/*.csv
output_dir =  args["OUTPUT_PATH"]       #"s3://aws-batch-input-bioinformatics/output/" # e.g. s3://my-genomics-output/

# Dict to hold DataFrames
dfs_by_chrom = {chrom: None for chrom in chromosomes}

# Read all CSVs from S3
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("quote", '"')
    .option("escape", '"')
    .csv(input_path)
)

# Split by chromosome
for chrom in chromosomes:
    df_chr = df.filter(col("variant_id").startswith(chrom + ":"))
    
    if dfs_by_chrom[chrom] is None:
        dfs_by_chrom[chrom] = df_chr
    else:
        dfs_by_chrom[chrom] = dfs_by_chrom[chrom].unionByName(
            df_chr, allowMissingColumns=True
        )

# Write per-chromosome Parquet to S3
for chrom, df_chr in dfs_by_chrom.items():
    if df_chr is None:
        continue

    df_chr.coalesce(1).write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(os.path.join(output_dir, f"{chrom}/"))

print("Processing completed!")
