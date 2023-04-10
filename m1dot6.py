# Kevin Hitt
# M1.6 - Hive / Impala Assignment

# We are not worried about the truthfulness of this data.
# This data should be downloaded from Kaggle and to be used for the following assignment.
# https://www.kaggle.com/datasets/jaykay12/odi-cricket-matches-19712017?resource=download

# Submit the code and the video.
# Assignment Rubric: 
# Completeness (50%)
# Accuracy (50%)

# 1. Load this data in HDFS
# File location and type
# file_location = "/FileStore/tables/originalDataset.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)
