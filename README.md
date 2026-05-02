# UTOPIA

## Distributed Computing Task I
I started this task by creating `create_dataset.py` to generate sample input Dataset A and Dataset B files to be used for local testing.

### Assumptions

#### Output parquet file format
The output parquet file after processing uses geographical_location from Dataset B as the first column instead of geographical_location_oid as stated in the test since the intention of the test is to get an output from 2 input files.

| Column Name | Column Type | Comment |
|:-----------|:------------:|------------:|
| geographical_location | bigint | The geographical location name        |
| item_rank | int | item_rank=1 corresponds to the most popular item detected in geographical location        |
| item_name | varchar(5000) | Item name

### Design considerations
1. Functional programming is used instead of Object Oriented Programming as functional programming is aligned with spark's distributed structure for parallelism.

#### SPARK CONFIGURATION
`process_event.py` allows for different spark configuration settings. By default local configuration setting is used. 

Production configuration can be supplied
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors 10 \
  --conf spark.sql.shuffle.partitions=160 \
  --conf spark.sql.adaptive.enabled=true \
  process_event.py --file1 <file path for dataset A> --file2 <file path for dataset B> --output_file <output file path> --top_x <number of top count to find> --env prod
```