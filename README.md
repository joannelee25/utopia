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
1. Functional programming is used instead of Object Oriented Programming as functional programming is aligned with spark's distributed structure for parallelism in data processing.
2. In `process_event.py`, broadcast is used for the small static file dataset B to cache the data at each worker node instead of doing a join which introduces a shuffle.
3. When there is a duplicate in detection_oid with different item name, which item is counted is non-deterministic.
4. When there is a tie during ranking of top x items, which item appears in the top x is non-deterministic.  

#### SPARK CONFIGURATION
`process_event.py` allows for different spark configuration settings. By default local configuration setting is used. 

In production workloads, the following is enabled to detect and handle skewed joins at runtime by splitting oversized partitions:
1. Enable AQE: `.config("spark.sql.adaptive.enabled", True)`
2. Enable skew join optimization: `.config("spark.sql.adaptive.skewJoin.enabled", True)`

To run development workloads locally, 
```bash
spark-submit \
process_event.py \
--file1 <file path for dataset A> \
--file2 <file path for dataset B> \
--output_path <output file path> \
--top_x <number of top count to find> \
```

To run production workloads, configuration can be supplied
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors 10 \
  --conf spark.sql.shuffle.partitions=160 \
  --conf spark.sql.adaptive.enabled=true \
  process_event.py \
  --file1 <file path for dataset A> \
  --file2 <file path for dataset B> \
  --output_path <output file path> \
  --top_x <number of top count to find> \
  --env prod
```

## Distributed Computing Task II
1.	Suppose there is data skew in one of the geographical locations in Dataset A. Please provide another code snippet on how you will be re-implement part of the program to speed up the computations 


If there is a data skew in geographical locations in Dataset A (For eg, 80% of detections are from one city), the shuffle reduceByKey on (item_name, geographical_location_oid) in function `count_unique_detections` will be less efficient as one worker ends up summing majority of the detection counts while others finish quickly and wait. 

The salting technique can be used to spread the hot key. The function `count_unique_detections` will be modified to take in a salt_partition parameter:

```
import random

def count_unique_detections(rdd: RDD, salt_partitions: int = 10) -> RDD:
     return (
         rdd.map(lambda row: (row.detection_oid,row))
         .reduceByKey(lambda a, b: a)
         .map(lambda kv: ((kv[1].item_name, kv[1].geographical_location_oid, random.randrange(salt_partitions)),1,))
         .reduceByKey(add)
         .map(lambda kv: ((kv[0][0], kv[0][1]), kv[1]))
         .reduceByKey(add)
     )
```