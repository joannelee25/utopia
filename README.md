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

#### Tie breaking
If there are 2 items having the same highest count and top x=1, the record that is filtered away is non-deterministic.

### Design considerations
1. Functional programming is used instead of Object Oriented Programming as functional programming is aligned with spark's distributed structure for parallelism in data processing.
2. In `process_event.py`, broadcast is used for the small static file dataset B to cache the data at each worker node instead of doing a join which introduces a shuffle.
3. When there is a duplicate in detection_oid with different item name, which item is counted is non-deterministic.
4. When there is a tie during ranking of top x items, which item appears in the top x is non-deterministic.  
5. PipelineConfig is used make `process_event.py` reusable with another table. 

#### SPARK CONFIGURATION
`process_event.py` allows for different spark configuration settings. By default local configuration setting is used. 

In production workloads, the following is enabled to detect and handle skewed joins at runtime by splitting oversized partitions:
1. Enable AQE: `.config("spark.sql.adaptive.enabled", True)`
2. Enable skew join optimization: `.config("spark.sql.adaptive.skewJoin.enabled", True)`

### RUNNING THE PROGRAM
Build docker in the root folder
```bash
 docker build -t utopia:latest .
```

To run development workloads locally, 
```bash
 # Run process_event (mount local data directories)                                    
  docker run --rm -v <path to repo root>/data/:/data utopia:latest \
    --file1 /data/raw/dataset_A.parquet \
    --file2 /data/raw/dataset_B.parquet \
    --output_path /data/processed/dataset_C \
    --top_x <number of top count to find>
```

To run production workloads, 
```bash
  docker run --rm -v <path to repo root>/data/:/data utopia:latest \
      --file1 /data/raw/dataset_A.parquet \
      --file2 /data/raw/dataset_B.parquet \
      --output_path /data/processed/dataset_C \
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