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