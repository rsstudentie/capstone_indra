# capstone_indra

## Introduction and Overview

This report outlines the process of ingesting, transforming, and analyzing bearing data using various methods and tools.

## ELT Process

The ELT process involves extracting data from multiple files in a sample folder, loading it into a database, and transforming it to generate insights.

### Ingestion of Sample Data

#### Method 1: Simple Loop

The simple loop method involves reading data from local files and inserting it into the database. It's suitable for initial data loading and small-scale ETL operations.

**Steps**:
1. Iterate through the folder to process all files.
2. Extract timestamp from the file name.
3. Extract individual channels' data from the text file.
4. Compute aggregates.

#### Method 2: Kafka Streams

For real-time data ingestion and processing, Kafka Streams hosted on Azure Cloud is used. 

Using Kafka Streams allows for easy modification of the code to handle real-time data processing and scalability by adding more consumers or producers.

**Steps**:
1. Read data from files in the folder.
2. Compute aggregates.
3. Send data to Kafka topic.
4. Consume data from Kafka topic and save it to the database.
5. Upload data to the database.
6. Use Kafka Streams for real-time processing.

### Database

Our database is hosted on CockroachDB, which offers scalability, resilience, and consistency.

#### Tables

- **bearings**: Stores time-series data of bearing measurements.

```sql
CREATE TABLE bearings (
    eventtime TIMESTAMP NOT NULL,
    bearing1_c1 INT8 NOT NULL,
    bearing1_c2 INT8 NOT NULL,
    bearing2_c1 INT8 NOT NULL,
    bearing2_c2 INT8 NOT NULL,
    bearing3_c1 INT8 NOT NULL,
    bearing3_c2 INT8 NOT NULL,
    bearing4_c1 INT8 NOT NULL,
    bearing4_c2 INT8 NOT NULL,
    rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
    CONSTRAINT bearings_pkey PRIMARY KEY (rowid ASC)
);
```

- **control_limits**: Stores Upper Control Limits (UCL) and Lower Control Limits (LCL) for monitoring process quality.

```sql
CREATE TABLE control_limits (
    bearing_name VARCHAR PRIMARY KEY,
    mean FLOAT8,
    ucl FLOAT8,
    lcl FLOAT8
);
```

### Interpolation

To handle missing values in the dataset, interpolation is used. Missing values in sample data are filled using the linear interpolation method.

**Steps**:
1. Sort the DataFrame by eventtime.
2. Set eventtime as the index.
3. Resample to a specified interval and interpolate.
4. Reset the index and fill any remaining missing values with the mean of each column.
5. Save the interpolated data to the database.

### Generate Historical Data

Synthetic historical data is generated using the DeepEcho library. DeepEcho generates synthetic data based on the patterns learned from the original data.

**Steps**:
1. Define data types for all columns.
2. Instantiate PARModel and generate synthetic data.
3. Calculate the start and end date for generating synthetic data.
4. Generate timestamps at regular intervals.
5. Sample new data for the defined period.
6. Save the synthetic data to the database.

## Calculate UCL and LCL

Upper Control Limits (UCL) and Lower Control Limits (LCL) are calculated for each bearing parameter.

The UCL and LCL are calculated as follows:
- UCL = Mean + 3 * Standard Deviation
- LCL = Mean - 3 * Standard Deviation

**Steps**:
1. Calculate the mean and standard deviation for each parameter.
2. Calculate UCL and LCL for each parameter.
3. Save the control limits to the database.
4. Monitor the process quality using control limits.
5. Send alerts if any parameter goes out of control limits.
6. Update control limits periodically based on new data.

