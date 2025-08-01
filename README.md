# PySpark RDD Transformations and DataFrames

Welcome to the PySpark RDD Transformations and DataFrames project! This hands-on project demonstrates how to use PySpark, the Python interface for Apache Spark, to process and analyze large-scale data using the NYC Yellow Taxi Trip Records dataset. The project covers core Spark concepts, including Resilient Distributed Datasets (RDDs), DataFrames, transformations, actions, and optimization techniques, providing a practical introduction to big data processing.

## Project Overview

This project aims to showcase how PySpark can be used to build a scalable data pipeline for processing large datasets. Using the NYC Yellow Taxi dataset, the project demonstrates:

- Loading and cleaning raw data.
- Performing transformations with RDDs and DataFrames.
- Analyzing data to derive insights, such as trip patterns or fare statistics.
- Optimizing Spark jobs for performance.
- Monitoring job execution using the Spark UI.

The goal is to provide a clear, end-to-end workflow that transforms raw taxi trip data into actionable insights, suitable for business applications like optimizing taxi operations or understanding passenger behavior.

## Objectives

- Learn Apache Spark architecture and its distributed processing capabilities.
- Understand RDDs and DataFrames for data manipulation.
- Implement transformations (e.g., filtering, grouping) and actions (e.g., counting, saving results).
- Apply optimization techniques like caching and partitioning.
- Explore the Spark UI and spark-submit for job monitoring and execution.
- Build a reusable data pipeline for real-world big data scenarios.

## Dataset Description

The project uses the NYC Yellow Taxi Trip Records dataset, which contains millions of trip records with 17 fields, including:

- `VendorID`: Identifier for the taxi vendor.
- `tpep_pickup_datetime`: Timestamp when the trip started.
- `tpep_dropoff_datetime`: Timestamp when the trip ended.
- `passenger_count`: Number of passengers in the taxi.
- `trip_distance`: Distance traveled in miles.
- `total_amount`: Total fare paid, including tips and fees.

The dataset is in CSV format and is ideal for practicing big data processing due to its large size and real-world relevance.

## Why This Project?

- **Real-World Application**: The NYC Taxi dataset mirrors datasets used in industries like transportation, urban planning, or analytics.
- **Scalability**: Demonstrates how Spark handles large datasets that exceed the capacity of a single machine.
- **Practical Skills**: Covers essential data engineering skills, including ETL (Extract, Transform, Load), data cleaning, and performance optimization.
- **Business Value**: Produces insights (e.g., average fares by vendor) that can inform business decisions.

## Architecture

The project leverages Apache Spark’s distributed architecture with PySpark as the interface. Here’s a high-level overview:

- **Driver Program**: A Python script (run in Jupyter Notebook) that initializes the SparkSession, the entry point for PySpark.
- **Cluster Manager**: Manages resources (in this project, a local cluster with `local[*]` mode).
- **Executors**: Worker nodes that process data in parallel across partitions.
- **RDDs**: Low-level data structure for distributed, fault-tolerant processing.
- **DataFrames**: Higher-level API for structured data, optimized with Spark’s Catalyst Optimizer.
- **Spark UI**: Web interface (`localhost:4040`) for monitoring job execution, stages, and performance.

## Data Pipeline Workflow

The pipeline processes the dataset in the following steps:

1. **Data Ingestion**: Load the CSV dataset into Spark as an RDD or DataFrame.
2. **Data Cleaning**: Remove invalid records (e.g., negative distances or zero passengers).
3. **Data Transformation**: Apply operations like filtering, grouping, or calculating trip durations.
4. **Data Analysis**: Compute metrics (e.g., average fare per vendor, total trips per day).
5. **Optimization**: Use caching, partitioning, or early filtering to improve performance.
6. **Output**: Save results as files (e.g., CSV, Parquet) or display them in Jupyter Notebook.
7. **Monitoring**: Track job progress and performance via the Spark UI.

Why this workflow? It’s a standard ETL pipeline that ensures data is clean, transformed, and analyzed efficiently, making it reusable for other datasets or business needs.

## Tech Stack

- **Language**: Python 3
- **Library**: PySpark (Spark Core, SQL, DataFrame APIs)
- **Environment**: Jupyter Notebook
- **Dataset**: NYC Yellow Taxi Trip Records (CSV)

## Prerequisites

To run this project, you’ll need:

- Python 3.7+
- Apache Spark (version 3.0 or higher)
- PySpark: Install via `pip install pyspark`
- Jupyter Notebook: Install via `pip install jupyter`
- Java 8 or 11: Required for Spark
- Dataset: Download the NYC Yellow Taxi dataset from the NYC Taxi & Limousine Commission or use a sample CSV file.

## Setup Instructions

### Install Dependencies:

```bash
pip install pyspark jupyter
```

### Set Up Spark:

- Download and install Apache Spark from [spark.apache.org](https://spark.apache.org/).
- Set the `SPARK_HOME` environment variable to the Spark installation directory.
- Add Spark’s `bin` directory to your `PATH`.

### Download the Dataset:

- Obtain the NYC Taxi dataset (e.g., `yellow_tripdata_2023-01.csv`) from the NYC TLC website.
- Place the CSV file in a local directory (e.g., `data/`).

### Clone the Repository:

```bash
git clone https://github.com/alyy10/Pyspark-RDD-Transformations-Dataframes.git
cd Pyspark-RDD-Transformations-Dataframes
```

### Launch Jupyter Notebook:

```bash
jupyter notebook
```

Open the provided notebook (e.g., `NYCTaxiAnalysis.ipynb`) in your browser.

## How to Run the Project

### Open the Notebook:

In Jupyter Notebook, navigate to the project directory and open `NYCTaxiAnalysis.ipynb`.

### Configure the Dataset Path:

Update the file path in the notebook to point to your NYC Taxi CSV file (e.g., `data/yellow_tripdata_2023-01.csv`).

### Execute the Pipeline:

Run the notebook cells sequentially to:

- Initialize the SparkSession.
- Load and clean the dataset.
- Perform transformations and analyses.
- Save or display results.

### Monitor with Spark UI:

Access the Spark UI at `http://localhost:4040` to view job progress, stages, and performance metrics.

### Run via spark-submit (Optional):

Export the notebook as a Python script (e.g., `script.py`).
Run it using:

```bash
spark-submit --master local[*] script.py
```

## Project Workflow Example

Here’s what the pipeline does with the NYC Taxi dataset:

- **Load**: Reads the CSV file into a DataFrame or RDD.
- **Clean**: Filters out trips with invalid data (e.g., `passenger_count <= 0`).
- **Transform**: Groups trips by `VendorID` and calculates metrics like average fare or total trips.
- **Analyze**: Computes insights, such as the busiest days or highest-earning vendors.
- **Optimize**: Caches frequently used data and adjusts partitions for efficiency.
- **Output**: Saves results as a Parquet file or displays them in the notebook.

**Example Insights:**

- Average fare per vendor.
- Total trips per day or hour.
- Most common trip distances.

## Optimization Techniques Used

To ensure the pipeline runs efficiently:

- **Caching**: Stores intermediate results in memory for faster access.
- **Partitioning**: Splits data into manageable chunks for parallel processing.
- **Early Filtering**: Removes invalid records early to reduce data volume.
- **Broadcast Variables**: Shares small lookup tables across executors to avoid shuffling.

Why optimize? These techniques reduce processing time and resource usage, critical for large datasets in production environments.

## Key Features

- **RDD Transformations**: Demonstrates low-level operations like `map`, `filter`, and `reduceByKey`.
- **DataFrame Operations**: Uses SQL-like queries for structured data analysis.
- **Spark SQL**: Enables relational queries on the dataset.
- **Fault Tolerance**: Leverages Spark’s RDD lineage for automatic recovery from failures.
- **Scalability**: Designed to handle datasets of any size by distributing work across a cluster.

## Challenges and Solutions

- **Challenge**: Slow processing due to large dataset size.
  **Solution**: Used caching and repartitioning to balance data across executors.
- **Challenge**: Data quality issues (e.g., missing or invalid records).
  **Solution**: Applied filters to clean data early in the pipeline.
- **Challenge**: Debugging distributed jobs.
  **Solution**: Utilized Spark UI to identify bottlenecks and optimize stages.

## Future Improvements

- Add Spark Streaming for real-time taxi data processing.
- Integrate MLlib for predictive analytics (e.g., fare prediction).
- Deploy on a cloud platform (e.g., AWS EMR, Databricks) for scalability.
- Create visualizations using Matplotlib or Tableau to present insights.

## Learnings

- Understood Spark’s architecture, including driver, executors, and lazy evaluation.
- Mastered RDDs vs. DataFrames for different use cases.
- Gained hands-on experience with data pipelines and ETL processes.
- Learned to optimize Spark jobs using caching, partitioning, and the Spark UI.
- Applied big data techniques to real-world datasets.

## Contributing

Contributions are welcome! To contribute:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Make your changes and commit (`git commit -m "Add new feature"`).
4. Push to the branch (`git push origin feature-branch`).
5. Open a Pull Request.

Please ensure your changes align with the project’s goals and include clear documentation.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Acknowledgments

- NYC Taxi & Limousine Commission for providing the dataset.
- Apache Spark for the powerful distributed processing framework.
- PySpark Documentation for comprehensive guides.

## Contact

For questions or feedback, feel free to reach out via GitHub Issues or connect with me on LinkedIn.

Happy Spark-ing! 🚕✨
