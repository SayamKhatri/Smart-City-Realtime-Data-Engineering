# Smart City Real-Time Data Engineering Pipeline

The Smart City Real-Time Data Engineering Pipeline is designed to ingest, process, and visualize real-time data from IoT devices deployed across urban environments. By leveraging a suite of modern data engineering tools, this project facilitates the collection and analysis of data related to traffic conditions, weather patterns, emergency incidents, and more, thereby supporting informed decision-making for city management and enhancing the quality of urban life.

## System Architecture

<img width="1242" alt="Screenshot 2025-03-16 at 10 50 33 AM" src="https://github.com/user-attachments/assets/9820dd33-8529-4d7c-b78a-9f2645f0fd25" />


## Technologies Used

- **Apache Kafka**: Serves as the backbone for real-time data ingestion, enabling the collection of high-throughput data streams from various IoT devices.
- **Apache Spark**: Utilized for real-time data processing and analytics, allowing for the transformation and analysis of streaming data.
- **Docker**: Facilitates containerization of services, ensuring consistent deployment and scalability across different environments.
- **AWS Services**:
  - **S3**: Provides scalable storage for raw and processed data.
  - **Glue**: Automates the ETL (Extract, Transform, Load) process, cataloging data and preparing it for analysis.
  - **Athena**: Enables serverless querying of data stored in S3 using standard SQL.
  - **Redshift**: Acts as a data warehouse for complex analytical queries and reporting.
  - **QuickSight**: Offers data visualization capabilities to create interactive dashboards and reports.

## Project Structure

The repository is organized as follows:

- **`jobs/`**: Contains Spark job scripts responsible for processing data streams from Kafka and writing the transformed data to AWS S3.
- **`.gitignore`**: Specifies files and directories to be ignored by Git, maintaining a clean version control history.
- **`docker-compose.yml`**: Defines the Docker services for Kafka, Zookeeper, and Spark, orchestrating their deployment and interaction.
- **`requirements.txt`**: Lists the Python dependencies required for the Spark jobs and other scripts.

## Setup Instructions

To deploy and run the Smart City Real-Time Data Engineering Pipeline, follow these steps:

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/SayamKhatri/Smart-City-Realtime-Data-Engineering.git
   cd Smart-City-Realtime-Data-Engineering
   ```

2. **Configure AWS Credentials**:

   Ensure that your AWS credentials are configured properly, either by setting environment variables or using the AWS credentials file.

3. **Start Docker Services**:

   Use Docker Compose to start the Kafka, Zookeeper, and Spark services:

   ```bash
   docker-compose up -d
   ```

4. **Submit Spark Jobs**:

   Execute the Spark job to process data streams:

   ```bash
   docker exec -it [spark-master-container-name] spark-submit --master spark://spark-master:7077 jobs/[your-spark-job].py
   ```

   Replace `[spark-master-container-name]` with the actual name of your Spark master container and `[your-spark-job].py` with the specific job script you intend to run.


## Data Flow

1. **Data Ingestion**: IoT devices send real-time data to Kafka topics.
2. **Data Processing**: Spark Streaming consumes data from Kafka, processes it, and writes the transformed data to AWS S3.
3. **Data Storage**: Processed data is stored in S3 in a structured format, such as Parquet.
4. **Data Cataloging**: AWS Glue crawlers catalog the data, making it available for querying.
5. **Data Analysis**: AWS Athena queries the cataloged data, providing insights and facilitating reporting.
6. **Data Visualization**: AWS QuickSight visualizes the data, creating interactive dashboards for stakeholders.

## Conclusion

This project showcases the integration of various data engineering tools to build a robust, scalable, and efficient pipeline for real-time data processing in a smart city context. By harnessing the power of Apache Kafka, Apache Spark, Docker, and AWS services, it provides a comprehensive solution for ingesting, processing, storing, and visualizing urban data, thereby contributing to smarter and more responsive city management. 
