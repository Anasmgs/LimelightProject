# LimelighProject

LimelightProject is an automated ETL pipeline using near-real time data of 1,462 self-service bike stations to map at any given point and with precision the supply&demand dynamics for alternative urban mobility in Paris urban area.

<img width="860" alt="Jedha - LimeProject _ vAM 1SLIDE" src="https://github.com/Anasmgs/LimelightProject/assets/104752748/05dfce61-6120-4149-b05f-daf95fa5e735">

## Description






 <br>
  
 1. Extract phase on Kafka Connect using Confluent Cloud platform with:
   - Source (Producer-equivalent): GET requests every minute to the [following API](https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records) with real-time comprehensive data on 1,462 bike stations localization, free capacity, bikes available by type etc.
   - Sink (Consumer-equivalent): GCS Bucket to save the raw JSON files obtained
     

<br>

<img width="450" alt="Kafka Connectors" src="https://github.com/Anasmgs/LimelightProject/assets/104752748/440b489c-e263-490a-8897-428e8b122dcc">  




<br>



 2. Transform phase: orchestrated through Airflow
 
 <img width="450" alt="AirflowUI_DAGS" src="https://github.com/Anasmgs/LimelightProject/assets/104752748/2f258dd6-4c1c-426d-b134-f7bac645256b"> 
 
 
 
 
 <br>


 3. Load phase: clean csv file into BigQuery table allowing efficient analysis with SQL queries and potential connection with BI solutions eg. LookerStudio

  <img width="450" alt="AirflowUI_DAGS" src="https://github.com/Anasmgs/LimelightProject/assets/104752748/60739ad1-c3b4-4dfa-80be-46e298208dd3"> 


 ## Getting Started

To get started with Limelight, follow these steps:

### Prerequisites
- Kafka Confluent Cloud Account (400$ credit offered)
- GCP project with two GCS buckets (one bucket for raw data, another as staging area for the cleaned data) and one BigQuery dataset
- Docker

### Installation&Usage

1. Clone the repository to your local machine:
   
   ```bash
    git clone https://github.com/Anasmgs/LimelightProject.git
    cd LimelightProject
   ```
   
(1.a Launch Docker Desktop if not already running)

2. Airflow database set-up
   
   ```bash
   Docker compose up airflow init
   ```
   
3. Launch Airflow server
   
   ```bash
   Docker compose up
   ```
   Airflow will be running at "http://localhost:8080/" (id & pwd : airflow)

5. In Airflow UI:
   - Set-up Google Cloud connection [Doc here](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html)
   - Set-up ENV variables:

  <img width="450" alt="AirflowUI_DAGS" src="https://github.com/Anasmgs/LimelightProject/assets/104752748/a06a1ad2-5bc7-409c-919f-7485d78a77d1"> 
  


## Contributing

Contributions from the community are welcome! If you'd like to contribute to this project, please follow these guidelines:

1. Fork the repository on GitHub.
2. Clone your forked repository to your local machine.
3. Make your changes and test them thoroughly.
4. Create a pull request with a clear description of your changes.

## Author

- [Anas Maghous](https://www.linkedin.com/in/anas-maghous/)


## Acknowledgments

Thanks to the open-source community for providing the tools and libraries that made this project possible.

## References

- https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/gcs/index.html
- https://airflow.apache.org/docs/apache-airflow-providers-google/6.3.0/_api/airflow/providers/google/cloud/transfers/gcs_to_bigquery/index.html
- https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/bigquery.html#update-dataset
- https://cloud.google.com/bigquery/docs/reference/rest/v2/Job


