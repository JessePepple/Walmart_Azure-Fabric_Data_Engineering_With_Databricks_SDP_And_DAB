# Walmart_Azure_Data_Engineering_With_SDP_DAB_-_Fabric

## Project Structure
Databricks notebooks and pipeline configurations are located in walmart_project/ and deployed via Databricks Asset Bundles. The databricks.yml file defines the bundle configuration for Dev → Test → Prod promotion. ADF pipeline artefacts are in pipeline/, dataset/, and linkedService/.
This project is an end-to-end data engineering solution designed to demonstrate how a modern, scalable data platform can be built using Azure Data Factory, Azure Data Lake, Databricks, and Microsoft Fabric, while following production-oriented engineering and CI/CD best practices.

The pipeline uses metadata-driven and incremental ingestion with Azure Data Factory from our source Azure SQL DB, allowing datasets to be ingested dynamically without creating separate pipelines for every source table. Azure Data Factory is also integrated with CI/CD practices to support controlled deployment and maintainability across environments. Logic Apps are incorporated into the solution for monitoring and alerting, providing visibility into pipeline failures and operational issues.

Once ingested, the data is landed in the Bronze layer within Databricks using Auto Loader, providing scalable and incremental file ingestion while supporting schema management and evolution.

The Silver layer focuses on cleaning, standardising, and transforming the raw data. I implemented the transformations using Object-Oriented Programming (OOP) by developing reusable transformation classes. This approach makes the transformation logic more modular, maintainable, reusable, and easier to test. The Silver layer also incorporates Delta Lake MERGE operations to handle incremental updates and maintain an up-to-date representation of the data.

For the Gold layer, I used Spark Declarative Pipelines (SDP) and Databricks' Auto CDC Flow to implement automated Slowly Changing Dimension Type 2 (SCD Type 2) processing. This allows historical changes to dimensional data to be retained while providing an accurate representation of how records changed over time.

The curated Gold layer supports dual consumption patterns. For traditional business intelligence and analytical workloads, the data is modelled using Fact and Dimension tables, providing a structured dimensional model suitable for reporting and data analysis. In parallel, I created a One Big Table (OBT) containing denormalised data to support Machine Learning and Data Science workloads, where having a consolidated dataset can simplify feature engineering and model development.

The entire Databricks solution is deployed using Databricks Asset Bundles, allowing the project code and resources to be managed and deployed in a consistent, repeatable manner. This further strengthens the project's CI/CD and infrastructure-as-code approach.

Finally, the curated datasets are made available for downstream consumption through Databricks SQL Warehouse and Microsoft Fabric Data Warehouse. Fabric integration uses OneLake shortcuts, allowing the curated data to be consumed across platforms without unnecessarily duplicating the underlying data.


Overall, the project demonstrates a modern Bronze → Silver → Gold data architecture, combining metadata-driven ingestion, incremental processing, reusable OOP transformations, automated CDC and SCD Type 2 processing, CI/CD, monitoring, and multiple consumption patterns for both Business Intelligence and Machine Learning/Data Science.


<img width="814" height="502" alt="Screenshot 2026-09-05 at 10 56 03" src="https://github.com/user-attachments/assets/9bdca7a5-15b8-44a7-965b-d9b05b24bd77" />

## Overall Project Impact

End-to-End Automation: Automated the complete data lifecycle from metadata-driven and incremental ingestion through transformation and delivery, orchestrated using Azure Data Factory and processed in Azure Databricks with Auto Loader and Spark Declarative Pipelines, significantly reducing manual intervention and improving pipeline reliability.

Version Control & CI/CD: Managed Azure Data Factory pipelines, Databricks notebooks, transformation code, and configurations through GitHub, with automated deployments using Databricks Asset Bundles, ensuring reproducible deployments, consistent environments, and maintainable data engineering workflows.

Scalability & Maintainability: Implemented a metadata-driven ingestion framework and Databricks Auto Loader to support incremental processing, schema evolution, and efficient onboarding of new datasets. Reusable OOP-based transformation classes further improved code modularity, maintainability, and reusability across the Silver layer.

Data Quality & Historical Tracking: Implemented Delta MERGE operations within the Silver layer and automated SCD Type 2 processing in the Gold layer using Spark Declarative Pipelines and Auto CDC Flow, ensuring reliable incremental updates while preserving historical changes for accurate analysis.

Analytics & ML Enablement: Delivered dual consumption models consisting of a dimensional Fact and Dimension star schema for BI and analytical workloads and a One Big Table (OBT) designed for Data Science and Machine Learning use cases, providing flexible access to curated data for different downstream requirements.

Monitoring & Reliability: Integrated Logic Apps for pipeline monitoring and alerting, improving operational visibility and enabling faster identification and response to pipeline failures.

Business Value: Delivered analytics-ready curated datasets to Databricks SQL Warehouse and Microsoft Fabric Data Warehouse, using OneLake shortcuts to enable cross-platform consumption while minimising unnecessary data duplication and improving accessibility for downstream analytical consumers.


## Technologies Used


| Layer                      | Technology                                                 | Description                                                                                                        |
| -------------------------- | ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| **Ingestion**              | Azure Data Factory                                         | Orchestrates metadata-driven and incremental batch ingestion from source systems into the data lake                |
| **Storage**                | Azure Data Lake Storage Gen2                               | Scalable cloud storage for raw, processed, and curated data across the medallion architecture                      |
| **Bronze Ingestion**       | Databricks Auto Loader                                     | Incrementally ingests new files into Databricks with scalable file discovery and schema evolution                  |
| **Processing**             | Azure Databricks / Apache Spark                            | Distributed data processing and transformation across the Bronze, Silver, and Gold layers                          |
| **Transformations**        | PySpark & OOP Classes                                      | Reusable, modular transformation logic implemented using object-oriented programming in the Silver layer           |
| **Incremental Processing** | Delta Lake MERGE                                           | Handles inserts and updates within the Silver layer to maintain current and consistent datasets                    |
| **Gold Curation**          | Spark Declarative Pipelines                                | Manages production-grade Gold transformations and curated datasets                                                 |
| **CDC / SCD Handling**     | Auto CDC Flow / SCD Type 2                                 | Automates change data capture and maintains historical versions of dimension records in the Gold layer             |
| **Data Modeling**          | Star Schema & One Big Table                                | Provides dimensional Fact/Dimension models for BI and an OBT for Data Science and ML workloads                     |
| **Monitoring**             | Azure Logic Apps                                           | Provides pipeline monitoring, failure notifications, and operational alerting                                      |
| **Data Warehouse**         | Databricks SQL Warehouse & Microsoft Fabric Data Warehouse | Serves curated datasets for SQL analytics, BI, reporting, and downstream consumption                               |
| **Data Sharing**           | Microsoft Fabric OneLake Shortcuts                         | Enables Fabric to consume curated data without unnecessary duplication of the underlying datasets                  |
| **CI/CD**                  | GitHub & Databricks Asset Bundles                          | Provides version control and automated, reproducible deployment of Databricks code and resources                   |
| **Architecture**           | Medallion Architecture                                     | Organises data into Bronze, Silver, and Gold layers to separate ingestion, transformation, and curated consumption |



## Phase 1 Data Factory Ingestion


The project began with the creation of a Git repository to enable version control and support the structured deployment of Azure Data Factory artifacts. A dedicated development branch was established to manage feature enhancements and isolate ongoing updates from the main production branch, ensuring a controlled, organized, and collaborative development workflow.


<img width="1436" height="712" alt="Screenshot 2026-09-02 at 14 16 14" src="https://github.com/user-attachments/assets/234d34e2-45a2-4f0c-bf28-0e8a003f06a6" />

<img width="1436" height="712" alt="Screenshot 2026-09-02 at 14 34 57" src="https://github.com/user-attachments/assets/50d6bec3-b0c4-4f30-a7b3-ce653ebc8a1d" />


<img width="1436" height="712" alt="Screenshot 2026-09-02 at 14 38 28" src="https://github.com/user-attachments/assets/3fe79aef-a535-4d51-933e-b785641a5d63" />

During pipeline development, I implemented the JSON watermark method. I created two JSON files: one called cdc.json, which stores a backfilled timestamp set far before the earliest record (to support the initial full load which in this is instance is 1900-01-01), and a second empty JSON file used during the process of shifting from historical backfill into incremental ingestion.

<img width="1436" height="712" alt="Screenshot 2026-09-02 at 18 14 34" src="https://github.com/user-attachments/assets/557bf09f-0164-45d7-af27-e1bde052870f" />

<img width="1436" height="712" alt="Screenshot 2026-09-03 at 01 17 21" src="https://github.com/user-attachments/assets/297de527-076d-4887-bf06-5461a2b804eb" />

An IF Condition activity controls the ingestion workflow based on whether new data is available. When new records are identified, the pipeline processes the data and performs the required backfill based on the latest load_date. If no new data is available, the pipeline automatically removes the previously ingested dataset, helping maintain data integrity and prevent duplicate data from being carried forward.

By combining pipeline parameters, dynamic ingestion logic, JSON watermarking, and conditional processing, this phase provides a flexible ingestion framework that can support multiple SQL tables while reducing pipeline duplication and manual intervention. 

Using the output from the MAX CDC script, an additional column was added in the update_cdc copy activity to backfill data up to the last_load value, ensuring that last_load.json in the data lake accurately reflected the latest processed records. While the pipeline initially processed data successfully, reruns were loading the entire dataset repeatedly. To resolve this, I implemented an IF activity using @greater(activity('SQLToLake').output.dataRead, 0). With this logic, the pipeline only ingests new data when available, preventing duplication of existing records—an approach particularly effective for scheduled pipeline runs. To finalise this pipeline and automate the workflow, I used a ForEach activity to load all the data from SQLDB with no manual intervention
<img width="1436" height="712" alt="Screenshot 2026-09-03 at 02 50 25" src="https://github.com/user-attachments/assets/2aee38ee-c347-4220-a154-445ba2313e4d" />
<img width="1436" height="712" alt="Screenshot 2026-09-03 at 02 50 55" src="https://github.com/user-attachments/assets/997c7c55-8342-4c10-8fce-f26880fc24b8" />
Pipeline was successful, and the cdc_json file was updated according to the process date. I also designed the ingestion framework with two execution approaches. The first is a manual ingestion pipeline, where a user can specify the table they want to ingest through the pipeline parameters. The second is an automated ingestion pipeline, which can process the required datasets without manual intervention.
This approach resulted in a reusable and scalable ingestion framework capable of supporting multiple Azure SQL tables while maintaining incremental processing, state tracking, and controlled data movement into the data lake. The successful pipeline executions and automatic CDC JSON updates demonstrate that the framework can reliably manage successive incremental loads without repeatedly ingesting previously processed data.

<img width="1436" height="712" alt="Screenshot 2026-09-03 at 02 51 05" src="https://github.com/user-attachments/assets/e6f0f9db-542b-4804-ac2f-cab661cf1204" />

## Phase 1.1 Monitoring with Logic Apps

Following the development of the ingestion pipelines, I integrated Azure Logic Apps to provide automated monitoring and alerting for scheduled pipeline runs. This enables pipeline execution status to be tracked and potential failures to be identified promptly, improving the overall reliability and operational visibility of the ingestion process.

<img width="1436" height="712" alt="Screenshot 2026-09-03 at 02 56 03" src="https://github.com/user-attachments/assets/147ca383-fbfd-4d46-aeb9-90f779ca750d" />

<img width="1436" height="712" alt="Screenshot 2026-09-03 at 02 59 53" src="https://github.com/user-attachments/assets/cd84959e-ddcb-47e8-b480-c3f994292183" />

<img width="1436" height="712" alt="Screenshot 2026-09-03 at 03 01 19" src="https://github.com/user-attachments/assets/77918dd2-ff9a-43e1-8c84-ff1a369da188" />

Following the development of the ingestion pipelines, I integrated **Azure Logic Apps via an API call** to monitor the status of scheduled Azure Data Factory pipeline runs. When a pipeline execution fails, the Logic App is triggered and automatically sends an **email notification**, providing immediate visibility of failures and enabling issues to be identified and addressed promptly.


<img width="1436" height="712" alt="Screenshot 2026-09-03 at 03 08 03" src="https://github.com/user-attachments/assets/f3662100-a0ba-4f12-b9f6-7e15b75563a1" />

Once the pipeline development was completed, I committed the final implementation to GitHub through our feature branch. I then created a Pull Request (PR) from the development branch into the main branch, allowing the changes to be reviewed and validated before being merged into the main codebase. This provided a controlled deployment workflow and ensured that the completed pipeline was version-controlled and maintained within the project repository.

<img width="1436" height="712" alt="Screenshot 2026-09-03 at 03 10 10" src="https://github.com/user-attachments/assets/f85432f9-f76b-4983-9142-1d9877e3eb6b" />
<img width="1436" height="712" alt="Screenshot 2026-09-03 at 03 11 32" src="https://github.com/user-attachments/assets/b45360c7-db02-42ad-8cb5-297ed1885731" />


## Phase 2 Bronze Ingestion With AutoLoaders
With the Azure Data Factory ingestion framework completed, I moved on to preparing the Databricks environment for incremental ingestion and Bronze-layer processing using Auto Loader.

Before implementing the Bronze ingestion pipelines, I established the required Unity Catalog architecture and Azure storage access. This involved creating the appropriate catalogs and external locations and configuring the required IAM permissions so that Databricks could securely interact with the Azure Data Lake Storage Gen2 environment.

The external locations provided Databricks with controlled access to the relevant data lake paths, while the IAM configuration ensured that the Databricks environment had the appropriate permissions to read and write data without exposing unnecessary access.

I also initialised Databricks Asset Bundles at this stage to establish the deployment structure for the Databricks components. This provided the foundation for managing notebooks, configurations, and other Databricks resources through version control and deploying them consistently across environments.

Once the Unity Catalog, external locations, IAM configuration, and Asset Bundle structure were established, the environment was ready for the next stage: implementing Databricks Auto Loader to incrementally ingest the data into Bronze Delta tables.

<img width="1440" height="680" alt="Screenshot 2026-09-05 at 11 30 50" src="https://github.com/user-attachments/assets/dae0c801-186f-4416-a0cc-249e7895423f" />

After creating IAM roles and permissions I proceeded to create external locations in our Databricks external locations

<img width="1436" height="712" alt="Screenshot 2026-09-04 at 09 21 19" src="https://github.com/user-attachments/assets/94462be4-850c-4e7b-905b-d36d729c1635" />
<img width="1436" height="712" alt="Screenshot 2026-09-04 at 09 20 17" src="https://github.com/user-attachments/assets/2c49f812-5811-4dfe-af4e-b0327567eed9" />
<img width="1436" height="712" alt="Screenshot 2026-09-04 at 09 19 18" src="https://github.com/user-attachments/assets/0dd379e2-7939-4a39-8d89-19e212b6cbdb" />

After establishing the Databricks environment, Unity Catalog structure, external locations, IAM configuration, and Databricks Asset Bundle, I began implementing the Bronze-layer ingestion using Databricks Auto Loader.

To avoid creating separate ingestion logic for each dataset, I implemented a loop-based ingestion process that dynamically iterates through the required datasets and applies the same Auto Loader framework across all of them. This allowed the entire set of datasets to be ingested through a consistent and reusable process.

Auto Loader was configured to support incremental and idempotent ingestion, ensuring that only newly available files were processed while previously ingested data was not unnecessarily reprocessed. This provides reliable ingestion behaviour and allows the Bronze layer to scale as additional data arrives in the data lake.

The resulting implementation provided a fully automated Bronze ingestion framework, reducing repetitive code and manual intervention while ensuring that datasets were consistently ingested into their respective Bronze tables.

<img width="1440" height="484" alt="Screenshot 2026-09-05 at 11 42 37" src="https://github.com/user-attachments/assets/90477696-9134-469d-9bf5-3db12d57e393" />

<img width="1423" height="712" alt="Screenshot 2026-09-04 at 10 57 47" src="https://github.com/user-attachments/assets/825a54e5-a9bc-41d7-93a7-dbc6c52127f0" />

## Phase 3 Silver Transformation & Enrichment

With the Bronze ingestion successfully implemented using Auto Loader, I moved into the next stage of the project: cleaning, standardising, and enriching the raw datasets within the Silver layer.

To make the transformation process reusable and maintainable, I implemented the transformation logic using Python classes and Object-Oriented Programming (OOP). This allowed common transformation operations to be encapsulated into reusable methods and applied consistently across multiple datasets, reducing code duplication and simplifying future maintenance.

The Silver transformations included handling null values, adding a CDC timestamp column, and deduplicating records to improve the quality and consistency of the data. These transformations prepared the raw Bronze datasets for downstream analytical processing and Gold-layer curation.

For incremental updates, I implemented Delta Lake MERGE INTO operations to perform reliable upserts into the Silver tables. This allowed new records to be inserted while existing records could be updated based on the relevant business keys, ensuring that the Silver layer maintained an accurate and current representation of the source data.

Overall, this stage established a clean, enriched, and incrementally maintained Silver layer, while the use of reusable Python classes provided a modular foundation for applying consistent transformation logic across the entire data platform.


<img width="1440" height="680" alt="Screenshot 2026-09-05 at 11 38 20" src="https://github.com/user-attachments/assets/b9ae7a00-7c8e-4e4b-9f3c-bb54796db427" />

<img width="1440" height="680" alt="Screenshot 2026-09-05 at 11 38 12" src="https://github.com/user-attachments/assets/d36bbd2c-a00e-494e-a758-5a0f1fbbf633" />

<img width="1440" height="680" alt="Screenshot 2026-09-05 at 11 38 27" src="https://github.com/user-attachments/assets/25f4e8de-61e3-4be4-b171-8b57a3529a9d" />

<img width="1440" height="680" alt="Screenshot 2026-09-05 at 11 38 36" src="https://github.com/user-attachments/assets/09e3a5bc-6a37-4f28-818a-1edd855abc86" />

<img width="1440" height="680" alt="Screenshot 2026-09-05 at 11 38 51" src="https://github.com/user-attachments/assets/25cae532-9825-4d79-b79d-6b88164b250b" />

<img width="1440" height="680" alt="Screenshot 2026-09-05 at 11 39 02" src="https://github.com/user-attachments/assets/8691f063-e3ee-4747-9279-38292ba52668" />



<img width="1440" height="680" alt="Screenshot 2026-09-05 at 11 39 11" src="https://github.com/user-attachments/assets/a1000d4a-c55f-4803-8d0c-0ed26a73e1a3" />

<img width="1440" height="680" alt="Screenshot 2026-09-05 at 11 39 20" src="https://github.com/user-attachments/assets/d1019c5b-5861-4c24-8c84-1556c6a49b98" />


## Phase 4 Gold Curated Data With SDP And SCD Type 2 and 1

With the Silver layer successfully created and incrementally maintained, I moved into the Gold layer, where the cleaned datasets were curated into analytics-ready structures for downstream consumption.

I created both Fact and Dimension tables using a dimensional star schema, alongside a One Big Table (OBT) to support alternative analytical and Data Science workloads. For the Dimension tables, I implemented SCD Type 2 to preserve historical changes and maintain a complete history of dimensional records. For the Fact tables and OBT, I implemented SCD Type 1-style upserts, ensuring that the datasets maintained the latest available values without retaining historical versions.

To implement this efficiently, I utilised Spark Declarative Pipelines (SDP) to define and manage the Gold-layer transformations and data flows. This provided a structured and maintainable approach to building the curated datasets while supporting automated processing.

I also implemented data quality expectations within the pipelines to validate the data before it was made available for analytical consumption. These expectations provided controls around the quality and integrity of the curated datasets, helping prevent poor-quality records from being served to downstream consumers.

Overall, this stage transformed the clean Silver datasets into business-ready analytical models, providing both historical dimensional analysis through SCD Type 2 and current-state Fact/OBT datasets for efficient downstream consumption.


### Dim_Customers
<img width="1423" height="712" alt="Screenshot 2026-09-04 at 22 08 39" src="https://github.com/user-attachments/assets/efc8b1a1-5b8b-44e7-9dc4-3a57360fc945" />

### Dim Customer's Data Quality Checks
<img width="1423" height="712" alt="Screenshot 2026-09-04 at 22 09 31" src="https://github.com/user-attachments/assets/6d01b0b1-f813-4041-9fe5-c8146b63b69a" />

### Dim_Employees
<img width="1423" height="712" alt="Screenshot 2026-09-04 at 22 08 17" src="https://github.com/user-attachments/assets/fdcff857-ccfc-4e09-8823-21a1e9fdcd1f" />


### Dim_Employees's Data Quality Checks
<img width="1423" height="712" alt="Screenshot 2026-09-04 at 22 08 25" src="https://github.com/user-attachments/assets/7e2ae2b7-a116-4892-b63e-34201290e16d" />


### Dim_Orders 
<img width="1423" height="712" alt="Screenshot 2026-09-04 at 22 08 02" src="https://github.com/user-attachments/assets/1f82cda8-b2cf-446a-acb1-6b69d5206b51" />

### Dim_Order's Data Quality Checks
<img width="1423" height="712" alt="Screenshot 2026-09-04 at 22 07 53" src="https://github.com/user-attachments/assets/323369a6-d823-4ecc-acaf-619b7cf79f70" />

### Dim_Products

<img width="1423" height="712" alt="Screenshot 2026-09-04 at 22 07 19" src="https://github.com/user-attachments/assets/0d61250a-ebc4-40ec-adc6-df67db9de6c2" />

### Dim_Products's Data Quality Checks
<img width="1423" height="712" alt="Screenshot 2026-09-04 at 22 07 27" src="https://github.com/user-attachments/assets/8af2df20-05c9-41af-a129-44e84858be31" />

### Dim_Stores
<img width="1423" height="712" alt="Screenshot 2026-09-04 at 22 06 53" src="https://github.com/user-attachments/assets/d3561024-a6b4-4e61-908f-8b0109c0825f" />

### Dim_Store's Data Quality Checks

<img width="1423" height="712" alt="Screenshot 2026-09-04 at 22 07 01" src="https://github.com/user-attachments/assets/5e490ca2-4dd2-4efc-9fce-4a639912d33c" />

### Fact_Order_Items
<img width="1423" height="712" alt="Screenshot 2026-09-04 at 22 06 30" src="https://github.com/user-attachments/assets/d63445f2-1457-42a1-bb9e-e72509308cd9" />

### Fact_Order_Item's Data Quality Checks

<img width="1423" height="712" alt="Screenshot 2026-09-04 at 22 06 38" src="https://github.com/user-attachments/assets/6bcfb876-ba3c-4166-b387-00dc28ed0506" />

### Walmart_OBT

<img width="1423" height="712" alt="Stop" src="https://github.com/user-attachments/assets/da197cde-99e2-48de-93c6-5a869585caf2" />

### Walmart_OBT's Data Quality Checks

<img width="1423" height="712" alt="Screenshot 2026-09-04 at 22 06 16" src="https://github.com/user-attachments/assets/98b4073f-f63b-43e1-af49-ca203700e7e6" />


## Expectations were met successfully

After 

<img width="1423" height="712" alt="Screenshot 2026-09-04 at 23 47 51" src="https://github.com/user-attachments/assets/8631d849-fd12-4f12-8759-d714bba77876" />

























