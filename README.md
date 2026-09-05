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
