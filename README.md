#STREAMING COMPANY DATA ANALYSIS:


Xyz is a Streaming service provides Movies, TV Shows and other content to their Subscribers. 
Developed an end-to-end data engineering pipeline for xyz company to analyze data and perform customer segmentation. 
This will help the company to better understand their customers' needs.

Services used: SQL Server, Azure Data Factory, Azure Datalake storage (ADLS), Pyspark, Azure Databricks, Azure Synapse Analytics

DATA INGESTION :

Migrating from SQL server to ADLS using Data Factory
Migrating data from SQL server to Cloud is little challenging. So, with help of Self-Hosted Integration Runtime functionality in ADF, we can easily Migrate to cloud at any scale.
Copied all tables with one single copy activity in Foreach using ADF. So, all tables that were available, will actually iterate through it and copied into Target location.

DATA TRANSFORMATIONS USING DATABRICKS:

Create transformedData container in ADLS and Database in Databricks.
Using SAS token, connected the landing container to access the files in Databricks i.e Mounting through Secret Scope.
Do cleaning, Removing Nulls, Outliers, validation checks and other basic transformations will be done. 
Doing some transformations, finding some trends and insights on the data based on the business requirement.
Then, all files get written into transformedData container in ADLS and also saved as Delta tables.

SYNAPSE ANLYTICS:
Is a Data Warehouse system, we can actually perform all queries in simple language SQL by creating SQL pool. 
And it also supports to create Pipelines and run Spark applications by using Spark pool i.e Cluster.

Azure Key vault:
Here, we have used Azure Key vault to store secret keys for more secured connection.   

Flow of the Project:
OnPrem SQL server --> ADLS --> Databricks (Delta tables) -- > ADLS -- > Azure Synapse Analytics




![DataPipeline_Architecture](https://github.com/Rajasekhar4314/Streamingcompany_DataPipeline/assets/82395124/e72d895a-dbe3-4f70-8e73-65b10b006fba)
