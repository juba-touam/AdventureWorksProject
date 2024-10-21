# AdventureWorksProject

This project is a simple end-to-end data engineering example using Azure technologies. The project plan is outlined below:

![image](https://github.com/user-attachments/assets/59ed6ad1-9709-45bf-b834-5ae9e506af3d)

Firstly, we install SQL Server Management Studio (SSMS) using this link: https://learn.microsoft.com/fr-fr/sql/ssms/download-sql-server-management-studio-ssms?view=sql-server-ver16 andwe need to download AdventureWorksLT2019 from this link: https://learn.microsoft.com/en-us/sql/samples/adventureworks-install-configure?view=sql-server-ver16&tabs=ssms  AdventureWorks Sample Database,and upload it to SQL Server Management Studio (SSMS)

For this project, we follow the steps shown in the schema below and describe each step:

![image](https://github.com/user-attachments/assets/34af2982-af23-4107-a26d-51e5cfc0b81a)

# 1-Data Ingestion:

We use a Self-hosted Integration Runtime to create a connection between SSMS and Azure Data Factory (ADF);
We create a pipeline in ADF, and in the first step, we transfer all tables from the local server (SSMS) to the Azure Data Lake Bronze folder.


