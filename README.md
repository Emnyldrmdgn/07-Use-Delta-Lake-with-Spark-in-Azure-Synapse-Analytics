# 07-Use-Delta-Lake-with-Spark-in-Azure-Synapse-Analytics
The script provisions an Azure Synapse Analytics workspace and an Azure Storage account to host the data lake, then uploads a data file to the data lake.

# DP-203: Data Engineering on Microsoft Azure - Module 07: Use Delta Lake

## Overview

This module demonstrates how to use Delta Lake on Azure Synapse Analytics to perform ETL operations and manage data in a scalable, reliable, and performant way. You will work with Delta tables, perform streaming data operations, and understand the differences between external and managed Delta tables.

## Table of Contents

1. [Create Delta Tables](#create-delta-tables)
2. [External and Managed Tables](#external-and-managed-tables)
3. [Use Delta Tables for Streaming Data](#use-delta-tables-for-streaming-data)
4. [Query Delta Tables from Serverless SQL Pools](#query-delta-tables-from-serverless-sql-pools)
5. [Delete Azure Resources](#delete-azure-resources)

## 1. Create Delta Tables

Delta Lake allows you to manage large amounts of data and query it efficiently. You will create Delta tables and use them for different types of workloads. 

### Steps:
- Create a **Database**:
    ```python
    spark.sql("CREATE DATABASE AdventureWorks")
    ```

- Create an **External Table**:
    ```python
    spark.sql("CREATE TABLE AdventureWorks.ProductsExternal USING DELTA LOCATION '{0}'".format(delta_table_path))
    ```

- View the Table Description:
    ```python
    spark.sql("DESCRIBE EXTENDED AdventureWorks.ProductsExternal").show(truncate=False)
    ```

- Query the External Table using SQL:
    ```sql
    %%sql
    USE AdventureWorks;
    SELECT * FROM ProductsExternal;
    ```

- Create a **Managed Table**:
    ```python
    df.write.format("delta").saveAsTable("AdventureWorks.ProductsManaged")
    ```

- Query the Managed Table using SQL:
    ```sql
    %%sql
    USE AdventureWorks;
    SELECT * FROM ProductsManaged;
    ```

## 2. External and Managed Tables

- **External Tables**: These are defined by the path to the Parquet files and are not managed by the Spark metastore.
- **Managed Tables**: These are managed by the Spark metastore, and their data files are handled automatically.

### Steps to compare:
1. **List Tables in Database**:
    ```sql
    %%sql
    USE AdventureWorks;
    SHOW TABLES;
    ```

2. **Drop Tables**:
    ```sql
    %%sql
    USE AdventureWorks;
    DROP TABLE IF EXISTS ProductsExternal;
    DROP TABLE IF EXISTS ProductsManaged;
    ```

- **Observe the Data Files**:
    - **External Table**: The data files will remain in their location.
    - **Managed Table**: The data files will be deleted.

## 3. Use Delta Tables for Streaming Data

Delta Lake supports streaming data and allows you to use Delta tables as both a source and a sink for streams.

### Steps:
1. **Create a Streaming Data Source**:
    ```python
    inputPath = '/data/'
    mssparkutils.fs.mkdirs(inputPath)

    jsonSchema = StructType([
        StructField("device", StringType(), False),
        StructField("status", StringType(), False)
    ])

    iotstream = spark.readStream.schema(jsonSchema).option("maxFilesPerTrigger", 1).json(inputPath)
    ```

2. **Write Streaming Data to a Delta Table**:
    ```python
    delta_stream_table_path = '/delta/iotdevicedata'
    checkpointpath = '/delta/checkpoint'
    deltastream = iotstream.writeStream.format("delta").option("checkpointLocation", checkpointpath).start(delta_stream_table_path)
    ```

3. **Create a Catalog Table Based on Streaming Data**:
    ```python
    spark.sql("CREATE TABLE IotDeviceData USING DELTA LOCATION '{0}'".format(delta_stream_table_path))
    ```

4. **Query the Streaming Data**:
    ```sql
    %%sql
    SELECT * FROM IotDeviceData;
    ```

5. **Add More Data to the Stream**:
    ```python
    more_data = '''{"device":"Dev1","status":"ok"}...'''
    mssparkutils.fs.put(inputPath + "more-data.txt", more_data, True)
    ```

6. **Stop the Stream**:
    ```python
    deltastream.stop()
    ```

## 4. Query Delta Tables from Serverless SQL Pools

Azure Synapse Analytics supports querying Delta Lake tables using the serverless SQL pool. You can run SQL queries directly on Delta tables from Synapse Studio.

### Steps:
1. **Select Data from Delta Folder**:
    ```sql
    SELECT
        TOP 100 *
    FROM
        OPENROWSET(
            BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/delta/products-delta/',
            FORMAT = 'DELTA'
        ) AS [result]
    ```

2. **Query Delta Tables in Synapse SQL Pool**:
    ```sql
    USE AdventureWorks;
    SELECT * FROM Products;
    ```

## 5. Delete Azure Resources

To avoid unnecessary Azure costs, make sure to delete the resources after completing the lab.

### Steps:
1. **Navigate to Azure Portal** > **Resource Groups**.
2. Select the **dp203-xxxxxxx** resource group.
3. At the top of the Overview page, click **Delete Resource Group**.
4. Enter the resource group name to confirm and select **Delete**.

This will delete the Azure Synapse workspace and the associated resources.

---

## Conclusion

In this module, you've learned how to work with Delta Lake in Azure Synapse Analytics. You've created Delta tables, managed external and managed tables, worked with streaming data, and queried Delta tables using both Spark and SQL pools. Finally, you've seen how to delete resources to avoid unnecessary charges.

---
## Screenshots

![lab72](https://github.com/user-attachments/assets/350e8894-b6fc-408e-b974-9fdafdc367c6)
![lab73](https://github.com/user-attachments/assets/72ef1aa6-b835-4695-9770-a27a5cebb4ae)
![lab74](https://github.com/user-attachments/assets/85f58fe0-442d-4a92-9981-ad355b80e6b8)
![lab75](https://github.com/user-attachments/assets/5dcb9888-bbca-4c6c-a799-34cf935de18f)
![lab755](https://github.com/user-attachments/assets/890e28a8-853d-4eac-859e-7a6eb7f063af)
![lab76](https://github.com/user-attachments/assets/30dbd5f8-c5a6-4f17-8fe8-2d3c8b6ba69c)
![lab77](https://github.com/user-attachments/assets/56993d27-301c-4796-b468-b1fcb3b1a112)
![lab78](https://github.com/user-attachments/assets/c5d9bdcf-e453-426a-84cb-7c1fed304b63)
![lab79](https://github.com/user-attachments/assets/61872ed3-5cb7-4eae-9d24-4dfd9c96d5c1)
![lab79](https://github.com/user-attachments/assets/834374b2-81db-4cd7-bd50-59d2539a3538)
![lab792](https://github.com/user-attachments/assets/ff5264a8-0c0e-43a7-a68d-fd33a0bb4f91)
![lab7son](https://github.com/user-attachments/assets/1a37f622-338a-4541-9f68-11e6ce60d1b5)
