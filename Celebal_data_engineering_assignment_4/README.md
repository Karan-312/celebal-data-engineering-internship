# Week 4 Assignment – Azure Data Factory & Blob Storage

## Overview

This project demonstrates the creation and configuration of Azure resources for data integration using Azure Blob Storage and Azure Data Factory (ADF). The workflow copies a CSV file from a source container to a destination container and verifies successful execution.

---

## Objectives Completed

### Task 1: Resource Group Creation

* Created a Resource Group in Microsoft Azure.
* Used it to organize and manage all resources for the assignment.

### Task 2: Storage Account & Blob Containers

* Created an Azure Storage Account.
* Created the following Blob Containers:

  * `source-data`
  * `destination-data`
* Uploaded the sample CSV file (`Sample - Superstore.csv`) to the source container.

### Task 3: Azure Data Factory Setup

* Created an Azure Data Factory instance.
* Accessed Azure Data Factory Studio.
* Configured a Linked Service connecting ADF to Azure Blob Storage.

### Task 4: IAM Configuration

* Assigned required IAM permissions to allow Azure Data Factory to access the Storage Account.

### Task 5: Data Pipeline Creation & Execution

* Created datasets for source and destination containers.
* Built a Copy Data pipeline to transfer the CSV file.
* Executed the pipeline successfully.
* Verified successful pipeline execution through monitoring output.

### Task 6: Data Copy Verification

* Confirmed that the copied CSV file appeared in the destination container.
* Verified successful data transfer.

---

## Bonus Mini Project

Implemented a workflow using the **Get Metadata** activity before the Copy Data activity.

### Workflow

1. Get Metadata Activity

   * Reads metadata from the source file.
2. Copy Data Activity

   * Copies the CSV file from source container to destination container.

### Result

* Pipeline executed successfully.
* Metadata retrieval completed successfully.
* File copied successfully to destination storage.

---

## Azure Services Used

* Azure Resource Group
* Azure Storage Account
* Azure Blob Storage
* Azure Data Factory (ADF)
* Azure IAM (Role Assignments)

---

## Outcome

Successfully configured Azure Data Factory to connect with Azure Blob Storage, created and executed a data pipeline, verified file transfer, and implemented a metadata-driven workflow as a bonus enhancement.
