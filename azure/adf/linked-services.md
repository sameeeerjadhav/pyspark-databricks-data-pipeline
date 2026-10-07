# ADF linked services

Design only. These services were not created in an Azure subscription.

## LS_ADLS_ShopSphere

- Type: Azure Data Lake Storage Gen2
- Authentication: system-assigned managed identity on the Data Factory
- URL: `https://<storage-account>.dfs.core.windows.net`
- The identity needs `Storage Blob Data Contributor` on the `shopsphere` container only

Do not store the account key in the linked service. If a secret is unavoidable in another environment, reference Azure Key Vault. This project does not include a vault.

## LS_ADB_ShopSphere

- Type: Azure Databricks
- Authentication: managed identity or a Databricks personal access token stored in Key Vault
- Domain: `https://<workspace>.azuredatabricks.net`
- The cluster used by the pipeline is a job cluster, not an all-purpose cluster left running all day

## LS_KeyVault_ShopSphere

- Type: Azure Key Vault
- Used only if a token cannot be avoided
- Secret names stay in ADF parameters. Secret values stay in the vault

The operational source, if ShopSphere later extracts from PostgreSQL or MySQL, would be a separate linked service. This repository does not connect to a database. ADF would use a copy activity to land extracts in `raw/` and the Spark code would stay the same.
