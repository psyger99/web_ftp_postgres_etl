# (Phase 2) FTP-to-Postgre Pipeline

## Overview

This is **Phase 2** of the `web_ftp_postgres_etl` project.  
It covers the process of **loading data from an FTP server into a PostgreSQL database**, completing the second stage of the ETL pipeline.

In this phase, the ETL pipeline does the following:

1. **Connects to an FTP server** to download CSV data files.
2. **Optionally performs light transformation or validation** on the CSVs.
3. **Loads the cleaned data into a PostgreSQL database** table for storage or further processing.

## Features

- Connects to an **FTP server** to retrieve staged CSV files.  
- Validates and parses CSV data before loading.  
- Loads cleaned data into a **PostgreSQL database**.  
- Includes basic logging for tracking pipeline execution.  
- Modular structure for easy extension to new data sources or destinations.  

## Requirements

- FTP server (configured and accessible)
- PostgreSQL 13+ (or higher)  
- Visual Studio
- SQL Server Management Studio (SSMS)

## Usage

1. Open the SSIS Project
- Launch Visual Studio 2022 with SQL Server Data Tools (SSDT).
- Open the solution file: **FtpToPostgre.sln**
2. Configure Connection Managers
- Update FTP Connection Manager with your FTP server credentials.
- Update pgsql_odbc_32/64 Connection Manager with your PostgreSQL connection details.
- Ensure the Flat File Connection Managers (CSV OFAC_ADD, CSV OFAC_ALT, CSV OFAC_SDN) point to the correct local paths under: **Extracts/**
3. Run the Package
- In Solution Explorer, right-click main.dtsx and select: **Execute Package**
- The workflow will download the latest OFAC CSV files from the FTP server → Extracts/. Then, load each CSV into the target PostgreSQL tables (ofac_add, ofac_alt, ofac_sdn).
4. Automate Execution
- Deploy the package to SQL Server Integration Services Catalog, then run it via SQL Server Agent for scheduling.

## Project Structure

```
phase2_ftp_to_postgres/
├── Extracts/
│ ├── OFAC_ADD.csv
│ ├── OFAC_ALT.csv
│ ├── OFAC_SDN.csv
├── FtpToPostgre/
│ ├── bin/
│ ├── obj/
│ ├── CSV OFAC_ADD.conmgr
│ ├── CSV OFAC_ALT.conmgr
│ ├── CSV OFAC_SDN.conmgr
│ ├── FTP Connection Manager.conmgr
│ ├── FtpToPostgre.dtproj
│ ├── FtpToPostgre.dtproj.user
│ ├── main.dtsx
│ ├── pgsql_odbc_64.conmgr
│ ├── Project.params
├── .gitignore
├── FtpToPostgre.sln
```