# Web → FTP → PostgreSQL ETL Pipeline

This repository demonstrates an **end-to-end ETL (Extract, Transform, Load) pipeline**.  
The project is divided into two phases:

1. **Phase 1: Web to FTP** – Extracts sanction list data (OFAC) from the web and uploads it to a local FTP server using Python.  
2. **Phase 2: FTP to PostgreSQL** – Loads the CSV files from FTP into a PostgreSQL database using an SSIS package.  

This workflow reflects a hybrid **traditional ETL pipeline** (SSIS-based) combined with **modern scripting (Python)**.

---

## Features

- Automated **file extraction** from the web  
- Secure **file transfer** to an FTP server (For staging)
- ETL processing using **SSIS (SQL Server Integration Services)**  
- Data loading into **PostgreSQL** for downstream analytics  
- Modular project structure to demonstrate each ETL phase  

---

## Phase 1: Web → FTP

- **Language**: Python
- **Goal**: Automate extraction of OFAC sanction list (CSV) from the web and upload it to a local FTP server.  
- **Key Steps**:
  1. Extract OFAC sanction list (SDN, ALT, ADD) via HTTP request.  
  2. Save CSV files locally.  
  3. Upload files to a designated folder on the FTP server (`/ftp/new`).  

📌 See [`phase1_web_to_ftp/README.md`](./phase1_web_to_ftp/README.md) for detailed setup and usage.

---

## Phase 2: FTP → PostgreSQL

- **Tool**: SSIS (SQL Server Integration Services)  
- **Goal**: Loading of CSV files from FTP server into PostgreSQL database.  
- **Key Steps**:
  1. Connect to FTP server (via `FTP Connection Manager.conmgr`).  
  2. Retrieve CSV files into SSIS project (`Extracts/`).  
  3. Configure ODBC connection to PostgreSQL (`pgsql_odbc_64.conmgr`).  
  4. Map CSV schema → PostgreSQL table structure.  
  5. Automate package execution via SQL Server Agent.  

📌 See [`phase2_ftp_to_postgres/README.md`](./phase2_ftp_to_postgres/README.md) for detailed setup and usage.

---

## Requirements

### Phase 1 (Python)
- Python 3.9+
- Virtual Environment Configuration
- Installed dependencies
- Local or remote FTP server for staged files

### Phase 2 (SSIS)
- SQL Server Data Tools (SSDT) with SSIS extension  
- PostgreSQL ODBC Driver (64-bit)  
- PostgreSQL database instance  
- (Optional) SQL Server Agent for automation 

---

## Usage

### Run Phase 1 (Web → FTP)

#### Manual Execution

```bash
cd phase1_web_to_ftp

# Sequential processing
python web_to_ftp.py manual sequential

# Parallel processing (recommended)
python web_to_ftp.py manual parallel
```

#### Scheduled Execution

```bash
# Schedule daily run at 22:23 (sequential)
python web_to_ftp.py schedule sequential

# Schedule daily run at 22:23 (parallel)
python web_to_ftp.py schedule parallel
```

### Run Phase 2 (FTP → PostgreSQL)

1. Open FtpToPostgre.sln in Visual Studio (with SSIS).
2. Deploy main.dtsx package.
3. Schedule with SQL Server Agent (or run manually).

---

## Appreciation

Special Thanks to Sir Josh Dev from Data Engineering Pilipinas (DEP) for the inspiration of this project!
