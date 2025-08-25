# (Phase 1) Web-to-FTP Pipeline

## Overview 

A Python ETL (Extract, Transform, Load) pipeline that downloads CSV files from web sources and uploads them to an FTP server with comprehensive error handling and parallel processing capabilities.

## Features

- ✨ **Parallel Processing** - Process multiple data sources simultaneously
- 🔄 **Retry Logic** - Automatic retry with exponential backoff for failed downloads
- 📊 **Comprehensive Logging** - Detailed logs with processing statistics
- 🛡️ **Error Handling** - Graceful error recovery and cleanup
- ⏰ **Scheduling** - Built-in scheduler for automated daily runs
- 🔒 **Secure FTP** - Uses FTP_TLS for secure file transfers
- 📈 **Progress Monitoring** - Real-time processing status and summaries
- 🧹 **Resource Management** - Automatic cleanup of temporary files and connections

## Prerequisites

- Python 3.8+
- Virtual environment
- FTP server (vsftpd configured in WSL Ubuntu)
- Required Python packages (see `requirements.txt`)

## Usage

### Manual Execution

```bash
# Sequential processing
python web_to_ftp.py manual sequential

# Parallel processing (recommended)
python web_to_ftp.py manual parallel
```

### Scheduled Execution

```bash
# Schedule daily run at 22:23 (sequential)
python web_to_ftp.py schedule sequential

# Schedule daily run at 22:23 (parallel)
python web_to_ftp.py schedule parallel
```

## Installation

1. Clone the repository and navigate to the project directory
2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1  # Windows PowerShell
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Set up environment variables in `.venv/Scripts/Activate.ps1`:
   ```powershell
   $Env:FTPHOST = "localhost"
   $Env:FTPUSER = "your_username"
   $Env:FTPPASS = "your_password"
   $Env:FTPTARGET = "/home/your_username/ftp/new"
   ```

## Configuration

Edit `config.json` to define your data sources:

```json
{
  "data_source_name": {
    "URL": "https://example.com/data.csv",
    "PARAMS": {
      "sep": ",",
      "encoding": "utf-8",
      "parse_dates": ["date_column"]
    }
  }
}
```

## Project Structure

```
phase1_web_to_ftp/
├── web_to_ftp.py          # Main ETL pipeline script
├── config.json            # Data source configurations
├── requirements.txt       # Python dependencies
├── etl_pipeline.log       # Processing logs (generated)
├── .venv/                 # Virtual environment
└── README.md             # This file
```

## FTP Server Setup (WSL Ubuntu)

1. Install and configure vsftpd:
   ```bash
   sudo apt install vsftpd
   sudo systemctl enable vsftpd
   sudo systemctl start vsftpd
   ```

2. Create target directory:
   ```bash
   mkdir -p /home/your_username/ftp/new
   chmod 775 /home/your_username/ftp/new
   ```

3. Configure vsftpd for local development (see configuration in the main script)

## Logging

The pipeline generates detailed logs in `etl_pipeline.log` including:
- Processing timestamps
- Success/failure status for each data source
- File sizes and processing times
- Error messages and stack traces
- Processing summaries

## Error Handling

- **Network Issues**: Automatic retry with exponential backoff
- **FTP Failures**: Graceful error reporting and cleanup
- **File Operations**: Safe file handling with proper cleanup
- **Configuration Errors**: Clear error messages for troubleshooting

## Performance

- **Parallel Processing**: Configurable worker threads (default: 4)
- **Connection Management**: Efficient FTP connection pooling
- **Resource Cleanup**: Automatic cleanup of temporary files and connections
- **Memory Efficient**: Processes files individually to minimize memory usage

## Monitoring

Processing summaries include:
- Total sources processed
- Success/failure counts
- Total data volume processed
- Processing time statistics
- Target directory confirmation

## Contribute

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Support

For issues and questions:
1. Check the logs in `etl_pipeline.log`
2. Verify FTP server status: `sudo systemctl status vsftpd`
3. Test FTP connection manually: `ftp localhost`
4. Check environment variables are properly set