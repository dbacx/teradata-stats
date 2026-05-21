# VantageOps — Teradata Assessment Suite

Enterprise observability tool for Teradata DBAs.
Analyzes system health, statistics management, and space utilization.

## Quick Start (Windows)

### Prerequisites
- Python 3.10 or higher: https://www.python.org/downloads/
  ⚠️ Check "Add Python to PATH" during installation
- Network access to your Teradata environment

### Installation (first time only)
1. Extract the VantageOps folder to your preferred location
2. Double-click `install.bat`
3. Edit `config\YOUR_CLIENT.env` with your Teradata credentials

### Running the tool
- Double-click `run.bat`
- Or from terminal: `run.bat CLIENTNAME`

### Adding a new client
1. Copy `config\.env.example` to `config\CLIENTNAME.env`
2. Fill in your Teradata connection details
3. Run: `run.bat CLIENTNAME`

## Modules Available
| Module | Description |
|--------|-------------|
| System Information | Cluster health, node specs, capacity |
| Statistics Management | Stale stats, missing stats, DDL remediation |
| Space Assessment | Database space utilization and alerts |

## Security
- Credentials are stored locally in `config/*.env` files
- Never share or commit `.env` files with credentials
- See SECURITY.md for details
