# airflow-json-chains

Airflow workflows defined using JSON graphs.
Docker containers do the work.
Airflow only orchestrates.

## Quick Start

1. Clone the repository
2. Set up the data directory permissions:
   ```bash
   chmod 777 data/
   ```
3. Start Airflow:
   ```bash
   docker-compose up
   ```
4. Access the web UI at http://localhost:8080

The database will be created automatically on first run.
