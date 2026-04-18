# CropGuard

Data engineering pipeline to fetch and store agricultural weather data.

Step 1: Install prerequisites
For macOS:
brew install python
brew install colima docker docker-compose
colima start

For Windows:
Download and install Python from python.org
Download and install Docker Desktop from docker.com
Open Docker Desktop and wait for the engine to start

Step 2: Set up Python environment
For macOS:
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

For Windows:
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

Step 3: Start the database
docker-compose up -d

Step 4: Create database tables
docker exec -i cropguard_db psql -U admin -d cropguard < storage/setup_db.sql

Step 5: Run the pipeline
python orchestration/flow.py

Step 6: Check pipeline dashboard
prefect server start
Open http://127.0.0.1:4200 in your browser to view the flow runs.

Step 7: View data
Open http://localhost:8080 in your browser.
Login: admin@admin.com
Password: admin
Add new server: host 127.0.0.1, port 5433, user admin, password password123.

## AI Usage Declaration
Tool: Gemini
Used for: Debugging Docker port collisions, generating database schema, structuring Prefect scripts
Extent: Boilerplate code generation and debugging assistance
