Cockroach Cloud TLS (Docker)

The Cockroach Cloud UI gives a PowerShell command that downloads the CA to:

  %AppData%\postgresql\root.crt   (Windows)

That path is on your PC only. Docker runs Linux and mounts this repo folder instead:

  cropguard\certs\root.crt  ->  /app/certs/root.crt  (inside the container)

docker-compose sets PGSSLROOTCERT=/app/certs/root.crt for api and pipeline.

Option A — After running the UI’s PowerShell command, copy the file:

  Copy-Item "$env:APPDATA\postgresql\root.crt" -Destination ".\certs\root.crt"

(Run from the cropguard repo folder.)

Option B — Download straight into the repo (use YOUR cert URL from the Cockroach Connect screen, not a copied cluster id):

  mkdir certs -Force
  Invoke-WebRequest -Uri "https://cockroachlabs.cloud/clusters/<YOUR-CLUSTER-ID>/cert" -OutFile ".\certs\root.crt"

Then:

  docker compose up -d --build

Alternative (dev only): use sslmode=require in DATABASE_URL and remove PGSSLROOTCERT plus the ./certs volume from api/pipeline in docker-compose.yml if you do not want a CA file.
