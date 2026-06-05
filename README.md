# ATS_Recruitment_Dashboard

This repository contains a Streamlit-based ATS recruitment dashboard.

## Postgres configuration

The dashboard now connects to Postgres via Streamlit secrets. Create a `secrets.toml` file with a `postgres` section:

```toml
[postgres]
dbname = "your_db_name"
user = "your_username"
password = "your_password"
host = "localhost"
port = 5432
schema = "public"
sslmode = "prefer"
```

Place this file under Streamlit secrets configuration for your deployment environment.
