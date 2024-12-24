Run postgres
```cmd
docker-compose -f compose.yml up -d
```

Create virtual env

```cmd
py -m venv .venv
```

Install requirements

```cmd
pip install -r requirements.txt
```

Add environment variables activate script

```bat
set KORVUS_DATABASE_URL=postgresql+psycopg2://postgresml:postgresml@localhost:5433}/postgresml
```

Activate environment

```cmd
.venv\Scripts\activate.bat
```
