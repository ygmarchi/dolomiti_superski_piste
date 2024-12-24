# workaround, l'import con dbeaver non funziona

import psycopg2
conn = psycopg2.connect("host=localhost dbname=postgresml user=postgresml password=postgresml port=5433" )
cur = conn.cursor()
with open('data/dolomiti_superski_piste.csv', 'r') as f:
    # Notice that we don't need the csv module.
    next(f) # Skip the header row.
    cur.copy_from(f, 'dolomiti_superski_piste', sep=';', null='')

conn.commit()