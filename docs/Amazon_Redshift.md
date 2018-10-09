# Connect to AWS RDS PostgreSQL database

* launch an AWS RDS instance
* install `psycopgs` on every node of spark-cluster

```
pip install psycopg2-binary
```

* find all the connection parameters
  - master-username,
  - password,
  - endpoint,
  - port (5432)
* run the following example python codes
```python
    import psycopg2
    params = {
        'database': database name,
        'user': '****',
        'password': '*****',
        'host': endpoint,
        'port': 5432
    }
    try:
        conn = psycopg2.connect(**params)
    except Exception as er:
        print(str(er))
    cur = conn.cursor()
    # create table
    cur.execute("CREATE TABLE IF NOT EXISTS test (id serial PRIMARY KEY, num integer, data varchar);")
    # insert a row
    cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (200, "JSON"))
    # query data
    cur.execute("SELECT * FROM test;")
    rows = cur.fetchall()
    print(rows)
    cur.close()
    conn.close()
```
