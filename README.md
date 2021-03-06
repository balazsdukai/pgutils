# pgutils – A minimal package for querying PostgreSQL

This package provides a couple of funcitons to make working with psycopg2 even quicker.
It naturally evolved from the patterns that I use for database access in several packages, so it made sense to extract it into a standalone package, instead of keep copying the module from one package to another.

There were two guiding principles for developing the functions:

1. No ORM. I didn't need anything fancy and I'm comfortable with SQL. Thus, I didn't want the computational and cognitive overhead of using an ORM package.
2. Keep it minimal. 

Essentially, I kept repeating four actions:

1. connect to PostgresSQL
2. send some SQL
3. receive the results from some SQL
4. disconnect from PostgreSQL

## Install

```
python -m pip install git+https://github.com/balazsdukai/pgutils.git pgutils
```

## Usage

Connect to postgres with full credentials:

```python
import pgutils

conn = pgutils.db.Db(dbname="database name", 
                     host="host name", 
                     port=1234,
                     user="user name", 
                     password="password")
conn.close()
```

Or get the credentials from a `.pgpass` file:

```python
import pgutils

conn = pgutils.db.Db(dbname="database name")
conn.close()
```

Retrive the results of an SQL query.

```python
from psycopg2 import sql
from pgutils import db

query_params = {
    "idx": sql.Identifier("schema.table"),
    "tile": sql.Identifier("field")
}

query = sql.SQL(
    """
    SELECT DISTINCT {tile} FROM {index}
    """
).format(**query_params)

resultset = conn.get_query(query)
```

The `get_query` method wraps `psycopg2.cursor.fetchall()`:

```python
def get_query(self, query: psycopg2.sql.Composable) -> List[Tuple]:
    """DB query where the results need to return (e.g. SELECT)."""
    with self.conn:
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()
```