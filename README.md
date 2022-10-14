# pgutils – A minimal package for querying PostgreSQL

This package provides a couple of funcitons to make working with psycopg even quicker.
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
from pgutils import PostgresConnection

conn = PostgresConnection(dbname="database name",
                          host="host name",
                          port=1234,
                          user="user name",
                          password="password")
```

Or get the credentials from a `.pgpass` file:

```python
from pgutils import PostgresConnection

conn = PostgresConnection(dbname="database name")
conn.close()
```

However, `PostgresConnection` **does not return an active Connection**.
The `PostgresConnection` object stores the connection parameters so that its methods can connect to the database and execute the queries.
This is in line with the recommendations of [psycopg3](https://www.psycopg.org/psycopg3/docs/basic/usage.html#connection-context).
Thus, the methods of `PostgresConnection` initiate and close their own connection, by using the Psycopg 3 Connection as a context manager.

For instance:

```python
def send_query(self, query: Composable):
    with connect(self.dsn) as conn:
        conn.execute(query)
```

### Retrive the results of an SQL query.

```python
from pgutils import PostgresConnection, PostgresTableIdentifier, inject_parameters

conn = PostgresConnection(dbname="database name")

query_params = {
    "index": PostgresTableIdentifier("myschema", "mytable"),
    "tile": "some_column"
}
query = inject_parameters("SELECT DISTINCT {tile} FROM {index}", query_params)
resultset = conn.get_query(query)

conn.close()
```

The `get_query` method wraps `psycopg.cursor.fetchall()`:

```python
def get_query(self, query: psycopg.sql.Composable) -> List[Tuple]:
    """DB query where the results need to return (e.g. SELECT)."""
    with connect(self.dsn) as conn:
        return conn.execute(query).fetchall()
```

### Execute some SQL without a return value

```python
from pgutils import PostgresConnection, PostgresTableIdentifier, inject_parameters

conn = PostgresConnection(dbname="database name")

query_params = {
    "table": PostgresTableIdentifier("myschema", "mytable"),
}
query = inject_parameters("CREATE INDEX my_index ON {table} (some_column)", query_params)
conn.send_query(query)
```