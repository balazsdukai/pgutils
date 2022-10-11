import os

from pytest import fixture

from connection import PostgresConnection, PostgresTableIdentifier, PostgresFunctions


@fixture(scope="session")
def conn():
    conn = PostgresConnection(
        dbname=os.environ["DB_NAME"],
        hostname=os.environ["DB_HOST"],
        username=os.environ["DB_USERNAME"],
        port=os.environ["DB_PORT"],
        password=os.environ["DB_PASSWORD"]
    )
    # options=f"-c search_path={self.schema_name}",
    # cursor_factory=psycopg2.extras.DictCursor,

    PostgresFunctions(conn)

    yield dict(conn=conn, schema=os.environ["DB_SCHEMA"],
               table=os.environ["DB_TABLE"],
               tblid = PostgresTableIdentifier(os.environ["DB_SCHEMA"],
                                               os.environ["DB_TABLE"]))
    conn.close()
