import os

from psycopg.sql import SQL

from pgutils import PostgresTableIdentifier, inject_parameters, PostgresConnection



class TestConnection:
    dbname = os.environ["DB_NAME"],
    hostname = os.environ["DB_HOST"],
    username = os.environ["DB_USERNAME"],
    port = os.environ["DB_PORT"],
    password = os.environ.get("DB_PASSWORD")

    def test_dsn(self):
        conn = PostgresConnection(dbname=self.dbname[0], host=self.hostname[0],
                                  user=self.username[0], port=self.port[0],
                                  password=self.password)
        print(conn.dsn)
        assert conn.dsn
        print(conn.dsn_gdal)
        conn.user = "myuser"
        assert "myuser" in conn.dsn

        dsn = f"dbname={self.dbname[0]} user={self.username[0]} port={self.port[0]} host={self.hostname[0]}"
        conn = PostgresConnection(dsn=dsn)
        print(conn.dsn)
        assert conn.dsn
        print(conn.dsn_gdal)
        conn.user = "myuser"
        assert "myuser" in conn.dsn

    def test_password_none(self):
        conn = PostgresConnection(dbname=self.dbname[0], host=self.hostname[0],
                                  user=self.username[0], port=self.port[0],
                                  password=None)



def test_tableref():
    tref = PostgresTableIdentifier("schema", "table")
    sql_format = "select * from {}".format(tref)
    sql_fstring = f"select * from {tref}"
    expected = 'select * from schema.table'
    assert sql_format == expected
    assert sql_fstring == expected


def test_inject_parameters():
    query = SQL('select * from {tbl} where field1 = {val};')
    query_params = {
        'tbl': PostgresTableIdentifier('myschema', 'mytable'),
        'val': 1
    }
    query = inject_parameters(sql=query, params=query_params)
    expect = "Composed([SQL('select * from '), Identifier('myschema', 'mytable'), SQL(' where field1 = '), Literal(1), SQL(';')])"
    assert str(query) == expect


def test_inject_parameters_str(conn):
    query = SQL("SELECT * FROM pg_namespace WHERE pg_namespace.nspname = {tbl};")
    tbls = conn["tblid"].schema.str
    query_params = {'tbl': tbls,}
    query = inject_parameters(sql=query, params=query_params)
    qstr = conn["conn"].print_query(query)
    expect = "SELECT * FROM pg_namespace WHERE pg_namespace.nspname = 'wippolder';"
    assert qstr == expect


def test_get_fields(conn):
    tbl = conn["tblid"]
    res = conn["conn"].get_fields(tbl)
    print(res)
    assert len(res) > 0


def test_count_nulls(conn):
    res = conn["conn"].count_nulls(conn["tblid"])
    print(res)


def test_count(conn):
    res = conn["conn"].get_count(conn["tblid"])
    print(res)
    assert isinstance(res, int)


def test_head(conn):
    res = conn["conn"].get_head(conn["tblid"])
    print(res)


def test_head_md_empty_table(conn):
    res = conn["conn"].get_head(conn["tblid"], md=True)
    print(res)


def summary_md(fields, null_count):
    if len(fields) != len(null_count):
        return None
    metacols = ["column", "type", "NULLs"]
    header = " ".join(["|", " | ".join(metacols), "|"])
    header_separator = " ".join(["|", " | ".join("---" for c in metacols), "|"])
    mdtbl = "\n".join([header, header_separator]) + "\n"
    _missing_vals = {rec["column_name"]: rec["missing_values"] for rec in
                     null_count}
    for colname, coltype in fields:
        metarow = "| "
        metarow += f"**{colname}**" + " | "
        metarow += f"`{coltype}`" + " | "
        metarow += str(_missing_vals[colname]) + " |" + "\n"
        mdtbl += metarow
    # remove the last \n
    return mdtbl[:-1]

def test_summary(conn):
    fields = conn["conn"].get_fields(conn["tblid"])
    null_count = conn["conn"].count_nulls(conn["tblid"])
    res = summary_md(fields, null_count)
    print(res)


def test_vacuum(conn):
    conn["conn"].vacuum(conn["tblid"])
