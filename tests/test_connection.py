from pgutils import PostgresTableIdentifier


def test_tableref():
    tref = PostgresTableIdentifier("schema", "table")
    sql_format = "select * from {}".format(tref)
    sql_fstring = f"select * from {tref}"
    expected = 'select * from "schema"."table"'
    assert sql_format == expected
    assert sql_fstring == expected
