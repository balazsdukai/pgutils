from connection import TableRef


def test_tableref():
    tref = TableRef("schema", "table")
    sql_format = "select * from {}".format(tref)
    sql_fstring = f"select * from {tref}"
    expected = 'select * from "schema"."table"'
    assert sql_format == expected
    assert sql_fstring == expected
