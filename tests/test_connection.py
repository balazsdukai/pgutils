from psycopg2.sql import SQL

from pgutils import PostgresTableIdentifier, inject_parameters


def test_tableref():
    tref = PostgresTableIdentifier("schema", "table")
    sql_format = "select * from {}".format(tref)
    sql_fstring = f"select * from {tref}"
    expected = 'select * from "schema"."table"'
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


def test_inject_parameters_str():
    query = SQL("select * from '{tbl}' where field1 = {val};")
    query_params = {
        'tbl': PostgresTableIdentifier('myschema', 'mytable'),
        'val': 1
    }
    query = inject_parameters(sql=query, params=query_params)
    expect = "Composed([SQL('select * from '), Identifier('myschema', 'mytable'), SQL(' where field1 = '), Literal(1), SQL(';')])"
    assert str(query) == expect
