# -*- coding: utf-8 -*-
"""Database connection handling.

Copyright (c) 2022, Balázs Dukai.

The MIT License (MIT)

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import logging
import re
from typing import List, Tuple, Union
from collections import abc
from keyword import iskeyword

from psycopg2 import connect, OperationalError, Error
from psycopg2 import errors
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.sql import Identifier, Literal, Composable, Composed, SQL

log = logging.getLogger(__name__)


class PostgresIdentifier:
    """PosgreSQL identifier that stores the schema with it.

    The attributes are :py:class:`psycopg2.sql.Identifier`.

    :ivar id: Relation indentifier (schema.name).
    :type id: :py:class:`psycopg2.sql.Identifier`
    """

    def __init__(self, schema: Union[str, Identifier], name: [str, Identifier]):
        self.schema = schema
        self.name = name

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        if isinstance(value, Identifier):
            self._schema = value
        else:
            self._schema = Identifier(value)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if isinstance(value, Identifier):
            self._name = value
        else:
            self._name = Identifier(value)

    @property
    def id(self):
        return Identifier(self.schema.string, self.name.string)

    def __repr__(self):
        return f'{self.schema.string}.{self.name.string}'


class PostgresTableIdentifier(PostgresIdentifier):

    def __init__(self, schema: Union[str, Identifier], table: [str, Identifier]):
        super().__init__(schema=schema, name=table)


def inject_parameters(sql: Union[str, Composable], params: dict = None) -> Composed:
    """Inject parameters into a parameterized SQL snippet.

    If a parameter value is a :py:class:`pgutils.PostgresIdentifier` it is handled as
    a SQL identifier, otherwise the value is handled
    as a :py:class:`psycopg2.sql.Literal`.

    Args:
        sql (Union[str, Composable]): A SQL query.
        params (dict): Query parameters as a dictionary.

    For example:

    .. code-block:: python

        query = SQL('select * from {tbl} where field1 = {val};')

        query_params = {
            'tbl': PostgresTableIdentifier('myschema', 'mytable'),
            'val': 1
        }

        inject_parameters(sql=query, params=query_params)
    """
    _sql = sql if isinstance(sql, SQL) else SQL(sql)
    if params is None:
        return _sql.format()
    else:
        _params = {}
        for k, v in params.items():
            if isinstance(v, PostgresIdentifier):
                _params[k] = v.id
            else:
                _params[k] = Literal(v)
        return _sql.format(**_params)


class PostgresConnection(object):
    """A database connection class.

    :raise: :class:`psycopg2.OperationalError`
    """

    def __init__(self, conn=None, dsn=None, dbname=None, hostname=None, port=None,
                 username=None, password=None):
        if conn is None:
            self.dbname = dbname
            self.hostname = hostname
            self.port = port
            self.username = username
            self.password = password
            try:
                if dsn is None:
                    self.conn = connect(
                        dbname=dbname, host=hostname, port=port, user=username,
                        password=password
                    )
                else:
                    self.conn = connect(dsn=dsn)
                log.debug(f"Opened connection to {self.conn.get_dsn_parameters()}")
            except OperationalError:
                log.exception("I'm unable to connect to the database")
                raise
        else:
            self.conn = conn

    def close(self):
        """Close connection."""
        self.conn.close()
        log.debug("Closed database successfully")

    def send_query(self, query: Composable):
        """Send a query to the DB when no results need to return (e.g. CREATE)."""
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(query)

    def get_query(self, query: Composable) -> List[Tuple]:
        """DB query where the results need to return (e.g. SELECT)."""
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchall()

    def get_dict(self, query: Composable) -> dict:
        """DB query where the results need to return as a dictionary."""
        with self.conn:
            with self.conn.cursor(
                    cursor_factory=RealDictCursor) as cur:
                cur.execute(query)
                return cur.fetchall()

    def print_query(self, query: Composable) -> str:
        """Format a SQL query for printing by replacing newlines and tab-spaces."""

        def repl(matchobj):
            if matchobj.group(0) == '    ':
                return ' '
            else:
                return ' '

        s = query.as_string(self.conn).strip()
        return re.sub(r'[\n    ]{1,}', repl, s)

    def vacuum(self, schema: str, table: str):
        """Vacuum analyze a table."""
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        schema = Identifier(schema)
        table = Identifier(table)
        query = SQL("""
        VACUUM ANALYZE {schema}.{table};
        """).format(schema=schema, table=table)
        self.send_query(query)

    def vacuum_full(self):
        """Vacuum analyze the whole database."""
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        query = SQL("VACUUM ANALYZE;")
        self.send_query(query)

    def check_postgis(self):
        """Check if PostGIS is installed."""
        try:
            version = self.get_query("SELECT PostGIS_version();")[0][0]
        except Error:
            version = None
        return version

    def get_fields(self, table: PostgresTableIdentifier):
        """List the fields and data types in a table.

        From: https://stackoverflow.com/a/58319308
        """
        _q = """
        SELECT
            pg_attribute.attname AS column_name,
            pg_catalog.format_type(pg_attribute.atttypid, pg_attribute.atttypmod) AS data_type
        FROM
            pg_catalog.pg_attribute
        INNER JOIN
            pg_catalog.pg_class ON pg_class.oid = pg_attribute.attrelid
        INNER JOIN
            pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
        WHERE
            pg_attribute.attnum > 0
            AND NOT pg_attribute.attisdropped
            AND pg_namespace.nspname = {schema}
            AND pg_class.relname = {table}
        ORDER BY
            attnum ASC;
        """
        query = inject_parameters(_q, {"schema": table.schema.string,
                                       "table": table.name.string})
        log.debug(self.print_query(query))
        return self.get_query(query)

    def count_nulls(self, table: PostgresTableIdentifier):
        query = inject_parameters(
            "SELECT * FROM pgutils_count_nulls({table})",
            {"table": str(table)}
        )
        try:
            return self.get_query(query)
        except errors.UndefinedFunction:
            raise errors.UndefinedFunction(
                "Create the pgutils-functions with PostgresFunctions().")


class PostgresFunctions:
    """A collection of PostgreSQL functions."""

    def __init__(self, conn: PostgresConnection):
        self.created = []
        self.__count_nulls(conn)

    def __count_nulls(self, conn):
        """Count the number of missing values in each column in a table.

        SELECT * FROM pgutils_count_nulls('schema.table');

        From https://dba.stackexchange.com/a/285850
        """
        fname = "pgutils_count_nulls(_tbl regclass)"
        try:
            func = """
            CREATE OR REPLACE FUNCTION pgutils_count_nulls(_tbl regclass)
              RETURNS TABLE (column_name text, missing_values bigint)
              LANGUAGE plpgsql STABLE PARALLEL SAFE AS
            $func$
            BEGIN
               RETURN QUERY EXECUTE (
               SELECT format(
               $$
               SELECT x.*
               FROM  (SELECT count(*) AS ct, %s FROM %s) t
               CROSS  JOIN LATERAL (VALUES %s) x(col, nulls)
               ORDER  BY nulls DESC, col DESC
               $$, string_agg(format('count(%1$I) AS %1$I', attname), ', ')
                 , $1
                 , string_agg(format('(%1$L, ct - %1$I)', attname), ', ')
                  )
               FROM   pg_catalog.pg_attribute
               WHERE  attrelid = $1
               AND    attnum > 0
               AND    NOT attisdropped
               -- more filters?
               );
            END
            $func$;
            """
            conn.send_query(func)
            log.info(f"Created function: {fname}")
            self.created.append(fname)
        except Exception as e:
            log.info(f"Failed to create function: {fname}\n{e}")


def identifier(relation_name):
    """Property factory for returning a :class:`psycopg2.sql.Identifier`."""

    def id_getter(instance):
        return Identifier(instance.__dict__[relation_name])

    def id_setter(instance, value):
        instance.__dict__[relation_name] = value

    return property(id_getter, id_setter)


def literal(relation_name):
    """Property factory for returning a :class:`psycopg2.sql.Literal`."""

    def lit_getter(instance):
        return Literal(instance.__dict__[relation_name])

    def lit_setter(instance, value):
        instance.__dict__[relation_name] = value

    return property(lit_getter, lit_setter)


class DatabaseRelation:
    """Database relation name.

    An escaped SQL identifier of the relation name is accessible through the
    `sqlid` property, which returns a :class:`psycopg2.sql.Identifier`.

    Concatenation of identifiers is supported through the `+` operator.

    >>> DatabaseRelation('schema') + DatabaseRelation('table')
    """
    sqlid = identifier('sqlid')

    def __init__(self, relation_name):
        self.sqlid = relation_name
        self.string = relation_name

    def __repr__(self):
        return self.string

    def __add__(self, other):
        if isinstance(other, self.__class__):
            return Identifier(self.string, other.string)
        else:
            raise TypeError(f"Unsupported type {other.__class__}")


class Schema:
    """Database relations.

    The class maps a dictionary to object, where the dict keys are accessible
    as object attributes. Additionally, the values (eg. table name) can be
    retrieved as an escaped SQL identifier through the `sqlid` property.

    >>> relations = {
        'schema': 'tile_index',
        'table': 'bag_index_test',
            'fields': {
            'geometry': 'geom',
            'primary_key': 'id',
            'unit_name': 'bladnr'}
        }
    >>> index = Schema(relations)
    >>> index.schema
    'tile_index'
    >>> index.schema.identifier
    Identifier('tile_index')
    >>> index.schema + index.table
    Identifier('tile_index', 'bag_index_test')
    """

    # TODO: skip Lists
    def __new__(cls, arg):
        if isinstance(arg, abc.Mapping):
            return super().__new__(cls)
        elif isinstance(arg, abc.MutableSequence):
            return [cls(item) for item in arg]
        else:
            return DatabaseRelation(arg)

    def __init__(self, mapping):
        self.__data = {}
        for key, value in mapping.items():
            if iskeyword(key):
                key += '_'
            self.__data[key] = value

    def __getattr__(self, name):
        if hasattr(self.__data, name):
            return getattr(self.__data, name)
        else:
            return Schema(self.__data[name])

