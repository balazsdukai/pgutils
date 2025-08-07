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
from typing import List, Tuple, Union, Dict, Any
from collections import abc
from keyword import iskeyword
from copy import deepcopy

import psycopg
from psycopg import connect, OperationalError, Error
from psycopg import errors
from psycopg.rows import dict_row, RowMaker
from psycopg.sql import Identifier, Literal, Composable, Composed, SQL

log = logging.getLogger(__name__)


def identifier(relation_name):
    """Property factory for returning a :class:`psycopg.sql.Identifier`."""

    def id_getter(instance):
        return Identifier(instance.__dict__[relation_name])

    def id_setter(instance, value):
        instance.__dict__[relation_name] = value

    return property(id_getter, id_setter)


def literal(relation_name):
    """Property factory for returning a :class:`psycopg.sql.Literal`."""

    def lit_getter(instance):
        return Literal(instance.__dict__[relation_name])

    def lit_setter(instance, value):
        instance.__dict__[relation_name] = value

    return property(lit_getter, lit_setter)


class PostgresIdentifier:
    """Database relation name.

    An escaped SQL identifier of the relation name is accessible through the
    `id` property, which returns a :class:`psycopg.sql.Identifier`.

    Concatenation of identifiers is supported through the `+` operator and results
    in a :class:`psycopg.sql.Identifier`.

    Examples:
        >>> PostgresIdentifier('schema') + PostgresIdentifier('table')
        Identifier('schema', 'table')
        >>> mytable = PostgresIdentifier('table')
        >>> mytable.id
        Identifier('table')
        >>> print(mytable)
        table
        >>> mytable
        table
        >>> PostgresIdentifier(Identifier('schema'))
        schema
    """
    id = identifier('id')

    def __init__(self, relation_name):
        self.id = relation_name
        if isinstance(relation_name, Identifier):
            self.str = relation_name._obj[0]
        else:
            self.str = relation_name

    def __repr__(self):
        return self.str

    def __add__(self, other):
        if isinstance(other, self.__class__):
            return Identifier(self.str, other.str)
        else:
            raise TypeError(f"Unsupported type {other.__class__}")


class PostgresTableIdentifier:
    """A :py:class:`PostgresIdentifier` specifically for tables.

    Args:
        schema (Union[str, :py:class:`psycopg2.sql.Identifier`]): Schema name.
        table (Union[str, :py:class:`psycopg2.sql.Identifier`]): Table name.

    Examples:
        >>> mytable = PostgresTableIdentifier("myschema", "mytable")
        >>> assert isinstance(mytable.schema, PostgresIdentifier)
        >>> assert isinstance(mytable.schema.str, str) and mytable.schema.str == "myschema"
        >>> print(mytable) # The string representaion of 'mytable' is 'schema.name'.
        myschema.mytable
        >>> print(mytable.id) # The '.id' property returns and identfier with 'schema.name'
        Identifier('myschema', 'mytable')
        >>> SQL("select * from {}").format(mytable.id) # Need the '.id' when used in a SQL snippet
        Composed([SQL('select * from '), Identifier('myschema', 'mytable')])
    """
    def __init__(self, schema: Union[str, PostgresIdentifier, Identifier],
                 table: [str, PostgresIdentifier, Identifier]):
        self.schema = schema
        self.table = table

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        if isinstance(value, PostgresIdentifier):
            self._schema = value
        else:
            self._schema = PostgresIdentifier(value)

    @property
    def table(self):
        return self._table

    @table.setter
    def table(self, value):
        if isinstance(value, PostgresIdentifier):
            self._table = value
        else:
            self._table = PostgresIdentifier(value)

    @property
    def id(self):
        return self.schema + self.table

    def __repr__(self):
        return f'{self.schema.str}.{self.table.str}'


def inject_parameters(sql: Union[str, Composable], params: dict = None) -> Composed:
    """Inject parameters into a parameterized SQL snippet.

    If a parameter value is a :py:class:`pgutils.PostgresIdentifier` it is handled as
    a SQL identifier, otherwise the value is handled
    as a :py:class:`psycopg2.sql.Literal`.

    Args:
        sql (Union[str, Composable]): A SQL query.
        params (dict): Query parameters as a dictionary.

    Examples:
        >>> query = SQL('select * from {tbl} where field1 = {val};')
        >>> query_params = {'tbl': PostgresTableIdentifier('myschema', 'mytable'), 'val': 1}
        >>> inject_parameters(sql=query, params=query_params)
        Composed([SQL('select * from '), Identifier('myschema', 'mytable'), SQL(' where field1 = '), Literal(1), SQL(';')])
    """
    _sql = sql if isinstance(sql, SQL) else SQL(sql)
    if params is None:
        return _sql.format()
    else:
        _params = {}
        for k, v in params.items():
            if isinstance(v, PostgresTableIdentifier) or isinstance(v, PostgresIdentifier):
                _params[k] = v.id
            elif isinstance(v, Identifier):
                _params[k] = v
            else:
                _params[k] = Literal(v)
        return _sql.format(**_params)


class PostgresConnection(object):
    """A database connection class.

    Args:
        conn (psycopg.Connection): A psycopg connection.
        dsn (str): PostgreSQL connection string https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING.
            If provided, all the other connection parameters are set to None.

    Raises:
         OperationalError: If cannot establish the connection.
    """

    def __init__(self, conn: psycopg.Connection = None, dsn=None, dbname=None,
                 host=None, port=None,
                 user=None, password=None, **otherparams):
        if conn is None:
            if dsn is not None:
                self._dsn = dsn
                self.dbname = None
                self.host = None
                self.port = None
                self.user = None
                self.password = None
                self.otherparams = None
            else:
                self._dsn = dsn
                self.dbname = dbname
                self.host = host
                self.port = port
                self.user = user
                self.password = password
                self.otherparams = otherparams

            # Test if we can connect
            try:
                if dsn is None:
                    with connect(dbname=self.dbname, host=self.host,
                                 port=self.port, user=self.user,
                                 password=self.password, **self.otherparams) as conn:
                        pass
                else:
                    with connect(conninfo=dsn) as conn:
                        pass
                _creds = deepcopy(self)
                _creds.password = None
                log.debug(f"Connection successful to {_creds.dsn}")
                del _creds
            except OperationalError:
                _creds = deepcopy(self)
                _creds.password = None
                log.exception(f"I'm unable to connect to {_creds.dsn}")
                del _creds
                raise
        else:
            self.conn = conn
            self._dsn = None
            self.dbname = conn.info.dbname
            self.host = conn.info.host
            self.port = conn.info.port
            self.user = conn.info.user
            self.password = conn.info.password
            self.otherparams = {k: v for k, v in conn.info.get_parameters().items() if
                                k not in ["user", "dbname", "host", "port"]}

    @property
    def dsn(self):
        """PostgreSQL's connection Data Source Name (DSN)."""
        if self._dsn is not None:
            return self._dsn
        else:
            _d = []
            if self.dbname:
                _d.append(f"dbname={self.dbname}")
            if self.user:
                _d.append(f"user={self.user}")
            if self.port:
                _d.append(f"port={self.port}")
            if self.host:
                _d.append(f"host={self.host}")
            if self.password:
                _d.append(f"password={self.password}")
            if self.otherparams:
                for key, val in self.otherparams.items():
                    _d.append(f"{key}={val}")
            return " ".join(_d)

    @property
    def dsn_gdal(self):
        """A DSN for using with GDAL."""
        return f"PG:'{self.dsn}'"

    def connect(self) -> psycopg.Connection:
        """Connect to a PostgreSQL database.

        Returns: psycopg.Connection
        """
        return psycopg.connect(self.dsn)

    def close(self):
        """Close the connection."""
        raise NotImplementedError(
            "PostgresConnection methods use psycopg 3 Connection as a context manager."
            "Currently there is no other way to manage transactions."
        )

    def send_query(self, query: Composable, query_params: dict = None):
        """Send a query to the DB when no results need to return (e.g. CREATE).

        Keep in mind that only one cursor at a time can perform operations!
        See the [Cursor class intro](https://www.psycopg.org/psycopg3/docs/api/cursors.html#cursor-classes).

        Args:
            query: A SQL snippet.
            query_params: Optional parameters to pass to the SQL.
                See :py:func:`inject_parameters`.
        """
        with connect(self.dsn) as conn:
            if query_params is not None:
                _q = inject_parameters(query, query_params)
            else:
                _q = query
            return conn.execute(_q)

    def get_query(self, query: Composable, query_params: dict = None) -> List[Tuple]:
        """DB query where the results need to return (e.g. SELECT).

        It returns the complete resultset with ``.fetchall()``.

        Args:
            query: A SQL snippet.
            query_params: Optional parameters to pass to the SQL.
                See :py:func:`inject_parameters`.
        """
        with connect(self.dsn) as conn:
            if query_params is not None:
                _q = inject_parameters(query, query_params)
            else:
                _q = query
            return conn.execute(_q).fetchall()

    def get_dict(self, query: Composable, query_params: dict = None) -> List[RowMaker[Dict[str, Any]]]:
        """DB query where the results need to return as a dictionary."""
        with connect(self.dsn, row_factory=dict_row) as conn:
            if query_params is not None:
                _q = inject_parameters(query, query_params)
            else:
                _q = query
            return conn.execute(_q).fetchall()

    def print_query(self, query: Composable) -> str:
        """Format a SQL query for printing by replacing newlines and tab-spaces."""

        def repl(matchobj):
            if matchobj.group(0) == '    ':
                return ' '
            else:
                return ' '

        with connect(self.dsn) as conn:
            s = query.as_string(conn).strip()
        return re.sub(r'[\n    ]{1,}', repl, s)

    def vacuum(self, table: PostgresTableIdentifier):
        """Vacuum analyze a table."""
        with connect(self.dsn, autocommit=True) as conn:
            query = inject_parameters("VACUUM ANALYZE {table};", {"table": table})
            conn.execute(query)

    def vacuum_full(self):
        """Vacuum analyze the whole database."""
        with connect(self.dsn, autocommit=True) as conn:
            conn.execute("VACUUM ANALYZE;")

    def check_postgis(self):
        """Check if PostGIS is installed."""
        try:
            version = self.get_query("SELECT PostGIS_version();")[0][0]
        except Error:
            version = None
        return version

    def get_fields(self, table: PostgresTableIdentifier) -> List[Tuple]:
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
        query = inject_parameters(_q, {"schema": table.schema.str,
                                       "table": table.table.str})
        log.debug(self.print_query(query))
        return self.get_query(query)

    def get_count(self, table: PostgresTableIdentifier):
        """Get the row count on the table."""
        query = inject_parameters(
            "SELECT count(*) FROM {table}",
            {"table": table}
        )
        return self.get_query(query)[0][0]

    def count_nulls(self, table: PostgresTableIdentifier) -> List:
        """Count the number of missing values in each column in a table."""
        query = inject_parameters(
            "SELECT * FROM pgutils_count_nulls({table})",
            {"table": str(table)}
        )
        try:
            return self.get_dict(query)
        except errors.UndefinedFunction:
            raise errors.UndefinedFunction(
                "Create the pgutils-functions with PostgresFunctions().")

    def get_head(self, table: PostgresTableIdentifier,
                 md=False, shorten=23) -> Union[List, str]:
        """Get the first five rows from a table.

        Args:
            table (PostgresTableIdentifier): Table name.
            md (bool): If true, return the result as a Markdown table.
            shorten (int): Only used when ``md`` is ``True``. Limit the number of
                characters of the field values in the Markdown table, in order to avoid
                super wide tables when a value has many characters/digits.
        """
        query = inject_parameters(
            "SELECT * FROM {table} LIMIT 5",
            {"table": table}
        )
        resultset = self.get_dict(query)

        if md:
            if len(resultset) == 0:
                fields = self.get_fields(table)
                resultset = [dict((col[0], "") for col in fields), ]
            header = " ".join(["|", " | ".join(resultset[0].keys()), "|"])
            header_separator = " ".join(
                ["|", " | ".join("---" for col in resultset[0].keys()), "|"])
            mdtbl = "\n".join([header, header_separator]) + "\n"
            for record in resultset:
                if shorten:
                    _vals = []
                    for val in record.values():
                        try:
                            _vals.append(str(val[:24]))
                        except TypeError:
                            _vals.append(str(val))
                else:
                    _vals = map(str, record.values())
                mdtbl += " ".join(["|" + " | ".join(_vals) + "|"]) + "\n"
            # remove the last \n
            return mdtbl[:-1]
        else:
            return resultset


class PostgresFunctions:
    """A collection of PostgreSQL helper functions.

    The functions are ``CREATE OR REPLACE``-d, when this class is instantiated.
    The list of successfully created functions are in ``PostgresFunctions().created``.
    """

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


class Schema:
    """Database relations.

    The class maps a dictionary to object, where the dict keys are accessible
    as object attributes. Additionally, the values (eg. table name) can be
    retrieved as an escaped SQL identifier through the `sqlid` property.

    Examples:
        >>> relations = {'schema': 'tile_index', 'table': 'bag_index_test', 'fields': {'geometry': 'geom', 'primary_key': 'id', 'unit_name': 'bladnr'}}
        >>> index = Schema(relations)
        >>> index.schema
        tile_index
        >>> index.schema.id
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
            return PostgresIdentifier(arg)

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
