from __future__ import annotations

import logging
import os
import re
from contextlib import nullcontext
from datetime import datetime, time, tzinfo
from io import IOBase, StringIO, TextIOWrapper
from pathlib import Path
from typing import Any, Generic, Sequence, TypeVar
from urllib.parse import ParseResult, quote, urlparse

from .. import (Literal, OutTable, build_url, hide_url_password, is_aware, make_aware,
                out_table, skip_utf8_bom)

try:
    from tabulate import tabulate
except ImportError:
    tabulate = None

try:
    from django.http import Http404 as NotFoundError

except ImportError:
    class NotFoundError(Exception):
        pass

class SeveralFoundError(Exception):
    pass


logger = logging.getLogger(__name__)

T_Connection = TypeVar('T_Connection')
T_Cursor = TypeVar('T_Cursor')
T_Composable = TypeVar('T_Composable')
T_Composed = TypeVar('T_Composed')


class DbAdapter(Generic[T_Connection, T_Cursor, T_Composable, T_Composed]):
    """
    Base class for database adapters.
    """
    URL_SCHEME: str
    _ALT_SCHEMES: set[str] = set()
    DEFAULT_SCHEMA = None
    ONLY_POSITIONAL_PARAMS = False
    EXPECTED_CONNECTION_TYPES: list[str]

    @classmethod
    def is_available(cls):
        raise NotImplementedError()
    

    def __init__(self, origin: T_Connection|str|dict|ParseResult, password_required: bool = False, autocommit: bool = True, tz: tzinfo = None):
        """
        Create a new adapter.
        - `origin`: an existing connection object, or the URL or django alias (e.g. 'default') for the new connection to create by the adapter.
        - `autocommit`: whether or not to auto-commit transactions (applies only for connections created by the adapter)
        """
        if not self.is_available():
            raise ValueError(f"Cannot use {type(self).__name__} (not available)")
        
        self.schema: str = None
        """ A specific schema associated to this adapter. Used for example as input of `OutTable` (when parsed from an URL). """

        self.table: str = None
        """ A specific table associated to this adapter. Used for example as input of `OutTable` (when parsed from an URL). """
        
        if isinstance(origin, (str,ParseResult)):
            self._must_close_connection = True
            self._connection: T_Connection = None
            if isinstance(origin, ParseResult) or ':' in origin or '/' in origin or ';' in origin or ' ' in origin:
                r = origin if isinstance(origin, ParseResult) else urlparse(origin)

                if r.fragment:
                    raise ValueError(f"Invalid {self.__class__.__name__}: unexpected fragment: {r.fragment}")
                if r.query:
                    raise ValueError(f"Invalid {self.__class__.__name__}: unexpected query: {r.query}")
                if r.params:
                    raise ValueError(f"Invalid {self.__class__.__name__}: unexpected params: {r.params}")
                
                if r.scheme != self.URL_SCHEME:
                    if r.scheme in self._ALT_SCHEMES:
                        r = r._replace(scheme=self.URL_SCHEME)
                    else:
                        raise ValueError(f"Invalid {self.__class__.__name__}: invalid scheme: {r.scheme}")

                m = re.match(r'^/?(?P<name>[^/@\:]+)/((?P<schema>[^/@\:\.]+)\.)?(?P<table>[^/@\:\.]+)$', r.path)
                if m:                
                    self.table = m['table']
                    self.schema = m['schema'] if m['schema'] else self.DEFAULT_SCHEMA
                
                    r = r._replace(path=m['name'])
                    self._connection_url = r.geturl()
                
                else:
                    self._connection_url = r.geturl()
            
            else:
                from django.conf import settings
                if not origin in settings.DATABASES:
                    raise ValueError(f"key \"{origin}\" not found in django DATABASES settings")
                config: dict[str,Any] = settings.DATABASES[origin]
                
                self._connection_url = build_url(
                    scheme = self.URL_SCHEME,
                    hostname = config.get('HOST', None),
                    port = config.get('PORT', None),
                    username = config.get('USER', None),
                    password = config.get('PASSWORD', None),
                    path = config.get('NAME', None),
                )
                self.table = config.get('TABLE', None)
                self.schema = config.get('SCHEMA', self.DEFAULT_SCHEMA if self.table else None)

        elif isinstance(origin, dict):
            self._must_close_connection = True
            self._connection: T_Connection = None

            if 'NAME' in origin:
                # uppercase (as used by django)
                self._connection_url = build_url(
                    scheme = self.URL_SCHEME,
                    hostname = origin.get('HOST', None),
                    port = origin.get('PORT', None),
                    username = origin.get('USER', None),
                    password = origin.get('PASSWORD', None),
                    path = origin.get('NAME', None),
                )
                self.table = origin.get('TABLE', None)
                self.schema = origin.get('SCHEMA', self.DEFAULT_SCHEMA if self.table else None)

            else:
                # lowercase (as used by some drivers' connection kwargs)
                self._connection_url = build_url(
                    scheme = self.URL_SCHEME,
                    hostname = origin.get('host', None),
                    port = origin.get('port', None),
                    username = origin.get('user', None),
                    password = origin.get('password', None),
                    path = origin.get('name', origin.get('dbname', None)),
                )
                self.table = origin.get('table', None)
                self.schema = origin.get('schema', self.DEFAULT_SCHEMA if self.table else None)

        else:
            origin = _get_connection_from_wrapper(origin)

            fulltype = type(origin).__module__ + '.' + type(origin).__qualname__
            if fulltype not in self.EXPECTED_CONNECTION_TYPES:
                raise ValueError(f"invalid connection type for {type(self).__name__}: {fulltype}")
            self._connection = origin
            self._connection_url: str = None
            self._must_close_connection = False
        
        self.password_required = password_required
        self.autocommit = autocommit
        self.tz = tz
    

    def get_url(self, *, hide_password = False):
        if self._connection_url:
            url = self._connection_url
        else:
            url = self._get_url_from_connection()

        if hide_password:
            url = hide_url_password(url)

        if self.table:
            url += f"/"
            if self.schema:
                url += quote(self.schema)
                url += '.'
            url += quote(self.table)

        return url


    def _get_url_from_connection(self):
        raise NotImplementedError()


    #region Connection

    def __enter__(self):
        return self


    def __exit__(self, exc_type = None, exc_val = None, exc_tb = None):
        self.close()


    def close(self):
        if self._connection and self._must_close_connection:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Close %s (%s) connection to %s", type(self).__name__, type(self._connection).__module__ + '.' + type(self._connection).__qualname__, hide_url_password(self._connection_url))
            self._connection.close()


    @property
    def connection(self):
        if not self._connection:                
            if self.password_required:
                password = urlparse(self._connection_url).password
                if not password:
                    raise ValueError("Cannot create %s connection to %s: password not provided" % (type(self).__name__, hide_url_password(self._connection_url)))
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Create %s connection to %s", type(self).__name__, hide_url_password(self._connection_url))
            self._connection = self._create_connection()
        return self._connection


    def _create_connection(self) -> T_Connection:
        raise NotImplementedError()


    def cursor(self) -> T_Cursor:
        return self.connection.cursor()

    #endregion
    

    #region Queries

    def to_positional_params(self, query: str, params: dict) -> tuple[str, Sequence[Any]]:
        from sqlparams import \
            SQLParams  # not at the top because the enduser might not need this feature

        if not hasattr(self.__class__, '_params_formatter'):
            self.__class__._params_formatter = SQLParams('named', 'qmark')
        query, params = self.__class__._params_formatter.format(query, params)

        return query, params
    
    
    def get_paginated_query(self, query: str, *, limit: int|None, offset: int|None) -> str:
        paginated_query, _ = self.get_paginated_and_total_query(query, limit=limit, offset=offset)
        return paginated_query
    

    def get_paginated_and_total_query(self, query: str, *, limit: int|None, offset: int|None) -> tuple[str,str]:        
        if limit is not None:
            if isinstance(limit, str) and re.match(r"^[0-9]+$", limit):
                limit = int(limit)
            elif not isinstance(limit, int):
                raise TypeError(f"Invalid type for limit: {type(limit).__name__} (expected int)")
            
        if offset is not None:
            if isinstance(offset, str) and re.match(r"^[0-9]+$", offset):
                offset = int(offset)
            elif not isinstance(offset, int):
                raise TypeError(f"Invalid type for offset: {type(limit).__name__} (expected int)")
        
        beforepart, selectpart, orderpart = self._parse_select_query(query)

        paginated_query = beforepart
        total_query = beforepart
        
        paginated_query += self._paginate_parsed_query(selectpart, orderpart, limit=limit, offset=offset)
        total_query += f"SELECT COUNT(*) FROM ({selectpart}) s"

        return paginated_query, total_query
    

    def _parse_select_query(self, query: str):
        import sqlparse  # not at the top because the enduser might not need this feature

        # Parse SQL to remove token before the SELECT keyword
        # example: WITH (CTE) tokens
        statements = sqlparse.parse(query)
        if len(statements) != 1:
            raise ValueError(f"query contains {len(statements)} statements")

        # Get first DML keyword
        dml_keyword = None
        dml_keyword_index = None
        order_by_index = None
        for i, token in enumerate(statements[0].tokens):
            if token.ttype == sqlparse.tokens.DML:
                if dml_keyword is None:
                    dml_keyword = str(token).upper()
                    dml_keyword_index = i
            elif token.ttype == sqlparse.tokens.Keyword:
                if order_by_index is None:
                    keyword = str(token).upper()
                    if keyword == "ORDER BY":
                        order_by_index = i

        # Check if the DML keyword is SELECT
        if not dml_keyword:
            raise ValueError(f"no SELECT found (query does not contain DML keyword)")
        if dml_keyword != 'SELECT':
            raise ValueError(f"first DML keyword is {dml_keyword}, expected SELECT")

        # Get part before SELECT (example: WITH)
        if dml_keyword_index > 0:
            tokens = statements[0].tokens[:dml_keyword_index]
            beforepart = ''.join(str(token) for token in tokens)
        else:
            beforepart = ''
    
        # Determine actual SELECT query
        if order_by_index is not None:
            tokens = statements[0].tokens[dml_keyword_index:order_by_index]
            selectpart = ''.join(str(token) for token in tokens)
            tokens = statements[0].tokens[order_by_index:]
            orderpart = ''.join(str(token) for token in tokens)
        else:
            tokens = statements[0].tokens[dml_keyword_index:]
            selectpart = ''.join(str(token) for token in tokens)
            orderpart = ''

        return beforepart, selectpart, orderpart
    

    def _paginate_parsed_query(self, selectpart: str, orderpart: str, *, limit: int|None, offset: int|None) -> str:
        result = f"{selectpart} {orderpart}"
        if limit is not None:
            result += f" LIMIT {limit}"
        if offset is not None:
            result += f" OFFSET {offset}"
        return result
    

    def get_select_table_query(self, table: str|tuple = None, *, schema_only = False) -> T_Composed:
        """
        Build a query on the given table.

        If `schema_only` is given, no row will be returned (this is used to get information on the table).
        Otherwise, all rows will be returned.

        The return type of this function depends on the database engine.
        It is passed directly to the cursor's execute function for this engine.
        """
        raise NotImplementedError()


    def _get_composable_param(self, value) -> T_Composable:
        if value is None:
            return "null"
        elif value == '__now__':
            return "CURRENT_DATETIME()"
        else:
            return self.escape_literal(value)
                

    def escape_identifier(self, value) -> T_Composable:
        raise NotImplementedError()
                

    def escape_literal(self, value) -> T_Composable:
        raise NotImplementedError()
    
    #endregion
    

    #region Execution

    def execute_query(self, query: str, params: list|tuple|dict = None, *, cursor: T_Cursor = None, results: bool|Literal['warning']|TextIOWrapper|OutTable|str|Path = False, tz: tzinfo = None, limit: int = None, offset: int = None, query_id: str = None):
        """
        - `results`:
            - If True, return results as a dict list.
            - If False, ignore results.
            - If `warning`, produce a warning log if there is results.
            - If a stream or a str/path, write results as CSV to the given stream and return tuple (columns, row_count)
        - `tz`: naive datetimes in results are made aware in the given timezone.
        """
        if limit is not None or offset is not None:
            query = self.get_paginated_query(query, limit=limit, offset=offset)
                
        # Example of positional param: cursor.execute("INSERT INTO foo VALUES (%s)", ["bar"])
        # Example of named param: cursor.execute("INSERT INTO foo VALUES (%(foo)s)", {"foo": "bar"})
        if params is None:
            params = []
        elif isinstance(params, dict) and self.ONLY_POSITIONAL_PARAMS:
            query, params = self.to_positional_params(query, params)

        with nullcontext(cursor) if cursor else self.cursor() as _cursor:            
            # Execute query
            _cursor.execute(query, params)
            self._log_execute_messages(_cursor)

            # Log number of affected rows for non-select queries
            if not _cursor.description and _cursor.rowcount >= 0:
                logger.debug(f"Affected rows: {_cursor.rowcount:,}")

            def format_row(row):
                if tz or self.tz:
                    for i, value in enumerate(row):
                        if isinstance(value, (datetime,time)):
                            if not is_aware(value):
                                row[i] = make_aware(value, tz or self.tz)
                return row
            
            # Handle results
            if results == 'warning':
                if _cursor.description:
                    rows = [row for row in _cursor]                    
                    row_count = len(rows)
                    if row_count > 0:
                        columns = self.get_cursor_column_names(_cursor)

                        if tabulate:
                            text_rows = tabulate(rows[0:10], headers=columns)
                        else:
                            fp = StringIO()
                            with out_table(fp, headers=columns) as o:
                                for row in rows[0:10]:
                                    o.append(row)
                            text_rows = fp.getvalue()

                        if row_count > 10:
                            text_rows += "\n…"
                        logger.warning("query%s returned %d row%s:\n%s", f" {query_id}" if query_id else "", row_count, "s" if row_count > 1 else "", text_rows)
                return None
            
            elif isinstance(results, (OutTable,IOBase,str,Path)):
                # Write results as CSV to the given stream
                columns = self.get_cursor_column_names(_cursor)

                if isinstance(results, OutTable):
                    o = results
                    o.headers = columns
                else:
                    o = out_table(results, headers=columns, title=False, tablefmt='csv')
                
                with o:
                    for row in _cursor:
                        o.append(format_row(row))
                    
                    o.file.seek(0)
                    return columns, o.row_count

            elif results:
                # Return results as a dict list
                columns = self.get_cursor_column_names(_cursor)
                return [{columns[i]: value for i, value in enumerate(format_row(row))} for row in _cursor]
            
            else:
                return None


    def _log_execute_messages(self, cursor: T_Cursor):
        """
        Log messages produced during execution of a query, if this cannot be done through a connection handler.
        """
        pass


    def execute_file(self, path: str|Path, params: list|tuple|dict = None, *, cursor: T_Cursor = None, results: bool|TextIOWrapper|str|Path = False, tz: tzinfo = None, limit: int = None, offset: int = None, encoding: str = 'utf-8') -> None:
        with open(path, 'r', encoding=encoding) as fp:
            skip_utf8_bom(fp)
            query = fp.read()
            
        self.execute_query(query, params, cursor=cursor, results=results, tz=tz, limit=limit, offset=offset)
    

    def execute_procedure(self, name: str|tuple, *args) -> T_Cursor:
        return NotImplementedError()
    
    #endregion


    #region Results

    def get_scalar(self, query: str, params: list|tuple|dict = None, *, limit: int = None, offset: int = None):
        with self.cursor() as cursor:
            self.execute_query(query, params, cursor=cursor, limit=limit, offset=offset)

            iterator = iter(cursor)
            try:
                row = next(iterator)
            except StopIteration:
                raise NotFoundError()

            try:
                next(iterator)
                raise SeveralFoundError()
            except StopIteration:
                pass

            return row[0]
    

    def get_scalars(self, query: str, params: list|tuple|dict = None, *, limit: int = None, offset: int = None):
        results = []

        with self.cursor() as cursor:
            self.execute_query(query, params, cursor=cursor, limit=limit, offset=offset)
            for row in cursor:
                results.append(row[0])

        return results


    def get_dict(self, query: str, params: list|tuple|dict = None, *, limit: int = None, offset: int = None):
        with self.cursor() as cursor:
            self.execute_query(query, params, limit=limit, offset=offset, cursor=cursor)

            iterator = iter(cursor)
            try:
                row = next(iterator)
            except StopIteration:
                raise NotFoundError()

            try:
                next(iterator)
                raise SeveralFoundError()
            except StopIteration:
                pass

            columns = self.get_cursor_column_names(cursor)
            return {columns[i]: value for i, value in enumerate(row)}
    

    def get_result(self, query: str, params: list|tuple|dict = None, *, limit: int = None, offset: int = None):
        cursor = self.cursor()
        self.execute_query(query, params, limit=limit, offset=offset, cursor=cursor)
        return CursorResult(self, cursor)
    

    def iter_dicts(self, query: str, params: list|tuple|dict = None, *, limit: int = None, offset: int = None):        
        with self.cursor() as cursor:
            self.execute_query(query, params, limit=limit, offset=offset, cursor=cursor)
            columns = self.get_cursor_column_names(cursor)
            for row in cursor:
                yield {columns[i]: value for i, value in enumerate(row)}
                
    
    def get_dicts(self, query: str, params: list|tuple|dict = None, *, limit: int = None, offset: int = None):        
        return [row for row in self.iter_dicts(query, params, limit=limit, offset=offset)]
    

    def get_dicts_and_total(self, query: str, params: list|dict = None, *, limit: int, offset: int):
        paginated_query, total_query = self.get_paginated_and_total_query(query, limit=limit, offset=offset)

        rows = self.get_dicts(paginated_query, params)
        total = self.get_scalar(total_query, params)
        return {"rows": rows, "total": total}

    #endregion


    #region Tables and columns

    def split_name(self, name: str|tuple = None) -> tuple[str,str]:
        if name is None:
            if not self.table:
                raise ValueError("No table given")
            return self.schema, self.table

        if isinstance(name, tuple):
            return name
        
        try:
            pos = name.index('.')
            schema = name[0:pos]
            name = name[pos+1:]
        except ValueError:
            schema = self.DEFAULT_SCHEMA
            name = name

        return (schema, name)
    

    def schema_exists(self, schema: str) -> bool:
        raise NotImplementedError()
    

    def create_schema(self, schema: str):
        raise NotImplementedError()
    

    def drop_schema(self, schema: str, cascade: bool = False):
        raise NotImplementedError()
    

    def table_exists(self, table: str|tuple = None) -> bool:
        raise NotImplementedError()


    def get_cursor_column_names(self, cursor: T_Cursor) -> list[str]:
        if not cursor.description:
            raise ValueError("No cursor description available")
        return [info[0] for info in cursor.description]


    def get_cursor_columns(self, cursor: T_Cursor) -> list[ColumnInfo]:
        if not cursor.description:
            raise ValueError("No cursor description available")
        return [ColumnInfo(self, cursor, index) for index in range(len(cursor.description))]
        

    def get_table_column_names(self, table: str|tuple = None) -> list[str]:
        query = self.get_select_table_query(table, schema_only=True)
        with self.cursor() as cursor:
            self.execute_query(query, cursor=cursor)
            return self.get_cursor_column_names(cursor)
        

    def get_table_columns(self, table: str|tuple = None) -> list[ColumnInfo]:
        query = self.get_select_table_query(table, schema_only=True)
        with self.cursor() as cursor:
            self.execute_query(query, cursor=cursor)
            return self.get_cursor_columns(cursor)
        

    def _update_column_info(self, info: ColumnInfo, cursor: T_Cursor, index: int):
        info.name = cursor.description[index][0]
    

    def drop_table(self, table: str|tuple = None):
        schema, table = self.split_name(table)
        
        query = "DROP TABLE "
            
        if schema:    
            query += f"{self.escape_identifier(schema)}."
        query += f"{self.escape_identifier(table)}"

        self.execute_query(query)


    def truncate_table(self, table: str|tuple = None, *, cascade: bool = False):
        schema, table = self.split_name(table)
        
        query = "TRUNCATE TABLE "
            
        if schema:    
            query += f"{self.escape_identifier(schema)}."
        query += f"{self.escape_identifier(table)}"

        if cascade:
            query += " CASCADE"

        self.execute_query(query)


    def load_from_csv(self, file: os.PathLike|IOBase, table: str|tuple = None, *, columns: list[str] = None, encoding: str = 'utf-8', merge: Literal['truncate', 'truncate-cascade', 'upsert'] = None, noheaders: bool = False, csv_delimiter: str = None, csv_quotechar: str = None, csv_nullval: str = None) -> int:
        raise NotImplementedError()
   

    # endregion


def _get_connection_from_wrapper(origin):    
    if type(origin).__module__.startswith(('django.db.backends.', 'django.utils.connection')):
        return origin.connection
    elif type(origin).__module__.startswith(('psycopg_pool.pool',)):
        return origin.connection()
    elif type(origin).__module__.startswith(('psycopg2.pool',)):
        return origin.getconn()
    else:
        return origin


class ColumnInfo:
    def __init__(self, db: DbAdapter, cursor: T_Cursor, index: int):
        self.name = None
        self.python_type: type = None
        self.sql_type: str = None
        self.sql_typecode: int = None
        self.nullable: bool = None
        db._update_column_info(self, cursor, index)

    def __str__(self):
        return self.name
    
    def __repr__(self):
        return self.name


class CursorResult(Generic[T_Cursor]):
    def __init__(self, db: DbAdapter, cursor: T_Cursor):
        self.cursor = cursor
        self.columns = db.get_cursor_column_names(cursor)
        self._column_indexes: dict[str, int] = None

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type = None, exc_val = None, exc_tb = None):
        self.close()

    def close(self):
        self.cursor.close()

    def __iter__(self):
        return self

    def __next__(self):
        values = next(self.cursor)
        return CursorRow(self, values)
    

class CursorRow(Generic[T_Cursor]):
    def __init__(self, result: CursorResult[T_Cursor], values: tuple):
        self.result = result
        self.values = values
        

    def __len__(self):
        return len(self.values)


    def __getitem__(self, key: int|str):
        if not isinstance(key, int):
            if self.result._column_indexes is None:
                self.result._column_indexes = {name: i for i, name in enumerate(self.result.columns)}
            key = self.result._column_indexes[key]
            
        return self.values[key]
    

    def as_dict(self):
        return {column_name: self[i] for i, column_name in enumerate(self.result.columns)}
