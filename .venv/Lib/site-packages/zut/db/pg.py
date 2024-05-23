from __future__ import annotations

import logging
import os
import re
from contextlib import nullcontext
from datetime import date, datetime, time
from decimal import Decimal
from io import IOBase
from secrets import token_hex
from typing import Any
from uuid import UUID

from .. import Literal, build_url, skip_utf8_bom, _get_csv_params
from .base import ColumnInfo, DbAdapter, T_Connection, T_Cursor, T_Composable, T_Composed

logger = logging.getLogger(__name__)


def pg_notice_handler(diag: Diagnostic, logger: logging.Logger = None):
    """
    Handler required by psycopg 3 `connection.add_notice_handler()`.
    """
    # determine level
    level = pg_get_logging_level(diag.severity_nonlocalized)
    
    # determine logger
    if logger:
        logger = logger
        message = diag.message_primary
    else:
        # parse context
        m = re.match(r"^fonction [^\s]+ (\w+)", diag.context or '')
        if m:
            logger = logging.getLogger(f"pg:{m[1]}")
            message = diag.message_primary
        else:
            logger = logging.getLogger("pg")
            message = f"{diag.context or ''}{diag.message_primary}"

    # write log
    logger.log(level, message)


def pg_get_logging_level(severity_nonlocalized: str):
    if severity_nonlocalized.startswith('DEBUG'): # not sent to client (by default)
        return logging.DEBUG
    elif severity_nonlocalized == 'LOG': # not sent to client (by default), written on server log (LOG > ERROR for log_min_messages)
        return logging.DEBUG
    elif severity_nonlocalized == 'NOTICE': # sent to client (by default) [=client_min_messages]
        return logging.DEBUG
    elif severity_nonlocalized == 'INFO': # always sent to client
        return logging.INFO
    elif severity_nonlocalized == 'WARNING': # sent to client (by default) [=log_min_messages]
        return logging.WARNING
    elif severity_nonlocalized in ['ERROR', 'FATAL']: # sent to client
        return logging.ERROR
    elif severity_nonlocalized in 'PANIC': # sent to client
        return logging.CRITICAL
    else:
        return logging.ERROR


OID_CATALOG = {
    16: ('bool', bool),
    17: ('bytea', bytes),
    18: ('char', str),
    19: ('name', str),
    20: ('int8', int),
    21: ('int2', int),
    23: ('int4', int),
    25: ('text', str),
    26: ('oid', int),
    114: ('json', None),
    650: ('cidr', None),
    700: ('float4', float),
    701: ('float8', float),
    869: ('inet', None),
    1042: ('bpchar', str),
    1043: ('varchar', str),
    1082: ('date', date),
    1083: ('time', time),
    1114: ('timestamp', datetime),
    1184: ('timestamptz', datetime),
    1186: ('interval', None),
    1266: ('timetz', time),
    1700: ('numeric', Decimal),
    2249: ('record', None),
    2950: ('uuid', UUID),
    3802: ('jsonb', None),
    3904: ('int4range', None),
    3906: ('numrange', None),
    3908: ('tsrange', None),
    3910: ('tstzrange', None),
    3912: ('daterange', None),
    3926: ('int8range', None),
    4451: ('int4multirange', None),
    4532: ('nummultirange', None),
    4533: ('tsmultirange', None),
    4534: ('tstzmultirange', None),
    4535: ('datemultirange', None),
    4536: ('int8multirange', None),
}


class BasePgAdapter(DbAdapter[T_Connection, T_Cursor, T_Composable, T_Composed]):
    """
    Base class for PostgreSql database adapters (:class:`PgAdapter` using `psycopg` (v3) driver or :class:`Pg2Adapter` using `psycopg2` driver).
    """
    URL_SCHEME = 'postgresql' # See: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
    _ALT_SCHEMES = {'pg', 'postgres'}
    DEFAULT_SCHEMA = 'public'    
    EXPECTED_CONNECTION_TYPES = ['psycopg.Connection']
    _sql: Any


    def _create_connection(self):
        return connect(self._connection_url, autocommit=self.autocommit)
    
    
    def _get_url_from_connection(self):
        with self.cursor() as cursor:
            cursor.execute("SELECT session_user, inet_server_addr(), inet_server_port(), current_database()")
            user, host, port, dbname = next(iter(cursor))
        return build_url(scheme=self.URL_SCHEME, username=user, hostname=host, port=port, path='/'+dbname)


    #region Execute utils
    
    def execute_procedure(self, name: str|tuple, *args):
        schema, name = self.split_name(name)
        
        query = "CALL "
        params = []
            
        if schema:    
            query +="{}."
            params += [self.escape_identifier(schema)]

        query += "{}"
        params += [self.escape_identifier(name)]

        query += "(" + ", ".join(['{}'] * len(args)) + ")"
        params += [self._get_composable_param(arg) for arg in args]

        with self.cursor() as cursor:
            with self.register_notice_handler(if_exists=None, logprefix=f"pg:{schema + '.' if schema and schema != self.DEFAULT_SCHEMA else ''}{name}"):
                cursor.execute(self._sql.SQL(query).format(*params))
                return cursor
            

    def register_notice_handler(self, if_exists = '__raise__', logprefix = 'pg'):
        raise NotImplementedError()
    
    #endregion


    #region Queries
    
    def get_select_table_query(self, table: str|tuple = None, *, schema_only = False):
        schema, table = self.split_name(table)

        query = "SELECT * FROM "
        params = []
            
        if schema:    
            query +="{}."
            params += [self.escape_identifier(schema)]

        query += "{}"
        params += [self.escape_identifier(table)]
        
        if schema_only:
            query += ' WHERE false'

        return self._sql.SQL(query).format(*params)


    def _get_composable_param(self, value):
        if value is None:
            return self._sql.SQL("null")
        elif value == '__now__':
            return self._sql.SQL("NOW()")
        elif isinstance(value, self._sql.Composable):
            return value
        else:
            return self.escape_literal(value)
        

    def escape_identifier(self, value):
        return self._sql.Identifier(value)
    

    def escape_literal(self, value):
        return self._sql.Literal(value)
    
    #endregion
    

    #region Schemas, tables and columns  

    def schema_exists(self, schema: str) -> bool:
        query = "SELECT EXISTS (SELECT FROM pg_namespace WHERE nspname = %s)"
        params = [schema]

        return self.get_scalar(query, params)
    

    def create_schema(self, schema: str):
        query = "CREATE SCHEMA {}"
        params = [self._sql.Identifier(schema)]

        return self.execute_query(self._sql.SQL(query).format(*params))
    

    def drop_schema(self, schema: str, cascade: bool = False):
        query = "DROP SCHEMA {}"
        params = [self._sql.Identifier(schema)]

        if cascade:
            query += " CASCADE"

        return self.execute_query(self._sql.SQL(query).format(*params))
    

    def table_exists(self, table: str|tuple = None) -> bool:
        schema, table = self.split_name(table)

        query = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = %s AND tablename = %s)"
        params = [schema, table]

        return self.get_scalar(query, params)
    

    def drop_table(self, table: str|tuple = None):
        schema, table = self.split_name(table)
        
        query = "DROP TABLE "
        params = []
            
        if schema:    
            query +="{}."
            params += [self.escape_identifier(schema)]

        query += "{}"
        params += [self.escape_identifier(table)]

        self.execute_query(self._sql.SQL(query).format(*params))
        

    def truncate_table(self, table: str|tuple = None, *, cascade: bool = False):
        schema, table = self.split_name(table)
        
        query = "TRUNCATE TABLE "
        params = []
            
        if schema:    
            query +="{}."
            params += [self.escape_identifier(schema)]

        query += "{}"
        params += [self.escape_identifier(table)]

        if cascade:
            query += " CASCADE"

        self.execute_query(self._sql.SQL(query).format(*params))


    def _update_column_info(self, info: ColumnInfo, cursor, index: int):
        info.name, info.sql_typecode, _display_size, _internal_size, _precision, _scale, _nullok_alwaysnone = cursor.description[index]
        type_info = OID_CATALOG.get(info.sql_typecode)
        if type_info:
            info.sql_type, info.python_type = type_info
    
    #endregion


    #region Copy

    def load_from_csv(self, file: os.PathLike|IOBase, table: str|tuple = None, *, columns: list[str] = None, encoding: str = 'utf-8', merge: Literal['truncate', 'truncate-cascade', 'upsert'] = None, noheaders: bool = False, csv_delimiter: str = None, csv_quotechar: str = None, csv_nullval: str = None) -> int:
        sche, tab = self.split_name(table)
        tmp_tab: str = None
        key_columns: list[str] = []
        nonkey_target_columns: list[str] = []

        _, csv_delimiter, csv_quotechar, csv_nullval = _get_csv_params(None, csv_delimiter, csv_quotechar, csv_nullval, context=file)

        try:
            if merge in ['truncate', 'truncate-cascade']:                
                self.truncate_table((sche, tab), cascade=merge == 'truncate-cascade')

            elif merge == 'upsert':
                with self.cursor() as cursor:
                    # Retrieve information about the columns
                    sql = """
                    WITH pk_columns AS (
                        SELECT c.column_name
                        FROM information_schema.table_constraints tc 
                        LEFT OUTER JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
                        LEFT OUTER JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
                        WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.constraint_schema = %(schema)s and tc.table_name = %(table)s
                    )
                    SELECT
                        c.ordinal_position AS position
                        ,c.column_name AS name
                        ,c.udt_name AS sql_type
                        ,c.is_nullable = 'YES' AS is_nullable
                        ,p.column_name IS NOT NULL AS is_primary_key
                    FROM information_schema.columns c
                    LEFT OUTER JOIN pk_columns p ON p.column_name = c.column_name
                    WHERE table_schema = %(schema)s AND table_name = %(table)s
                    """
                    logger.debug("Retrieve %s.%s columns", sche, tab)
                    target_colinfos = self.execute_query(sql, {'schema': sche, 'table': tab}, cursor=cursor, results=True)

                    # Build a temporary table
                    tmp_tab = f"tmp_{tab}_{token_hex(4)}"
                    params = []                
                    sql = "CREATE TEMPORARY TABLE {} ("; params += [self._sql.Identifier(tmp_tab)]
                    pk = []
                    target_colnames = set()

                    for i, colinfo in enumerate(target_colinfos):
                        name = colinfo['name']
                        
                        is_primary_key = colinfo['is_primary_key']
                        if columns and not name in columns:
                            if is_primary_key:
                                raise ValueError(f"Primary key column '{name}' must be included in the list of copied columns")
                            continue

                        sql += ("," if i > 0 else " ") + "{} {} {}"; params += [self._sql.Identifier(name), self._sql.Identifier(colinfo['sql_type']), self._sql.SQL('NULL' if colinfo['is_nullable'] else 'NOT NULL')]
                        target_colnames.add(name)
                        if is_primary_key:
                            pk.append(name)
                            key_columns.append(name)
                        else:
                            nonkey_target_columns.append(name)

                    # Additional columns if any ('COPY FROM' cannot discard some columns so we have to import them anyway)
                    if columns:
                        for name in columns:
                            if not name in target_colnames:
                                sql += ',{} text NULL'; params += [self._sql.Identifier(name)]
                    
                    if pk:
                        sql += ", PRIMARY KEY ("
                        for i, name in enumerate(pk):
                            sql += (", " if i > 0 else " ") + "{}"; params += [self._sql.Identifier(name)]
                        sql += ")"
                    
                    sql += ")"
                    
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug("columns (for COPY operation): %s", columns)
                        logger.debug("key_columns: %s", key_columns)
                        logger.debug("nonkey_target_columns: %s", nonkey_target_columns)

                    logger.debug("Create temp table %s", tmp_tab)
                    self.execute_query(self._sql.SQL(sql).format(*params), cursor=cursor)

            # Prepare actual copy operation
            sql = "COPY "; params = []
                
            if tmp_tab:
                sql += "{}"; params += [self.escape_identifier(tmp_tab)]
            else:    
                if sche:    
                    sql +="{}."; params += [self.escape_identifier(sche)]
                sql += "{}"; params += [self.escape_identifier(tab)]

            if columns:
                sql += " ("
                for i, colinfo in enumerate(columns):
                    sql += (", " if i > 0 else "") + "{}"; params += [self.escape_identifier(colinfo)]
                sql += ")"

            sql += " FROM STDIN (FORMAT csv"
            sql += ', ENCODING {}'; params.append('utf-8' if encoding == 'utf-8-sig' else self.escape_literal(encoding))
            sql += ', DELIMITER {}'; params.append(self.escape_literal(csv_delimiter))
            sql += ', QUOTE {}'; params.append(self.escape_literal(csv_quotechar))            
            sql += ', ESCAPE {}'; params.append(self.escape_literal(csv_quotechar))
            sql += ', NULL {}'; params.append(self.escape_literal(csv_nullval))
                
            if not noheaders:
                sql += ", HEADER match"
            sql += ")"

            with nullcontext(file) if isinstance(file, IOBase) else open(file, "rb") as fp:
                skip_utf8_bom(fp)
                if tmp_tab:
                    logger.debug("Actual copy from %s to %s", file, tmp_tab)
                result_count = self._actual_copy(self._sql.SQL(sql).format(*params), fp)
            
            # Upsert from tmp table if necessary
            if tmp_tab:
                params = []
                sql = "INSERT INTO {}.{} ("; params += [self._sql.Identifier(sche), self._sql.Identifier(tab)]
                for i, colinfo in enumerate([*key_columns, *nonkey_target_columns]):
                    sql += ("," if i > 0 else "") + "{}"; params += [self._sql.Identifier(colinfo)]
                sql += ") SELECT "
                for i, colinfo in enumerate([*key_columns, *nonkey_target_columns]):
                    sql += ("," if i > 0 else "") + "{}"; params += [self._sql.Identifier(colinfo)]
                sql += " FROM {}"; params += [self._sql.Identifier(tmp_tab)]
                sql += " ON CONFLICT ("
                for i, colinfo in enumerate(key_columns):
                    sql += ("," if i > 0 else "") + "{}"; params += [self._sql.Identifier(colinfo)]
                sql += ") DO UPDATE SET "
                for i, colinfo in enumerate(nonkey_target_columns):
                    sql += ("," if i > 0 else "") + "{}=EXCLUDED.{}"; params += [self._sql.Identifier(colinfo), self._sql.Identifier(colinfo)]
                
                logger.debug("upsert from %s to %s.%s", tmp_tab, sche, tab)
                self.execute_query(self._sql.SQL(sql).format(*params))

            return result_count

        finally:
            if tmp_tab:
                self.execute_query(self._sql.SQL("DROP TABLE IF EXISTS {}").format(self._sql.Identifier(tmp_tab)))

    

    def _actual_copy(self, query, fp):
        BUFFER_SIZE = 65536

        with self.cursor() as cursor:
            with cursor.copy(query) as copy:
                while True:
                    data = fp.read(BUFFER_SIZE)
                    if not data:
                        break
                    copy.write(data)
            return cursor.rowcount
        
    #endregion


try:
    from psycopg import Connection, Cursor, connect, sql
    from psycopg.errors import Diagnostic
    from psycopg.sql import Composable, Composed


    class PgAdapter(BasePgAdapter[Connection, Cursor, Composable, Composed]):
        """
        Database adapter for PostgreSQL (using `psycopg` (v3) driver).
        """
        _sql = sql

        @classmethod
        def is_available(cls):
            return True
        

        def register_notice_handler(self, if_exists = '__raise__', logprefix = 'pg'):
            if self.connection._notice_handlers:
                if if_exists != '__raise__':
                    return nullcontext(if_exists)
                raise ValueError(f"notice handler already registered: {self.connection._notice_handlers}")

            return PgNoticeManager(self.connection, logprefix)


    class PgNoticeManager:
        """
        This class can be used as a context manager that remove the handler on exit.

        The actual handler required by psycopg 3 `connection.add_notice_handler()` is the `pg_notice_handler` method.
        """
        def __init__(self, connection: Connection, logprefix: str = None):
            self.connection = connection
            self.logger = logging.getLogger(logprefix) if logprefix else None
            self.connection.add_notice_handler(self.handler)

        def __enter__(self):
            return self.handler
        
        def __exit__(self, *args):
            self.connection._notice_handlers.remove(self.handler)


        def handler(self, diag: Diagnostic):
            return pg_notice_handler(diag, logger=self.logger)


except ImportError:

    class PgAdapter(BasePgAdapter):
        """
        Database adapter for PostgreSQL (using `psycopg` (v3) driver).
        """

        @classmethod
        def is_available(cls):
            return False
