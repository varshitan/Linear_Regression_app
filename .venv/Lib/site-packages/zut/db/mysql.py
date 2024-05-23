from __future__ import annotations

from decimal import Decimal
import logging
import os
import re
from contextlib import nullcontext
from datetime import date, datetime, timedelta, tzinfo
from io import IOBase, TextIOWrapper
from pathlib import Path
from urllib.parse import unquote, urlparse

from .. import OutTable, Literal, build_url, skip_utf8_bom, _get_csv_params
from .base import ColumnInfo, DbAdapter

logger = logging.getLogger(__name__)
notice_logger = logging.getLogger("mysql")

try:
    from MySQLdb import connect
    from MySQLdb.connections import Connection
    from MySQLdb.cursors import Cursor
    from MySQLdb.constants import FIELD_TYPE

    # TODO: cursor/procedure messages (_log_execute_messages?)
    class MysqlAdapter(DbAdapter[Connection, Cursor, str, str]):
        """
        Database adapter for Microsoft SQL Server (using `pyodbc` driver).
        """
        URL_SCHEME = 'mysql'
        EXPECTED_CONNECTION_TYPES = ['MySQLdb.connections.Connection'] # Compatible with the Python DB API interface version 2 (not "_mysql.connection")

        @classmethod
        def is_available(cls):
            return True
        

        def _create_connection(self) -> Connection:
            r = urlparse(self._connection_url)
            kwargs = {}
            if r.hostname:
                kwargs['host'] = r.hostname
            if r.port:
                kwargs['port'] = r.port
            if r.path:
                kwargs['database'] = r.path.lstrip('/')
            if r.username:
                kwargs['user'] = r.username
            if r.password:
                kwargs['password'] = r.password
            return connect(**kwargs, sql_mode='STRICT_ALL_TABLES', autocommit=self.autocommit)


        def _get_url_from_connection(self):
            raise NotImplementedError() # TODO
        

        # -------------------------------------------------------------------------
        # Queries
        #

        def get_select_table_query(self, table: str|tuple = None, *, schema_only = False) -> str:
            _, table = self.split_name(table)
            
            query = f'SELECT * FROM {self.escape_identifier(table)}'
            if schema_only:
                query += ' WHERE 1 = 0'

            return query
            

        def escape_identifier(self, value: str) -> str:
            if '`' in value:
                raise ValueError(f"Identifier cannot contain back ticks.")
            return f"`" + value + "`"
        

        def escape_literal(self, value: str) -> str:
            return f"'" + value.replace("'", "''") + "'"


        # -------------------------------------------------------------------------
        # Tables and columns
        #    
    
        def split_name(self, name: str|tuple = None) -> tuple[str,str]:
            schema, name = super().split_name(name)
            if schema is not None:
                raise ValueError(f"Cannot use schema (\"{schema}\") with mysql.")
            return None, name


        def table_exists(self, table: str|tuple = None) -> bool:        
            _, table = self.split_name(table)

            query = "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = %s)"
            params = [table]

            return self.get_scalar(query, params) == 1

            
        def truncate_table(self, table: str|tuple = None, *, cascade: bool = False):
            if cascade:
                raise ValueError("Cascade truncate is not supported by mssql.")
            super().truncate_table(table)
            

        def _update_column_info(self, info: ColumnInfo, cursor: Cursor, index: int):
            info.name, info.sql_typecode, _display_size, _internal_size, _precision, _scale, nullok = cursor.description[index]

            info.nullable = nullok == 1
            
            if info.sql_typecode == FIELD_TYPE.TINY:
                info.sql_type = 'TINY'
            elif info.sql_typecode == FIELD_TYPE.ENUM:
                info.sql_type = 'ENUM'
            else:
                for name in dir(FIELD_TYPE):
                    if name.startswith('_'):
                        continue
                    code = getattr(FIELD_TYPE, name)
                    if code == info.sql_typecode:
                        info.sql_type = name
                        break
            
            info.python_type = PYTHON_TYPES.get(info.sql_typecode)


        def load_from_csv(self, file: os.PathLike|IOBase, table: str|tuple = None, *, columns: list[str] = None, encoding: str = 'utf-8', merge: Literal['truncate', 'truncate-cascade', 'upsert'] = None, noheaders: bool = False, csv_delimiter: str = None, csv_quotechar: str = None, csv_nullval: str = None) -> int:
            if isinstance(file, IOBase):
                # TODO: use named pipes?
                raise NotImplementedError("Cannot use IOBase file with mssql.")
            
            # TODO: end-of-line of CSV input should be verified. Windows end-of-lines does not work as of now.
            # (e.g. on Windows, tests/samples/mixed-mysql.csv is forced to LF. If not, "test_load_csv_ordered" results in last column being 0 instead of NULL, and "test_load_csv_upsert" fails)

            _, tab = self.split_name(table)
            _, csv_delimiter, csv_quotechar, csv_nullval = _get_csv_params(None, csv_delimiter, csv_quotechar, csv_nullval, context=file)

            if merge in ['truncate', 'truncate-cascade']:                
                self.truncate_table((None, tab), cascade=merge == 'truncate-cascade')

            sql = f"LOAD DATA LOCAL INFILE {self.escape_literal(str(os.path.abspath(file)).replace('\\', '\\\\'))}"
            if merge == 'upsert':
                sql += " REPLACE"
            sql += f" INTO TABLE {self.escape_identifier(tab)}"

            sql += f' CHARACTER SET {self.escape_literal('utf8' if encoding in ['utf-8', 'utf-8-sig'] else encoding)}'
            sql += f' FIELDS TERMINATED BY {self.escape_literal(csv_delimiter)}'
            sql += f' ENCLOSED BY {self.escape_literal(csv_quotechar)}'
            sql += f' ESCAPED BY {self.escape_literal(csv_quotechar)}'
            #TODO: csv_nullval handling?
            
            if not noheaders:
                #TODO: check that file headers match given columns
                sql += " IGNORE 1 LINES"
            
            if columns:
                sql += " ("
                for i, colinfo in enumerate(columns):
                    sql += (", " if i > 0 else "") + f"{self.escape_identifier(colinfo)}"
                sql += ")"
            
            #TODO skip_utf8_bom(fp)
            with self.cursor() as cursor:
                self.execute_query(sql)
                result_count = cursor.rowcount
            
            return result_count


    PYTHON_TYPES = {
        FIELD_TYPE.DECIMAL: Decimal,
        FIELD_TYPE.TINY: int,
        FIELD_TYPE.SHORT: int,
        FIELD_TYPE.LONG: int,
        FIELD_TYPE.FLOAT: float,
        FIELD_TYPE.DOUBLE: float,
        FIELD_TYPE.NULL: None,
        FIELD_TYPE.TIMESTAMP: datetime,
        FIELD_TYPE.LONGLONG: int,
        FIELD_TYPE.INT24: int,
        FIELD_TYPE.DATE: date,
        FIELD_TYPE.TIME: timedelta,
        FIELD_TYPE.DATETIME: datetime,
        FIELD_TYPE.YEAR: int,
        FIELD_TYPE.VARCHAR: str,
        FIELD_TYPE.BIT: bytes,
        FIELD_TYPE.JSON: None,
        FIELD_TYPE.NEWDECIMAL: Decimal,
        FIELD_TYPE.ENUM: None,
        FIELD_TYPE.SET: None,
        FIELD_TYPE.TINY_BLOB: str,
        FIELD_TYPE.MEDIUM_BLOB: str,
        FIELD_TYPE.LONG_BLOB: str,
        FIELD_TYPE.BLOB: str,
        FIELD_TYPE.VAR_STRING: str,
        FIELD_TYPE.STRING: str,
        FIELD_TYPE.GEOMETRY: None,
    }

except ImportError:  

    class MysqlAdapter(DbAdapter):
        """
        Database adapter for Microsoft SQL Server (using `pyodbc` driver).
        """
                
        URL_SCHEME = 'mysql'
        EXPECTED_CONNECTION_TYPES = ['MySQLdb.connections.Connection'] # Compatible with the Python DB API interface version 2 (not "_mysql.connection")

        @classmethod
        def is_available(cls):
            return False
