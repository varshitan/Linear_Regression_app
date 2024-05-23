from __future__ import annotations

import logging
import os
import re
from contextlib import nullcontext
from datetime import tzinfo
from io import IOBase, TextIOWrapper
from pathlib import Path
from urllib.parse import unquote, urlparse

from .. import OutTable, Literal, build_url, skip_utf8_bom, _get_csv_params
from .base import ColumnInfo, DbAdapter

logger = logging.getLogger(__name__)
notice_logger = logging.getLogger("mssql")

try:
    from pyodbc import Connection, Cursor, connect, drivers

    class MssqlAdapter(DbAdapter[Connection, Cursor, str, str]):
        """
        Database adapter for Microsoft SQL Server (using `pyodbc` driver).
        """
        URL_SCHEME = 'mssql' # or mssqls (if encrypted)
        DEFAULT_SCHEMA = 'dbo'
        ONLY_POSITIONAL_PARAMS = True
        EXPECTED_CONNECTION_TYPES = ['pyodbc.Connection']

        @classmethod
        def is_available(cls):
            return True
        

        def _create_connection(self) -> Connection:
            
            def escape(s):
                if ';' in s or '{' in s or '}' in s or '=' in s:
                    return "{" + s.replace('}', '}}') + "}"
                else:
                    return s
                
            r = urlparse(self._connection_url)
            
            server = unquote(r.hostname) or '(local)'
            if r.port:
                server += f',{r.port}'

            # Use "ODBC Driver XX for SQL Server" if available ("SQL Server" seems not to work with LocalDB, and takes several seconds to establish connection on my standard Windows machine with SQL Server Developer).
            driver = "SQL Server"
            for a_driver in sorted(drivers(), reverse=True):
                if re.match(r'^ODBC Driver \d+ for SQL Server$', a_driver):
                    driver = a_driver
                    break

            connection_string = 'Driver={%s};Server=%s;Database=%s;' % (escape(driver), escape(server), escape(r.path.lstrip('/')))

            if r.username:
                connection_string += 'UID=%s;' % escape(unquote(r.username))
                if r.password:
                    connection_string += 'PWD=%s;' % escape(unquote(r.password))
            else:
                connection_string += 'Trusted_Connection=yes;'
                
            connection_string += f"Encrypt={'yes' if r.scheme == 'mssqls' else 'no'};"
            return connect(connection_string, autocommit=self.autocommit)


        def _get_url_from_connection(self):
            with self.cursor() as cursor:
                cursor.execute("SELECT @@SERVERNAME, local_tcp_port, SUSER_NAME(), DB_NAME() FROM sys.dm_exec_connections WHERE session_id = @@spid")
                host, port, user, dbname = next(iter(cursor))
            return build_url(scheme=self.URL_SCHEME, username=user, hostname=host, port=port, path='/'+dbname)
        

        # -------------------------------------------------------------------------
        # Execution
        #
        
        def execute_file(self, path: str|Path, params: list|tuple|dict = None, *, cursor: Cursor = None, results: bool|TextIOWrapper|OutTable|str|Path = False, tz: tzinfo = None, limit: int = None, offset: int = None, encoding: str = 'utf-8') -> None:
            import sqlparse  # not at the top because the enduser might not need this feature

            # Read file
            with open(path, 'r', encoding=encoding) as fp:
                skip_utf8_bom(fp)
                file_content = fp.read()

            # Split queries
            queries = sqlparse.split(file_content, encoding)
                
            # Execute all queries
            query_count = len(queries)
            with nullcontext(cursor) if cursor else self.cursor() as _cursor:
                for query_index, query in enumerate(queries):
                    query_num = query_index + 1
                    if logger.isEnabledFor(logging.DEBUG):
                        title = re.sub(r"\s+", " ", query).strip()[0:100] + "…"
                        logger.debug("Execute query %d/%d: %s ...", query_num, query_count, title)

                    # Execute query
                    query_id = f'{query_num}/{query_count}' if query_count > 1 else None
                    if query_num < query_count:
                        # Not last query: should not have results
                        self.execute_query(query, params, cursor=_cursor, results='warning', tz=tz, query_id=query_id, limit=limit, offset=offset)

                    else:
                        # Last query
                        return self.execute_query(query, params, cursor=_cursor, results=results, tz=tz, query_id=query_id, limit=limit, offset=offset)


        # -------------------------------------------------------------------------
        # Queries
        #
            
        def _paginate_parsed_query(self, selectpart: str, orderpart: str, *, limit: int|None, offset: int|None) -> str:
            if orderpart:
                result = f"{selectpart} {orderpart} OFFSET {offset or 0} ROWS"
                if limit is not None:
                    result += f" FETCH NEXT {limit} ROWS ONLY"
                return result
            elif limit is not None:
                if offset is not None:
                    raise ValueError("an ORDER BY clause is required for OFFSET")
                return f"SELECT TOP {limit} * FROM ({selectpart}) s"
            else:
                return selectpart


        def get_select_table_query(self, table: str|tuple = None, *, schema_only = False) -> str:
            schema, table = self.split_name(table)
            
            query = f'SELECT * FROM {self.escape_identifier(schema)}.{self.escape_identifier(table)}'
            if schema_only:
                query += ' WHERE 1 = 0'

            return query
            

        def escape_identifier(self, value: str) -> str:
            return f"[{value.replace(']', ']]')}]"
        

        def escape_literal(self, value: str) -> str:
            return f"'" + value.replace("'", "''") + "'"


        def _log_execute_messages(self, cursor: Cursor):
            if cursor.messages:
                for msg_type, msg_text in cursor.messages:
                    m = re.match(r"^\[Microsoft\]\[ODBC Driver \d+ for SQL Server\]\[SQL Server\](.+)$", msg_text)
                    if m:
                        msg_text = m[1]
                    
                    if msg_type in {"[01000] (0)", "[01000] (50000)"}: # PRINT or RAISERROR
                        level = logging.INFO
                    else:
                        msg_text = f"{msg_type} {msg_text}"
                        if msg_type == "[01003] (8153)": # Avertissement : la valeur NULL est éliminée par un agrégat ou par une autre opération SET.
                            level = logging.INFO
                        else:
                            level = logging.WARNING

                    notice_logger.log(level, f"{msg_text}")

        # -------------------------------------------------------------------------
        # Tables and columns
        #    

        def table_exists(self, table: str|tuple = None) -> bool:        
            schema, table = self.split_name(table)

            query = "SELECT CASE WHEN EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?) THEN 1 ELSE 0 END"
            params = [schema, table]

            return self.get_scalar(query, params) == 1

            
        def truncate_table(self, table: str|tuple = None, *, cascade: bool = False):
            if cascade:
                raise ValueError("Cascade truncate is not supported by mssql.")
            super().truncate_table(table)
            

        def _update_column_info(self, info: ColumnInfo, cursor: Cursor, index: int):
            info.name, info.python_type, display_size, internal_size, precision, scale, info.nullable = cursor.description[index]

        #endregion


        def load_from_csv(self, file: os.PathLike, table: str|tuple = None, *, columns: list[str] = None, encoding: str = 'utf-8', merge: Literal['truncate', 'truncate-cascade', 'upsert'] = None, noheaders: bool = False, csv_delimiter: str = None, csv_quotechar: str = None, csv_nullval: str = None) -> int:
            if isinstance(file, IOBase):
                # TODO: use named pipes?
                raise NotImplementedError("Cannot use IOBase file with mssql.")
            if columns or not noheaders:
                # TODO: compare CSV headers with columns (check consistency) + use named pipes?
                raise NotImplementedError("Arguments 'columns' or 'noheaders' cannot be used yet.")

            sche, tab = self.split_name(table)
            tmp_tab: str = None
            key_columns: list[str] = []
            nonkey_target_columns: list[str] = []

            _, csv_delimiter, csv_quotechar, csv_nullval = _get_csv_params(None, csv_delimiter, csv_quotechar, csv_nullval, context=file)
            if csv_nullval != '':
                raise ValueError(f"Invalid csv nullval for mssql: \"{csv_nullval}\"")

            try:
                if merge in ['truncate', 'truncate-cascade']:                
                    self.truncate_table((sche, tab), cascade=merge == 'truncate-cascade')

                elif merge == 'upsert':
                    raise NotImplementedError("Cannot use 'upsert' yet") #TODO

                # Prepare actual copy operation
                sql = f"BULK INSERT "
                    
                if tmp_tab:
                    sql += f"{self.escape_identifier(tmp_tab)}"
                else:    
                    if sche:    
                        sql +=f"{self.escape_identifier(sche)}."
                    sql += f"{self.escape_identifier(tab)}"

                sql += f" FROM {self.escape_literal(os.path.abspath(file))}"
                sql += f' WITH ('
                sql += f' CODEPAGE = {self.escape_literal('utf-8' if encoding == 'utf-8-sig' else encoding)}'
                sql += f', FIELDTERMINATOR = {self.escape_literal(csv_delimiter)}'
                sql += f', FIELDQUOTE = {self.escape_literal(csv_quotechar)}'
                if csv_nullval == '':
                    sql += f', KEEPNULLS'
                sql += ")"

                if tmp_tab:
                    logger.debug("Actual copy from %s to %s", file, tmp_tab)
                
                #TODO: skip_utf8_bom(fp)
                with self.cursor() as cursor:
                    self.execute_query(sql)
                    result_count = cursor.rowcount
                
                # Upsert from tmp table if necessary
                if tmp_tab:
                    pass #TODO

                return result_count

            finally:
                if tmp_tab:
                    self.execute_query(f"DROP TABLE IF EXISTS {self.escape_identifier(tmp_tab)}")



except ImportError:  

    class MssqlAdapter(DbAdapter):
        """
        Database adapter for Microsoft SQL Server (using `pyodbc` driver).
        """
                
        URL_SCHEME = 'mssql' # or mssqls (if encrypted)
        DEFAULT_SCHEMA = 'dbo'
        ONLY_POSITIONAL_PARAMS = True
        EXPECTED_CONNECTION_TYPES = ['pyodbc.Connection']

        @classmethod
        def is_available(cls):
            return False
