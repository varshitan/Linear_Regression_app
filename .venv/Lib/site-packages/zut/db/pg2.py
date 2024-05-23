from __future__ import annotations

import logging
import re
from contextlib import nullcontext
from urllib.parse import urlparse

from .. import build_url
from .pg import BasePgAdapter, pg_get_logging_level

try:
    from psycopg2 import connect, sql
    from psycopg2.extensions import connection, cursor
    from psycopg2.sql import Composable, Composed

    class Pg2Adapter(BasePgAdapter[connection, cursor, Composable, Composed]):
        """
        Database adapter for PostgreSQL (using `psycopg2` driver).
        """

        EXPECTED_CONNECTION_TYPES = ['psycopg2.extensions.connection']
        _sql = sql

        @classmethod
        def is_available(cls):
            return True

        def _create_connection(self):
            kwargs = {}
            
            r = urlparse(self._connection_url)

            if r.hostname:
                kwargs['host'] = r.hostname
            if r.port:
                kwargs['port'] = r.port

            name = r.path.lstrip('/')
            if name:
                kwargs['dbname'] = name

            if r.username:
                kwargs['user'] = r.username
            if r.password:
                kwargs['password'] = r.password

            conn = connect(**kwargs)
            conn.autocommit = self.autocommit
            return conn
        

        def _get_url_from_connection(self):    
            params = self.connection.get_dsn_parameters()
            return build_url(
                scheme=self.URL_SCHEME,
                path='/' + params.get('dbname', None),
                hostname=params.get('host', None),
                port=params.get('port', None),
                username=params.get('user', None),
                password=params.get('password', None),
            )
        

        def _actual_copy(self, query, fp):
            with self.cursor() as cursor:
                cursor.copy_expert(query, fp)
                return cursor.rowcount
        

        def register_notice_handler(self, logprefix = None, if_exists = '__raise__'):
            if self.connection.notices:
                if if_exists != '__raise__':
                    return nullcontext(if_exists)
                raise ValueError(f"notice handler already registered: {self.connection.notices}")

            return Pg2NoticeHandler(self.connection, logprefix)


    class Pg2NoticeHandler:
        """
        This class is the actual handler required by psycopg 2 `connection.notices`.
        
        It can also be used as a context manager that remove the handler on exit.
        """
        _pg_msg_re = re.compile(r"^(?P<pglevel>[A-Z]+)\:\s(?P<message>.+(?:\r?\n.*)*)$", re.MULTILINE)

        def __init__(self, connection, logprefix: str = None):
            self.connection = connection
            self.logger = logging.getLogger(logprefix if logprefix else 'pg')
            self.connection.notices = self

        def __enter__(self):
            return self
        
        def __exit__(self, *args):
            self.connection.notices = []

            
        def append(self, fullmsg: str):
            fullmsg = fullmsg.strip()
            m = self._pg_msg_re.match(fullmsg)
            if not m:
                self.logger.error(fullmsg)
                return

            message = m.group("message").strip()
            severity = m.group("pglevel")
            level = pg_get_logging_level(severity)

            self.logger.log(level, message)


except ImportError:    
    class Pg2Adapter(BasePgAdapter):
        """
        Database adapter for PostgreSQL (using `psycopg2` driver).
        """

        @classmethod
        def is_available(cls):
            return False
