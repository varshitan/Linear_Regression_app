"""
Common operations on databases.
"""
from __future__ import annotations

import re
from urllib.parse import urlparse

from .base import DbAdapter, _get_connection_from_wrapper
from .mssql import MssqlAdapter
from .mysql import MysqlAdapter
from .pg import PgAdapter
from .pg2 import Pg2Adapter


def get_db_adapter(origin) -> DbAdapter:
    if isinstance(origin, str):
        if origin.startswith('db:'):
            origin = origin[3:]

        r = urlparse(origin)

        if r.scheme in ['postgresql', 'postgres', 'pg']:
            if PgAdapter.is_available():
                adapter = PgAdapter
            elif Pg2Adapter.is_available():
                adapter = Pg2Adapter
            else:
                raise ValueError(f"PgAdapter and Pg2Adapter not available (psycopg missing)")
        elif r.scheme in ['mssql', 'mssqls']:
            adapter = MssqlAdapter
        elif r.scheme in ['mysql', 'mariadb']:
            adapter = MysqlAdapter
        elif r.scheme:
            raise ValueError(f"unsupported db engine: {r.scheme}")
        else:
            raise ValueError(f"invalid db: no scheme in {origin}")
        
        if not adapter.is_available():
            raise ValueError(f"cannot use db {r.scheme} ({adapter.__name__} not available)")
        
        return adapter(r)
    
    elif isinstance(origin, dict) and 'ENGINE' in origin: # Django
        engine = origin['ENGINE']
        if engine in ["django.db.backends.postgresql", "django.contrib.gis.db.backends.postgis"]:
            if PgAdapter.is_available():
                return PgAdapter(origin), None, None
            else:
                return Pg2Adapter(origin), None, None
        elif engine in ["django.db.backends.mysql", "django.contrib.gis.db.backends.mysql"]:
            return MysqlAdapter(origin), None, None
        elif engine in ["mssql"]:
            return MssqlAdapter(origin), None, None
        else:
            raise ValueError(f"invalid db: unsupported django db engine: {engine}")
        
    elif isinstance(origin, DbAdapter):
        return origin, None, None
    
    else: # connection types
        origin = _get_connection_from_wrapper(origin)
        
        type_fullname: str = type(origin).__module__ + '.' + type(origin).__qualname__

        if type_fullname == 'psycopg2.extension.connection':
            return Pg2Adapter(origin), None, None
        elif type_fullname == 'psycopg.Connection':
            return PgAdapter(origin), None, None
        elif type_fullname == 'MySQLdb.connections.Connection':
            return MysqlAdapter(origin), None, None
        elif type_fullname == 'pyodbc.Connection':
            return MssqlAdapter(origin), None, None

        raise ValueError(f"invalid db: unsupported origin type: {type(origin)}")


__all__ = (
    'DbAdapter', 'MssqlAdapter', 'MysqlAdapter', 'PgAdapter', 'Pg2Adapter',
    'get_db_adapter',
)
