from __future__ import annotations
import logging, re, unicodedata, csv, locale
from typing import Union
from types import ModuleType
from io import TextIOBase
from pathlib import Path
from psycopg import connect, sql, connection
from importlib import import_module
from .column import Column

logger = logging.getLogger(__name__)

VALID_DATEFORMATS = ["YMD", "DMY", "MDY"]
#TODO: VALID_DECIMALFORMATS = [",", "."]

class BaseContext:
    def __init__(self,
        # CSV file
        file: Path|str, encoding: str = None, relativeto: Path|str = None, blocksize: int = None,
        # Database table
        table: str = None, schema: str = None,
        # List of columns. For "copy from" this is a list of type specifications (all CSV columns must be copied). For "copy to" this is a list of table columns to include.
        columns: list[str] = None, noslug: bool = False, where: str = None,
        # Options to deal with existing table/file
        truncate: bool = False, recreate: bool = False,
        # CSV options
        noheader: bool = False, dialect: str|csv.Dialect = None, delimiter: str = None, quote: str = None, escape: str = None,
        # Format options
        dateformat: str = None,
        # Connection options
        settings: ModuleType|str = None, using: str = "default", **connect_kwargs
    ):
        self.file = file if isinstance(file, Path) else Path(file)
        self.encoding = encoding if encoding else "utf-8"
        
        if relativeto:
            relativeto = relativeto if isinstance(relativeto, Path) else Path(relativeto)
            self.filepath = self.file.relative_to(relativeto).as_posix()
        else:
            self.filepath = self.file.as_posix()

        self.blocksize = blocksize if blocksize else 8192
        
        if not table:
            table = self.table_for_file()

        if not schema:
            pos = table.find(".")
            if pos > 0 and len(table) > pos + 1:
                schema = table[0:pos]
                table = table[pos+1:]
            else:
                schema = "public"

        self.table = table
        self.schema = schema
        
        self.noslug = noslug
        self.columns: list[Column] = [Column.from_spec(column, slug_func = None if self.noslug else self.slugen_identifier) for column in columns] if columns else []
        self.where = where # WARNING: raw SQL, subject to SQL injections - TODO/SECURITY: how to secure this?

        self.truncate = truncate
        self.recreate = recreate

        # CSV options
        self.noheader = noheader
        self.dialect = dialect
        self.delimiter = delimiter
        self.quote = quote
        self.escape = escape

        # Format options
        # TODO: numeric format option? setting lc_numeric does not work with COPY, even if it works with to_number: `set lc_numeric = "de_DE.UTF-8"; select to_number('9669,84', '99999D999')`
        self.datestyle = None
        if dateformat:
            if not dateformat in VALID_DATEFORMATS:
                raise ValueError(f"invalid value \"{dateformat}\" for dateformat, must be one of: {', '.join(VALID_DATEFORMATS)}")
            self.datestyle = f"ISO, {dateformat}"

        # Build connect kwargs
        self.connect_kwargs = self.build_connect_kwargs(settings, using)
        for key, value in connect_kwargs.items():
            self.connect_kwargs[key] = value

        if not self.connect_kwargs:
            try:
                from django.conf import settings # type: ignore
                self.connect_kwargs = self.build_connect_kwargs(settings.DATABASES, using)
            except:
                self.connect_kwargs = "" # will use environment variables


    # Assigned after __int__
    txt_reader: TextIOBase


    def build_connect_kwargs(self, databases: dict|ModuleType|str, using: str):
        if not isinstance(databases, dict):
            if isinstance(databases, ModuleType):
                databases = getattr(databases, "DATABASES")
            elif isinstance(databases, str):
                module = import_module(databases)
                databases = getattr(module, "DATABASES")
            elif databases is None:
                return {}
            else:
                raise ValueError(f"invalid type for settings: {type(databases).__name__}")

        database = databases[using]

        connect_kwargs = {}
        if "NAME" in database:
            connect_kwargs["dbname"] = database["NAME"]
        if "HOST" in database:
            connect_kwargs["host"] = database["HOST"]
        if "PORT" in database:
            connect_kwargs["port"] = database["PORT"]
        if "USER" in database:
            connect_kwargs["user"] = database["USER"]
        if "PASSWORD" in database:
            connect_kwargs["password"] = database["PASSWORD"]
        return connect_kwargs


    def __enter__(self):
        # A dedicated session (connection) is always opened so that it can be specially parametized for COPY command
        if isinstance(self.connect_kwargs, str):
            self.connection = connect(self.connect_kwargs)
        else:
            self.connection = connect(**self.connect_kwargs)
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()
        if self.datestyle:
            self.cursor.execute(sql.SQL("set datestyle to {datestyle}").format(datestyle=sql.Identifier(self.datestyle)))
        return self


    def __exit__(self, *args):
        self.cursor.close()
        self.connection.close()


    def slugen_identifier(self, value) -> str:
        from zut import slugen
        return slugen(value, separator='_')


    def table_for_file(self):
        return self.slugen_identifier(self.file.stem)


    def get_table_columns(self):        
        logger.debug("get table columns for %s.%s", self.schema, self.table)

        query = """
        select
            column_name, data_type, coalesce(numeric_precision, character_maximum_length, datetime_precision), numeric_scale, is_nullable = 'NO', column_default
        from information_schema.columns
        where table_schema=%s and table_name=%s
        order by ordinal_position
        """

        self.cursor.execute(query, (self.schema, self.table))
        columns: list[Column] = []
        for row in self.cursor:
            column = Column(
                name = row[0],
                datatype = row[1],
                precision = row[2],
                scale = row[3],
                notnull = row[4],
                default = row[5],
                slug_func = None if self.noslug else self.slugen_identifier,
            )

            columns.append(column)

        if not columns:
            return None # table does not exist

        # determine which columns are part of 1-column indexes
        query = """
        select
            a.attname as column_name
            ,i.relname as index_name
            ,ix.indnatts
            ,ix.indisprimary
        from pg_index ix
        inner join pg_class t on t.oid = ix.indrelid
        inner join pg_class i on i.oid = ix.indexrelid
        inner join pg_namespace n on n.oid = t.relnamespace
        inner join pg_attribute a on a.attrelid = t.oid and a.attnum = ANY(ix.indkey)
        where n.nspname = %s and t.relname = %s
        """
        self.cursor.execute(query, (self.schema, self.table))
        for row in self.cursor:
            column_name = row[0]
            index_name = row[1]
            indnatts = row[2]
            indisprimary = row[3]

            for column in columns:
                if column.name == column_name:
                    if indisprimary:
                        column.primarykey = True
                    elif indnatts == 1: #TODO: handle multicolumn indexes
                        column.index = True
                    break
        
        logger.debug("table columns for %s.%s: %s", self.schema, self.table, [str(column) for column in columns])
        return columns

    def ignore_bom(self):
        """
        Ignore UTF-8 BOM if file starts with it.
        """
        if self.encoding == "utf-8":
            data = self.txt_reader.read(1)
            if data != "\ufeff":
                self.txt_reader.seek(0) # move back to top


    def get_file_columns(self) -> list[Column]:
        logger.debug("get file columns")

        self.ignore_bom()

        header_row = next(self.csv_reader)        
        self.txt_reader.seek(0) # move back to top

        columns = [Column.from_spec(header_value, slug_func = None if self.noslug else self.slugen_identifier) for header_value in header_row]
        logger.debug("file columns: %s", [str(column) for column in columns])

        return columns

    @property
    def csv_reader(self):
        try:
            return self._csv_reader

        except AttributeError:
            if self.escape == self.quote:
                escapechar = None
                doublequote = True
            else:
                escapechar = self.escape
                doublequote = False
            
            self._csv_reader = csv.reader(self.txt_reader, delimiter=self.delimiter, quotechar=self.quote, escapechar=escapechar, doublequote=doublequote)
            return self._csv_reader

    def get_default_dialect_name(self):
        dialects = csv.list_dialects()
        locs = locale.getdefaultlocale()
        for loc in locs:
            m = re.match(r"^([a-z]{2})_[A-Z]{2}$", loc)
            if m:
                lang = m.group(1)
                name = f"excel-{lang}"
                if name in dialects or lang == "fr":
                    return name
        
        return "excel"


    def get_dialect(self, name: str = None):
        if not name:
            name = self.get_default_dialect_name()

        if name == "excel-fr":
            if not "excel-fr" in csv.list_dialects():
                class excel_fr(csv.excel):
                    delimiter = ";"
                csv.register_dialect(f"excel-fr", excel_fr)
        
        return csv.get_dialect(name)


    def prepare_csv_params(self):
        def display_quoted_char(s):
            if s is None:
                return "<none>"
            if s == "\t":
                return "<\\t>"
            if s == "\r":
                return "<\\r>"
            if s == "\n":
                return "<\\n>"
            if s == "\"":
                return "<\">"
            return f"<{s}>"

        if self.dialect and not isinstance(self.dialect, csv.Dialect):
            self.dialect = self.get_dialect(self.dialect)

        if not self.dialect and not self.delimiter and not self.quote and not self.escape:
            if hasattr(self, "txt_reader"):
                logger.info(f"detect dialect for {self.filepath}")
                sample = "\n".join(self.txt_reader.readlines(100))
                self.dialect = csv.Sniffer().sniff(sample, delimiters=[";", ",", "|", "\t"])
                self.txt_reader.seek(0)
            else:
                self.dialect = self.get_dialect()

        if not self.delimiter:
            if self.dialect.delimiter is not None:
                self.delimiter = self.dialect.delimiter
            else:
                self.delimiter = ","

        if not self.quote:
            if self.dialect.quotechar is not None:
                self.quote = self.dialect.quotechar
            else:
                self.quote = "\""
        
        if not self.escape:
            if self.dialect.escapechar is not None:
                self.escape = self.dialect.escapechar
            else:
                self.escape = self.quote

        logger.debug(f"using csv params: delimiter={display_quoted_char(self.delimiter)} quote={display_quoted_char(self.quote)} escape={display_quoted_char(self.escape)}")


    def merge_columns(self, target_columns: list[Column], other_columns: list[Column]) -> tuple(list[Column],list[Column]):
        if not target_columns:
            self.columns = other_columns
            return (None,None)
        elif not other_columns:
            self.columns = target_columns
            return (None,None)

        other_columns_dict = {column.slug: column for column in other_columns}

        unmerged = []
        for column in target_columns:
            other_column = other_columns_dict.pop(column.slug, None)
            if other_column is None:
                unmerged.append(column)
            else:
                column.merge(other_column)

        self.columns = target_columns
        logger.debug("merged columns: %s", [str(column) for column in self.columns])

        unused = [column for column in other_columns_dict.values()]
        return unmerged, unused
