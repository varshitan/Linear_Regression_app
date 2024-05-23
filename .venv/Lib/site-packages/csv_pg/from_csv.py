from __future__ import annotations
from io import TextIOBase
import logging
import re
from psycopg import sql
from .base import BaseContext
from .column import DEFAULT_SEQ, Column, DataType

logger = logging.getLogger(__name__)

class FromCsvMixin(BaseContext):
    def prepare_columns_from_file(self) -> list[Column]|None:
        if not self.columns and self.noheader:
            raise ValueError(f"if csv file does not contain a header, columns must be given as an argument")

        if self.noheader:
            return None
        
        file_columns = self.get_file_columns()
        
        _, unused = self.merge_columns(file_columns, self.columns)
        self.columns_default = []

        if unused:
            for column in unused:
                if not column.notnull or column.default is not None:
                    self.columns_default.append(column)
                else:
                    raise ValueError("column not found in csv file and without a default value: %s" % ", ".join(unused))

        return file_columns


    def drop_table(self):
        logger.info("drop table %s.%s", self.schema, self.table)
        self.cursor.execute(sql.SQL("drop table {schema}.{table}").format(
            schema = sql.Identifier(self.schema),
            table = sql.Identifier(self.table),
        ))


    def truncate_table(self):
        logger.info("truncate table %s.%s", self.schema, self.table)
        self.cursor.execute(sql.SQL("truncate {schema}.{table}").format(
            schema = sql.Identifier(self.schema),
            table = sql.Identifier(self.table),
        ))


    def build_create_table_query(self):
        query = "create table {schema}.{table} ("
        params = {
            "schema": sql.Identifier(self.schema),
            "table": sql.Identifier(self.table),
        }

        nb_primary_keys = sum(1 if column.primarykey else 0 for column in self.columns)
        query_multi_primary_key = ""
        query_indexes = ""

        for index, column in enumerate(self.columns_default + self.columns):
            params[f"column{index}"] = sql.Identifier(column.slug)
            
            query += "\n  " + (',' if index >= 1 else '') + "{column%d}" % index + " "
            
            # append type
            if column.default == DEFAULT_SEQ:
                if column.datatype == DataType.BIGINT:
                    query += "bigserial"
                else:
                    query += "serial"
            elif column.datatype:
                query += column.datatype.value
            else:
                query += "text"

            # append precision and scale
            if isinstance(column.precision, int) and isinstance(column.scale, int):
                query += f"({column.precision},{column.scale})"
            elif isinstance(column.precision, int):
                query += f"({column.precision})"
                
            if column.notnull:
                query += " not null"

            if column.default and column.default != DEFAULT_SEQ:
                query += f" default {column.default}"

            if column.primarykey:
                if nb_primary_keys == 1:
                    query += " primary key"
                else:
                    query_multi_primary_key += (", " if query_multi_primary_key else "") + "{column%d}" % index

            if column.index:
                query_indexes += ("\n" if query_indexes else "") + "create index {ix%d} on {schema}.{table}({column%d});" % (index, index)
                params["ix%d" % index] = sql.Identifier(f"{self.table}_{column.slug}_idx")

        if query_multi_primary_key:
            query += f"\n  ,primary key ({query_multi_primary_key})"

        query += "\n);"

        if query_indexes:
            query += "\n" + query_indexes

        return sql.SQL(query).format(**params).as_string(self.connection)


    def create_table(self):
        # Create schema (if not exists)
        schema_exists = False
        logger.debug("check if schema %s exists", self.schema)
        self.cursor.execute("select 1 from information_schema.schemata where schema_name = %s", (self.schema,))
        for _ in self.cursor:
            schema_exists = True

        if not schema_exists:
            query = "create schema {schema}"
            params = {
                "schema": sql.Identifier(self.schema),
            }

            logger.info("create schema %s", self.schema)
            self.cursor.execute(sql.SQL(query).format(**params))

        # Create table
        logger.info("create table %s.%s", self.schema, self.table)
        query = self.build_create_table_query()
        logger.debug(query)
        self.cursor.execute(query)


    def prepare_table(self):
        table_columns = self.get_table_columns()
        if table_columns is None:
            self.create_table()

        if self.recreate:
            self.drop_table()
            self.create_table()

        if self.truncate:
            self.truncate_table()


    def warn_if_quoted_empty_string(self):
        # TODO
        logger.warning("warn_if_quoted_empty_string")


    def build_copy_from_query(self):
        query = "copy {schema}.{table} ("
        params = {
            "schema": sql.Identifier(self.schema),
            "table": sql.Identifier(self.table),
        }

        i = 0
        query_force_not_null = ""
        for column in self.columns:
            params["column%d" % i] = sql.Identifier(column.slug)

            # column name
            if i > 0:
                query += ", "
            query += "{column%d}" % i

            # notnull str columns
            if (column.datatype is None or column.datatype.is_str) and column.notnull:
                query_force_not_null += (", " if query_force_not_null else "") + "{column%d}" % i

            i += 1

        query += ")\nfrom stdin\nwith csv"
                
        if not self.noheader:
            query += " header"

        query += " encoding {encoding} delimiter {delimiter} quote {quote} escape {escape}"
        params["encoding"] = sql.Literal(self.encoding)
        params["delimiter"] = sql.Literal(self.delimiter)
        params["quote"] = sql.Literal(self.quote)
        params["escape"] = sql.Literal(self.escape)

        if query_force_not_null:
            query += " force not null " + query_force_not_null

        if self.where:
            query += f"\nwhere {self.where}"
                
        return sql.SQL(query).format(**params).as_string(self.connection)


    def build_alter_default_queries(self):
        updates = []
        reverts = []

        query = "alter table {schema}.{table} alter column {column} set default {default}"

        for column in self.columns_default:
            if column.default == DEFAULT_SEQ:
                continue

            if "__filename__" in column.default or "__filepath__" in column.default:
                params = {
                    "schema": sql.Identifier(self.schema),
                    "table": sql.Identifier(self.table),
                    "column": sql.Identifier(column.slug),
                }

                reverts.append(sql.SQL(query).format(default=sql.SQL(column.default), **params))

                updated_default = column.default.replace("__filename__", self.file.name).replace("__filepath__", self.filepath)

                updates.append(sql.SQL(query).format(default=sql.SQL(updated_default), **params))

        return updates, reverts


    def copy_from(self):
        query = self.build_copy_from_query()

        updates, reverts = self.build_alter_default_queries()

        rows = None
        try:
            for update in updates:
                logger.debug(update.as_string(self.connection))
                self.cursor.execute(update)
                
            logger.info(f"copy file {self.filepath} to table {self.schema}.{self.table}")
            
            with self.cursor.copy(query) as copy:
                if False: #TODO: any argument that requires row-by-row copy (e.g. decimal numbers?)
                    # Row-by-row copy
                    self.warn_if_quoted_empty_string()
                    for row in self.csv_reader:
                        print(row)
                        copy.write_row(row) # TODO: debug/test it
                else:
                    # Direct file copy
                    while data := self.txt_reader.read(self.blocksize):
                        copy.write(data)

            rows = self.cursor.rowcount
        finally:
            for revert in reverts:
                logger.debug(revert.as_string(self.connection))
                self.cursor.execute(revert)

        logger.info(f"{rows} rows copied")
        return rows


    def from_csv(self):
        with open(self.file, "r", newline="", encoding=self.encoding) as self.txt_reader:
            self.prepare_csv_params()
            self.prepare_columns_from_file()
            self.prepare_table()
            self.copy_from()
