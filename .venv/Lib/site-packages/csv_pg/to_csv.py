from __future__ import annotations
import logging
from psycopg import sql
from .base import BaseContext

logger = logging.getLogger(__name__)

class ToCsvMixin(BaseContext):
    def prepare_columns_from_table(self):
        table_columns = self.get_table_columns()

        unmerged, _ = self.merge_columns(self.columns, table_columns)

        if unmerged:
            raise ValueError("column not found in table: %s" % ", ".join(unmerged))

    def prepare_file(self):
        if not self.file.exists():
            return
            
        if self.truncate or self.recreate:
            logger.info("csv file {self.filepath} already exists, will be recreated")
            return

        # determine whether we can append to the existing file
        if self.noheader:
            logger.info(f"csv file {self.filepath} already exists, will append to it")
            return

        self.noheader = True # don't append headers

        with open(self.file, "r", newline="", encoding=self.encoding) as self.txt_reader:
            file_columns = self.get_file_columns()

        # reorder columns to match those of the file
        current_columns_dict = {column.slug: column for column in self.columns}
        reordered_columns = []
        for index, column in enumerate(file_columns):
            found = current_columns_dict.pop(column.slug, None)
            if found is None:
                raise ValueError(f"csv file {self.filepath} already exists with unexpected column {index+1} named \"{column.name}\"")
            reordered_columns.append(found)

        self.columns = reordered_columns
        logger.debug("reordered columns: %s", [str(column) for column in self.columns])

        if current_columns_dict:
            logger.warning(f"csv file {self.filepath} already exists with missing columns %s, will append to it ignoring missing columns", [str(column) for column in current_columns_dict.values()])
        else:
            logger.info(f"csv file {self.filepath} already exists, will append to it")

    
    def build_copy_to_query(self):
        query = "copy {schema}.{table} ("
        params = {
            "schema": sql.Identifier(self.schema),
            "table": sql.Identifier(self.table),
        }

        i = 0
        for column in self.columns:
            params["column%d" % i] = sql.Identifier(column.name)

            # column name
            if i > 0:
                query += ", "
            query += "{column%d}" % i

            i += 1

        query += ") to stdout with csv"

        if not self.noheader:
            query += " header"

        query += " encoding {encoding} delimiter {delimiter} quote {quote} escape {escape}"
        params["encoding"] = sql.Literal(self.encoding)
        params["delimiter"] = sql.Literal(self.delimiter)
        params["quote"] = sql.Literal(self.quote)
        params["escape"] = sql.Literal(self.escape)

        return sql.SQL(query).format(**params).as_string(self.connection)


    def copy_to(self):
        logger.info(f"copy table {self.schema}.{self.table} to file {self.filepath}")
        query = self.build_copy_to_query()
        
        
        with open(self.file, "wb" if self.truncate or self.recreate else "a") as bin_reader:
            with self.cursor.copy(query) as copy:
                for data in copy:
                    bin_reader.write(data)

        logger.info(f"{self.cursor.rowcount} rows copied")
        return self.cursor.rowcount


    def to_csv(self):
        self.prepare_csv_params()
        self.prepare_columns_from_table()
        self.prepare_file()
        self.copy_to()
