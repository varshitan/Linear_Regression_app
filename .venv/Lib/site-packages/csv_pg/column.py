from __future__ import annotations
import re
from enum import Enum
from typing import Callable

TIMESTAMP_DEFAULT_PRECISION = 6

DEFAULT_SEQ = "__seq__"
DEFAULT_FILENAME = "'__filename__'"
DEFAULT_FILEPATH = "'__filepath__'"

class DataType(Enum):
    TEXT = "text"
    VARCHAR = "varchar"
    CHAR = "char"
    INT = "int"
    BIGINT  = "bigint"
    SMALLINT = "smallint"
    DECIMAL = "decimal"
    BOOL = "bool"
    DATE = "date"
    TIMESTAMPTZ = "timestamptz" # with timezone
    TIMESTAMP = "timestamp" # without timezone - if timezone is provided in the CSV file, it is ignored

    @property
    def is_str(self):
        return self in [self.CHAR, self.VARCHAR, self.TEXT]

class Column:
    def __init__(self, name: str, datatype: DataType|str = None, precision: int = None, scale: int = None, notnull: bool = False, primarykey: bool = False, index: bool = False, default: str = None, slug_func: Callable = None):
        self.name = name
        self.slug: str = slug_func(self.name) if slug_func else self.name

        # Parse datatype
        if datatype is None:
            self.datatype = None
        elif isinstance(datatype, DataType):
            self.datatype = datatype
        else:
            try:
                self.datatype = DataType(datatype)
            except ValueError:
                # Aliases
                if datatype == "character varying":
                    self.datatype = DataType.VARCHAR
                elif datatype == "character":
                    self.datatype = DataType.CHAR
                elif datatype == "integer":
                    self.datatype = DataType.INT
                elif datatype == "boolean":
                    self.datatype = DataType.BOOL
                elif datatype == "numeric":
                    self.datatype = DataType.DECIMAL
                elif datatype in ["timestamp with time zone", "datetimetz"]:
                    self.datatype = DataType.TIMESTAMPTZ
                elif datatype in ["timestamp without time zone", "datetime"]:
                    self.datatype = DataType.TIMESTAMP
                else:
                    raise NotImplementedError(f"data type \"{datatype}\" (column {name})")

        if self.datatype == DataType.DECIMAL:
            self.precision = precision
            self.scale = scale
        elif self.datatype in [DataType.CHAR, DataType.VARCHAR, DataType.TIMESTAMP, DataType.TIMESTAMPTZ]:
            self.precision = precision
            self.scale = None
        else:
            self.precision = None
            self.scale = None
    
        self.notnull = notnull
        self.primarykey = primarykey
        self.index = index

        if default:
            if default.startswith("nextval("):
                self.default = DEFAULT_SEQ
            elif default == DEFAULT_FILENAME or default.startswith(DEFAULT_FILENAME + "::"):
                self.default = DEFAULT_FILENAME
            elif default == DEFAULT_FILEPATH or default.startswith(DEFAULT_FILEPATH + "::"):
                self.default = DEFAULT_FILEPATH
            else:
                self.default = default
        else:
            self.default = None

        # Specific fixes
        if self.datatype in [DataType.TIMESTAMP, DataType.TIMESTAMPTZ] and self.precision == TIMESTAMP_DEFAULT_PRECISION:
            self.precision = None

        if self.primarykey or self.default:
            self.notnull = True


    def __str__(self):
        result = f"{self.name}"

        if self.datatype:
            result += f":{self.datatype.value}"

        if isinstance(self.precision, int) and isinstance(self.scale, int):
            result += f"({self.precision},{self.scale})"
        elif isinstance(self.precision, int):
            result += f"({self.precision})"
        
        if self.primarykey:
            result += "-pk"

        if self.default == DEFAULT_SEQ:
            result += "-seq"
        elif self.default == DEFAULT_FILENAME:
            result += "-filename"
        elif self.default == DEFAULT_FILEPATH:
            result += "-filepath"
        elif self.default:
            result += f"-default({self.default})"

        if self.notnull and not (self.primarykey or self.default):
            result += "-notnull"

        if self.index:
            result += "-ix"
        
        return result


    @classmethod
    def from_spec(cls, spec: str, slug_func: Callable = None) -> Column:
        def split_options(options: str):
            parsed_options = []
            in_parenthesis = 0
            in_string = False
            accu = ""
            for c in options:
                if c == '-':
                    if in_parenthesis:
                        accu += c
                    else:
                        accu = accu.strip()
                        if accu:
                            parsed_options.append(accu)
                        accu = ""
                else:
                    accu += c
                    if c == '(' and not in_string:
                        in_parenthesis += 1
                    elif c == ')' and not in_string:
                        if in_parenthesis == 0:
                            raise ValueError(f"syntax error: cannot close parenthis in \"{accu}\"")
                        in_parenthesis -= 1
                    elif c == "'" and in_parenthesis:
                        in_string = not in_string

            accu = accu.strip()
            if accu:
                parsed_options.append(accu)

            return parsed_options

        m = re.match(r"^(?P<name>[^\:]+)(?:\:(?P<options>.+))?$", spec)
        if not m:
            raise ValueError(f"invalid column spec: {spec}")
        
        name = m.group("name").strip()

        datatype = DataType.TEXT
        precision = None
        scale = None
        notnull = False
        primarykey = False
        index = False
        default = None

        options = m.group("options")
        if options:
            for option in split_options(options):
                if option == "notnull":
                    notnull = True
                elif option == "pk":
                    primarykey = True
                elif option == "ix":
                    index = True
                elif option == "seq":
                    default = DEFAULT_SEQ
                elif option == "filename":
                    default = DEFAULT_FILENAME
                elif option == "filepath":
                    default = DEFAULT_FILEPATH
                elif m := re.match(r"^default\((.+)\)$", option):
                    default = m.group(1)
                elif m := re.match(r"^([a-z]+)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$", option):
                    datatype = m.group(1)
                    precision = int(m.group(2))
                    scale = int(m.group(3))
                elif m := re.match(r"^([a-z]+)\s*\(\s*(\d+)\s*\)$", option):
                    datatype = m.group(1)
                    precision = int(m.group(2))
                else:
                    datatype = option

        return Column(name=name, datatype=datatype, precision=precision, scale=scale, notnull=notnull, primarykey=primarykey, index=index, default=default, slug_func=slug_func)


    def merge(self, other: Column):
        if other.datatype is not None:
            self.datatype = other.datatype
        if isinstance(other.precision, int):
            self.precision = other.precision
        if isinstance(other.scale, int):
            self.scale = other.scale
        if other.notnull:
            self.notnull = other.notnull
        if other.primarykey:
            self.primarykey = other.primarykey
        if other.index:
            self.index = other.index
