import sys
from dotenv import load_dotenv
from argparse import ArgumentParser
from zut import configure_logging, exec_command  
from . import from_csv, to_csv
from .base import VALID_DATEFORMATS

load_dotenv()
configure_logging()

def add_common_options(subparser):
    # CSV file options
    subparser.add_argument("--encoding")

    # List of columns
    subparser.add_argument("--columns", "-c", nargs="*")
    subparser.add_argument("--where", "-w")
    subparser.add_argument("--noslug", action="store_true")

    # Options to deal with existing table/file
    subparser.add_argument("--truncate", action="store_true")
    subparser.add_argument("--recreate", action="store_true")

    # CSV options
    subparser.add_argument("--noheader", action="store_true")
    subparser.add_argument("--dialect")
    subparser.add_argument("--delimiter")
    subparser.add_argument("--quote")
    subparser.add_argument("--escape")

    # Format options
    subparser.add_argument("--dateformat", choices=VALID_DATEFORMATS)

parser = ArgumentParser()

# Connection options (global)
parser.add_argument("--settings")
parser.add_argument("--using")
parser.add_argument("--dbname", "-d")
parser.add_argument("--host", "-H")
parser.add_argument("--port", "-p")
parser.add_argument("--user", "-U")
parser.add_argument("--password", "-W")

subparsers = parser.add_subparsers()

# Define "from" command
subparser = subparsers.add_parser("from_csv")
subparser.set_defaults(func=from_csv)
subparser.add_argument("file")
subparser.add_argument("table", nargs="?")
add_common_options(subparser)

# Define "to" command
subparser = subparsers.add_parser("to_csv")
subparser.set_defaults(func=to_csv)
subparser.add_argument("file")
subparser.add_argument("table", nargs="?")
add_common_options(subparser)

# Execute command
exec_command(parser)
