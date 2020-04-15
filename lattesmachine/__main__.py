import sys
import argparse
import logging
from .extract import extract_cmd
from .genforcelist import genforcelist_cmd
from .exportjson import exportjson_cmd


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='lattesmachine')
    subparsers = parser.add_subparsers()

    parser_extract = subparsers.add_parser('extract')
    parser_extract.add_argument('--people', type=open, required=True)
    parser_extract.add_argument('--db_cv', type=str, default='./db-cv/')
    parser_extract.set_defaults(func=lambda args: extract_cmd(args.db_cv, args.people))

    parser_extract = subparsers.add_parser('genforcelist')
    parser_extract.add_argument('--db_cv', type=str, default='./db-cv/')
    parser_extract.set_defaults(func=lambda args: genforcelist_cmd(args.db_cv))

    parser_extract = subparsers.add_parser('exportjson')
    parser_extract.add_argument('--db_cv', type=str, default='./db-cv/')
    parser_extract.set_defaults(func=lambda args: exportjson_cmd(args.db_cv))

    args = parser.parse_args(sys.argv[1:])

    logging.basicConfig(level=logging.INFO)
    args.func(args)
