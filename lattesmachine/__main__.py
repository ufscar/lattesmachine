import sys
import argparse
import logging
from .extract import extract_cmd
from .genforcelist import genforcelist_cmd
from .exportjson import exportjson_cmd
from .splititems import splititems_cmd
from .dedup import dedup_cmd


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='lattesmachine')
    subparsers = parser.add_subparsers()

    p = subparsers.add_parser('genforcelist')
    p.add_argument('--db_cv', type=str, default='./db-cv/')
    p.set_defaults(func=lambda args: genforcelist_cmd(args.db_cv))

    p = subparsers.add_parser('exportjson')
    p.add_argument('--db', type=str, default='./db-items/')
    p.set_defaults(skip_dup=False)
    p.add_argument('--skip_dup', action='store_true')
    p.set_defaults(func=lambda args: exportjson_cmd(args.db, args.skip_dup))

    p = subparsers.add_parser('extract')
    p.add_argument('--people', type=open, required=True)
    p.add_argument('--db_cv', type=str, default='./db-cv/')
    p.set_defaults(func=lambda args: extract_cmd(args.db_cv, args.people))

    p = subparsers.add_parser('splititems')
    p.add_argument('--db_cv', type=str, default='./db-cv/')
    p.add_argument('--db_items', type=str, default='./db-items/')
    p.add_argument('--from_year', type=int, default=1901)
    p.add_argument('--to_year', type=int, default=9999)
    p.set_defaults(func=lambda args: splititems_cmd(args.db_cv, args.db_items, args.from_year, args.to_year))

    p = subparsers.add_parser('dedup')
    p.add_argument('--db_items', type=str, default='./db-items/')
    p.set_defaults(func=lambda args: dedup_cmd(args.db_items))

    args = parser.parse_args(sys.argv[1:])

    logging.basicConfig(level=logging.INFO)
    args.func(args)
