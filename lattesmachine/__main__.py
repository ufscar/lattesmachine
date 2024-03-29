import sys
import argparse
import logging
from .genforcelist import genforcelist_cmd
from .exportjson import exportjson_cmd
from .extract import extract_cmd
from .getids import getids_cmd
from .splititems import splititems_cmd
from .dedup import dedup_cmd
from .scopus import scopus_cmd


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='lattesmachine')
    subparsers = parser.add_subparsers()

    p = subparsers.add_parser('genforcelist')
    p.add_argument('--db_cv', type=str, default='db-cv')
    p.set_defaults(func=lambda args: genforcelist_cmd(args.db_cv))

    p = subparsers.add_parser('exportjson')
    p.add_argument('--db', type=str, default='db-items')
    p.set_defaults(skip_dup=False)
    p.add_argument('--skip_dup', action='store_true')
    p.set_defaults(func=lambda args: exportjson_cmd(args.db, args.skip_dup))

    p = subparsers.add_parser('extract')
    p.add_argument('--people', type=open, required=True)
    p.add_argument('--db_cv', type=str, default='db-cv')
    p.set_defaults(func=lambda args: extract_cmd(args.db_cv, args.people))

    p = subparsers.add_parser('getids')
    p.add_argument('--db_cv', type=str, default='db-cv')
    p.add_argument('--kind', type=str, choices=['resolved', 'unresolved'], default='resolved')
    p.set_defaults(func=lambda args: getids_cmd(args.db_cv, args.kind))

    p = subparsers.add_parser('splititems')
    p.add_argument('--db_cv', type=str, default='db-cv')
    p.add_argument('--db_items', type=str, default='db-items')
    p.add_argument('--from_year', type=int, default=1901)
    p.add_argument('--to_year', type=int, default=9999)
    p.set_defaults(func=lambda args: splititems_cmd(args.db_cv, args.db_items, args.from_year, args.to_year))

    p = subparsers.add_parser('dedup')
    p.add_argument('--db_items', type=str, default='db-items')
    p.set_defaults(ignore_year=False)
    p.add_argument('--ignore_year', action='store_true')
    p.set_defaults(func=lambda args: dedup_cmd(args.db_items, ignore_year=args.ignore_year))

    p = subparsers.add_parser('scopus')
    p.add_argument('--db_items', type=str, default='db-items')
    p.add_argument('--cache', type=str, default='.cache.scopus.json')
    p.set_defaults(func=lambda args: scopus_cmd(args.db_items, args.cache))

    args = parser.parse_args(sys.argv[1:])

    logging.basicConfig(level=logging.INFO)
    args.func(args)
