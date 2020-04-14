import sys
import argparse
import logging
from .extract import extract_cmd


def extract_handler(args):
    extract_cmd(args.db_cv, args.people)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(prog='lattesmachine')
    subparsers = parser.add_subparsers()

    parser_extract = subparsers.add_parser('extract')
    parser_extract.add_argument('--people', type=open, required=True)
    parser_extract.add_argument('--db_cv', type=str, default='./db-cv/')
    parser_extract.set_defaults(func=extract_handler)

    args = parser.parse_args(sys.argv[1:])

    logging.basicConfig(level=logging.INFO)
    args.func(args)
