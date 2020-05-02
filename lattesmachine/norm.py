import unicodedata
import re


def uniq_spaces(s):
    return re.sub(r'\s+', ' ', s)


def no_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', uniq_spaces(s))
                   if unicodedata.category(c) != 'Mn')\
        .strip()\
        .lower()\
        .encode('ascii', 'ignore').decode('ascii')


def no_punct(s):
    return re.sub(r'[^a-z\d\s]', '', no_accents(s))


def letters_spaces(s):
    return re.sub(r'[^a-z\s]', '', no_accents(s))


def letters_no_spaces(s):
    return re.sub(r'[^a-z]', '', no_accents(s))
