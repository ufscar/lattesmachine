import more_itertools
import requests
import rocksdb
import json
import os
from .jsonwalk import jsoniterkeys, jsonset
from . import settings
from collections import defaultdict
from atomicwrites import atomic_write
from retry import retry


scopus_cache = {}


@retry(tries=5, delay=10, backoff=2)
def scopus_query(issn):
    if issn not in scopus_cache:
        r = requests.get('https://api.elsevier.com/content/serial/title',
                         {'issn': issn,
                          'view': 'CITESCORE',
                          'apiKey': settings.scopus_api_key})
        r.raise_for_status()
        scopus_cache[issn] = r.json()
    return scopus_cache[issn]


def scopus_percentiles(issn):
    data = scopus_query(issn)
    area_code = {}
    entries = data.get('serial-metadata-response', {}).get('entry', [])
    for entry in entries:
        for area in entry.get('subject-area', []):
            area_code[area['@code']] = area['@abbrev']
    res = defaultdict(lambda: 0)
    for entry in entries:
        csyil = entry.get('citeScoreYearInfoList', {})
        year = csyil.get('citeScoreCurrentMetricYear')
        if not year:
            continue
        for csyi in csyil.get('citeScoreYearInfo', []):
            if csyi.get('@year') != year:
                continue
            for csi in csyi.get('citeScoreInformationList', []):
                for cs in csi.get('citeScoreInfo', []):
                    for cssr in cs.get('citeScoreSubjectRank', []):
                        area = area_code.get(cssr.get('subjectCode'))
                        if area:
                            res[area] = max(res[area], int(cssr['percentile']))
    return res


def scopus(items_db):
    it = items_db.iteritems()
    it.seek_to_first()
    for batch in more_itertools.chunked(it, settings.item_batch_size):
        wb = rocksdb.WriteBatch()
        for item_key, item in batch:
            item = json.loads(item)
            for p, v in jsoniterkeys(item, {'@ISSN', }):
                percentiles = scopus_percentiles(v)
                if percentiles:
                    jsonset(item, p[:-1] + ['@@scopus'], percentiles)
                    wb.put(item_key, json.dumps(item).encode('utf-8'))
        items_db.write(wb)


def scopus_cmd(items_db_path, scopus_cache_path):
    if scopus_cache_path and os.path.exists(scopus_cache_path):
        with open(scopus_cache_path, 'r') as f:
            scopus_cache.update(json.load(f))
    items_db = rocksdb.DB(items_db_path, rocksdb.Options(compression=rocksdb.CompressionType.lz4_compression))
    scopus(items_db)
    if scopus_cache_path:
        with atomic_write(scopus_cache_path, overwrite=True) as f:
            json.dump(scopus_cache, f)
    items_db.close()
