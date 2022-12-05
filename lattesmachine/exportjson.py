import rocksdb
import json
import sys


def exportjson(db, out, skip_dup=False):
    out.write('{\n')
    first = True
    it = db.iteritems()
    it.seek_to_first()
    for key, value in it:
        if first:
            prefix = '  '
            first = False
        else:
            prefix = ', '
        if skip_dup:
            # pula itens que contenham a chave indicando que são duplicatas
            if json.loads(value).get('@@dup_of'):
                continue
        out.write(prefix + json.dumps(key.decode('utf-8'), ensure_ascii=False) +
                  ': ' + value.decode('utf-8') + '\n')
    out.write('}\n')
    out.flush()


def exportjson_cmd(db_path, skip_dup):
    db = rocksdb.DB(db_path, rocksdb.Options(), read_only=True)
    exportjson(db, sys.stdout, skip_dup)
    db.close()
