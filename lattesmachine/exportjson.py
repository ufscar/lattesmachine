import plyvel
import json
import sys


def exportjson(db, out, skip_dup=False):
    out.write('{\n')
    first = True
    for key, value in db:
        if first:
            prefix = '  '
            first = False
        else:
            prefix = ', '
        if skip_dup:
            # pula itens que contenham a chave indicando que são duplicatas
            if json.loads(value).get('@@dup_of'):
                continue
        out.write(prefix + json.dumps(key.decode('utf-8')) +
                  ': ' + value.decode('utf-8') + '\n')
    out.write('}\n')


def exportjson_cmd(db_path, skip_dup):
    db = plyvel.DB(db_path)
    exportjson(db, sys.stdout, skip_dup)
    db.close()
