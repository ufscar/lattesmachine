import plyvel
import json
import sys


def exportjson(db, out):
    out.write('{\n')
    first = True
    for key, value in db:
        if first:
            prefix = '  '
            first = False
        else:
            prefix = ', '
        out.write(prefix + json.dumps(key.decode('utf-8')) +
                  ': ' + value.decode('utf-8'))
    out.write('}\n')


def exportjson_cmd(db_path):
    db = plyvel.DB(db_path)
    exportjson(db, sys.stdout)
    db.close()
