import re


def jsoniter(obj, _path=[]):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from jsoniter(v, _path + [k])
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from jsoniter(v, _path + [i])
    elif isinstance(obj, str):
        yield (_path, obj)


def jsoniterkeys(obj, keys, _path=[]):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in keys:
                yield (_path + [k], v)
            else:
                yield from jsoniterkeys(v, keys, _path + [k])
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from jsoniterkeys(v, keys, _path + [i])


def jsonget(obj, path):
    for k in path:
        obj = obj[k]
    return obj


def jsonset(obj, path, value):
    for k in path[:-1]:
        obj = obj[k]
    obj[path[-1]] = value


def renkey(pattern, repl, dic):
    for k in dic.keys():
        if re.match(pattern, k):
            v = dic.pop(k)
            if v:
                dic[repl] = v
            return v

