def jsonfilter(json, f, path=[]):
    if isinstance(json, dict):
        for k, v in json.items():
            p = path + [k]
            if isinstance(v, str):
                json[k] = f(p, v)
            else:
                jsonfilter(v, f, p)
    elif isinstance(json, list):
        for i, v in enumerate(json):
            p = path + [i]
            if isinstance(v, str):
                json[i] = f(p, v)
            else:
                jsonfilter(v, f, p)


def jsoniter(json, path=[]):
    if isinstance(json, dict):
        for k, v in json.items():
            yield from jsoniter(v, path + [k])
    elif isinstance(json, list):
        for i, v in enumerate(json):
            yield from jsoniter(v, path + [i])
    elif isinstance(json, str):
        yield (path, json)


def jsonvalue(json, path):
    for k in path:
        json = json[k]
    return json
