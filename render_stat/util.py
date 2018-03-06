import os
import sys
import json


def venv_to_syspath():
    paths = read_json('path.json')
    for each in ['/Lib', '/Lib/site-packages', '/DLL']:
        tmp = paths['venv'] + each
        if tmp not in sys.path:
            sys.path.insert(0, paths['venv'] + each)


def read_json(json_file):
    # Get path to JSON file
    # Note !! All JSON files must be under 'json' directory
    base_path = os.path.dirname(__file__)
    json_path = os.path.join(base_path, 'json', json_file)
    if os.path.isfile(json_path):
        return json.load(open(json_path))
    else:
        raise IOError('{} does not exists.\n Full path = {}'.format(json_file, json_path))


def get_pgtokens():
    return read_json('pg.json')


