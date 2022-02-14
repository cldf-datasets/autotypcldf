"""

"""
import itertools
import collections

from csvw.metadata import Datatype
from clldutils import jsonlib
from clldutils.path import walk

from cldfbench_autotypcldf import Dataset


def register(parser):
    parser.add_argument('dataset')


def make_obj(lid, vals, dt, multivalued, vnames, lang):
    res = {'LID': str(lid), 'Language': lang['Name'], 'Glottocode': lang['Glottocode']}
    for vid, values in itertools.groupby(vals, lambda v: v['Parameter_ID']):
        values = [v['Value'] for v in values]
        if multivalued[vid]:
            res[vnames[vid]] = [dt[vid].parse(v) for v in values]
        else:
            assert len(values) == 1, '{}:{}: {}'.format(vnames[vid], lid, values)
            try:
                res[vnames[vid]] = dt[vid].parse(values[0])
            except:
                print(values[0], type(values[0]), dt[vid], vnames[vid])
                raise
    return res


def normalize_json_obj(obj):
    res = {k: str(v) if k == 'LID' else v for k, v in obj.items() if v is not None}
    if set(res.keys()) != {'LID', 'Language', 'Glottocode'}:
        return res


def run(args):
    ds = Dataset()
    for jsonpath in walk(ds.raw_dir / 'autotyp-data' / 'data' / 'json'):
        if jsonpath.stem == args.dataset:
            break
    else:
        raise ValueError(args.dataset)
    jsondata = collections.defaultdict(list)
    for d in jsonlib.load(jsonpath):
        d = normalize_json_obj(d)
        if d and 'LID' in d:
            jsondata[str(d['LID'])].append(d)
    jdl = len(jsondata)

    cldf = ds.cldf_reader()
    # collect all variables for the dataset:
    variables = [p for p in cldf['ParameterTable'] if p['dataset'] == args.dataset]
    langs = {l['ID']: l for l in cldf['LanguageTable']}
    vids = set(p['ID'] for p in variables)
    datatypes = {v['ID']: Datatype.fromvalue(v['typespec']) for v in variables}
    multivalued = {v['ID']: v['multivalued'] for v in variables}
    unitset = len(variables) == 1 and variables[0]['unitset']
    vnames = {v['ID']: v['Name'] for v in variables}

    values = [v for v in cldf['ValueTable'] if v['Parameter_ID'] in vids]

    # group values by lid:
    roundtripped = 0
    for lid, vals in itertools.groupby(
        sorted(values, key=lambda v: (v['Language_ID'], v['Parameter_ID'])),
        lambda v: v['Language_ID'],
    ):
        if unitset:
            objs = []
            for v in vals:
                obj = make_obj(lid, [v], datatypes, multivalued, vnames, langs[lid])
                lid = obj.pop('LID')
                lang = obj.pop('Language')
                gc = obj.pop('Glottocode')
                assert len(obj) == 1 and variables[0]['Name'] in obj
                objs.append(dict(LID=lid, Language=lang, Glottocode=gc, **obj[variables[0]['Name']][0]))
            if len(jsondata[lid]) == len(objs):
                if all(o in jsondata[lid] for o in objs):
                    del jsondata[lid]
                    roundtripped += 1
                else:
                    print(objs)
                    print(jsondata[lid])
                    raise (ValueError)
            else:
                #print(objs)
                #print(jsondata[lid])
                raise(ValueError)
        else:
            jd = jsondata.pop(lid)
            obj = make_obj(lid, vals, datatypes, multivalued, vnames, langs[lid])
            assert obj == jd[0], '{} --- {}'.format(obj, jd[0])
            roundtripped += 1
    print('records for {} of {} LIDs roundtripped, missed {}'.format(roundtripped, jdl, jsondata.keys()))
    if not jsondata:
        args.log.info('OK')
