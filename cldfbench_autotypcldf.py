import collections
import re
import json
import pathlib
import itertools

from csvw.metadata import Datatype
import attr
import yaml
from clldutils import jsonlib
from clldutils.path import walk
from cldfbench import Dataset as BaseDataset, CLDFSpec

DTYPES = {
    'comment': 'string',
    'logical': 'boolean',
    'value-list': 'string',
    'table': 'json',
    'list-of<value-list>': 'string',
    'list-of<integer>': 'integer',
}


def data_path(mdpath):
    autotyp_data = mdpath.parent
    while autotyp_data.name != 'autotyp-data':
        autotyp_data = autotyp_data.parent
    assert autotyp_data.name == 'autotyp-data'
    base = autotyp_data / 'data' / 'json'
    if mdpath.stem != 'Register':
        dp = base / mdpath.parent.name / '{}.json'.format(mdpath.stem)
    else:
        dp = base / '{}.json'.format(mdpath.stem)
    assert dp.exists()
    return dp


class Counts:
    def __init__(self):
        self.cid = 0
        self.vid = 0

    def inc(self, att):
        setattr(self, att, getattr(self, att) + 1)
        return getattr(self, att)


@attr.s
class Parameter:
    id = attr.ib()
    name = attr.ib()
    vars = attr.ib()
    dataset = attr.ib()
    counts = attr.ib()
    unitset = attr.ib()
    data = attr.ib(default=None)
    md = attr.ib(default=attr.Factory(dict))
    code_map = attr.ib(default=attr.Factory(dict))
    dt = attr.ib(default=None)

    def __attrs_post_init__(self):
        self.md.update(self.vars[0])
        if self.unitset:
            self.md.update(self.dataset[1])
        else:
            self.md.update(self.dataset[1]['fields'][self.name])
        self.data = data_path(self.dataset[0])
        #
        # FIXME: adapt description for datatype == 'table'
        #
        if self.datatype == 'table':
            assert self.md['fields']
            self.dt = Datatype.fromvalue('json')
        else:
            self.dt = Datatype.fromvalue(DTYPES.get(self.datatype, self.datatype))

    @property
    def dataset_id(self):
        return self.md['dataset']

    @property
    def fields(self):
        return self.md['fields']

    @property
    def datatype(self):
        return self.md['data']

    @property
    def multivalued(self):
        return self.datatype == 'table' or self.datatype.startswith('list-of<')

    def iter_codes(self):
        if not self.unitset:
            if self.datatype in ['value-list', 'list-of<value-list>']:
                for code, desc in self.md['values'].items():
                    yield self.check_code(code, desc=desc)

    def _iter_values(self):
        objs = {}
        for obj in jsonlib.load(self.data):
            lid = obj.get('LID')
            if lid:
                if not self.unitset:
                    if lid in objs:
                        assert objs[lid] == obj
                        # drop duplicates
                        continue
                    objs[lid] = obj
                    v = obj[self.name]
                    if v is not None:
                        yield obj['LID'], v
                else:
                    yield obj['LID'], {
                        k: v for k, v in obj.items()
                        if k not in ['LID', 'Language', 'Glottocode'] and v != None}

    def check_code(self, value, desc=None):
        assert value not in self.code_map
        cid = self.counts.inc('cid')
        self.code_map[value] = str(cid)
        return dict(
            ID=str(cid),
            Name=value,
            Description=desc,
            Parameter_ID=str(self.id),
        )

    def iter_values(self):
        for lid, v in self._iter_values():
            values = []
            if self.unitset:
                values.append(v)
            else:
                if self.multivalued:
                    assert isinstance(v, list)
                    for vv in v:
                        if self.datatype == 'table':
                            nv = {}
                            for kkk, vvv in vv.items():
                                if vvv is not None:
                                    if 'values' in self.md['fields'][kkk]:
                                        if vvv not in self.md['fields'][kkk]['values']:
                                            # FIXME: do something here!
                                            pass
                                    nv[kkk] = vvv
                            values.append(nv)
                        elif self.datatype == 'list-of<integer>':
                            values.append(vv)
                        elif self.datatype == 'list-of<value-list>':
                            values.append(vv)
                else:
                    values.append(v)

            for value in values:
                yield dict(
                    ID=str(self.counts.inc('vid')),
                    Language_ID=lid,
                    Parameter_ID=self.id,
                    Value=self.dt.formatted(value),
                    Code_ID=self.code_map.get(value) if not (self.unitset or isinstance(value, dict)) else None,
                )


def iter_cols(md, fmap=None):
    fmap = fmap or {}
    for col, spec in md['fields'].items():
        if col not in fmap:
            csvwspec = {
                "name": col,
                "dc:description": spec['description'],
                "dc:format": spec['kind'],
                "datatype": DTYPES.get(spec['data']) or spec['data'],
            }
            if spec['data'] == 'value-list':
                if col not in ['OriginContinent']:
                    csvwspec['datatype'] = {
                        'base': 'string', 'format': '|'.join(re.escape(k) for k in spec['values'].keys())}
                    #csvwspec['separator'] = "; "
                    csvwspec['dc:description'] = "{}\n\n{}".format(
                        csvwspec['dc:description'],
                        '\n'.join('{}: {}'.format(k, v) for k, v in spec['values'].items())
                    )
            yield csvwspec


def fix_bib(s):
    n = []
    for line in s.split('\n'):
        if 'author' in line or ('editor' in line):
            line = line.replace(' / ', ' and ')
            line = line.replace(' & ', ' and ')
            line = line.replace('&', ' and ')
            line = line.replace('/', ' and ')
        n.append(line)
    s = '\n'.join(n)
    repls = {
        'Csató, Éva Ágnes, Isaksson, Bo': 'Csató, Éva Ágnes and Isaksson, Bo',
        'Aikhenvald, A., R.M.W.Dixon,': 'Aikhenvald, A. and R.M.W.Dixon,',
        'Rivai, F.S., Sorrentino, A.': 'Rivai, F.S. and Sorrentino, A.',
        'E. Ashton, E. M. K. Ostell, E. G. M. Mulira, Ndawula': 'E. Ashton, E. M. K. and Ostell, E. G. M. and Mulira, Ndawula',
        'Bickel, Balthasar, Martin Gaenszle, Arjun Rai, Prem D. Rai,  Shree K. Rai, Vishnu S. Rai, Narayan P. Sharma (Gautam)':
            'Bickel, Balthasar and Martin Gaenszle and Arjun Rai and Prem D. Rai and Shree K. Rai and Vishnu S. Rai and Narayan P. Sharma (Gautam)',
        'Zigmond, Maurice L. , Munro, Pamela': 'Zigmond, Maurice L. and Munro, Pamela',
        'Balthasar Bickel, Manoj Rai, Netra P. Paudyal, Goma Banjade, Toya N. Bhatta, Martin Gaenszle, Elena Lieven, Ichchha Purna Rai, Novel Kishore Rai,':
            'Balthasar Bickel and Manoj Rai and Netra P. Paudyal and Goma Banjade and Toya N. Bhatta and Martin Gaenszle and Elena Lieven and Ichchha Purna Rai and Novel Kishore Rai',
    }
    for k, v in repls.items():
        s = s.replace(k, v)
    return s


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "autotypcldf"

    def cldf_specs(self):  # A dataset must declare all CLDF sets it creates.
        return CLDFSpec(dir=self.cldf_dir, module='StructureDataset')

    def cmd_download(self, args):
        # FIXME: update git submodule?
        pass

    def cmd_makecldf(self, args):
        """
We can meaningfully only split datasets with no multiples per language into individual variables!
I.e. for the ones listed below, each autotyp "record" must be converted to **one**
composite JSON value.
        """
        unitsets = [
            # Morphology:
            'MorphemeClasses',
            'GrammaticalMarkers',
            'LocusOfMarkingPerMicrorelation',
            'VerbSynthesis',
            'DefaultLocusOfMarkingPerMacrorelation',
            # GrammaticalRelations
            'PredicateClasses',
            'GrammaticalRelations',
            'GrammaticalRelationsRaw',
            'Alignment',
            # PerLanguageSummaries
            'NPStructurePresence',
            # Word
            'WordDomains',
            # NP
            'NPStructure',
            # Sentence
            'ClauseLinkage',
        ]

        # read the bib!
        args.writer.cldf.sources.add(fix_bib(self.raw_dir.joinpath(
            'autotyp-data', 'bibliography', 'autotyp.bib').read_text(encoding='utf8')))
        l2src = collections.defaultdict(set)
        for src in args.writer.cldf.sources.items():
            for lid in src['LanguageID'].split(','):
                lid = lid.strip()
                if lid:
                    l2src[lid].add(src.id)

        counts = Counts()
        datasets = {
            p.stem: (p.resolve(), yaml.load(p.read_text(encoding='utf8'), yaml.CLoader))
            for p in walk(self.raw_dir / 'autotyp-data' / 'metadata', mode='files')
            if p.suffix == '.yaml' and p.parent.name != 'Definitions'}
        assert len(datasets) == 46

        parameters = []
        for i, ((ds, var), rows) in enumerate(itertools.groupby(
                self.raw_dir.joinpath('autotyp-data').read_csv('variables_overview.csv', dicts=True),
                lambda r: (r['dataset'], None if r['dataset'] in unitsets else r['variable'].split('$')[0]),
        ), start=1):
            if ds != 'Register' and (var in ['LID', 'Language', 'Glottocode']):
                continue
            rows = list(rows)
            assert ds in datasets
            datasets[ds][1]['dataset_kind'] = rows[0]['dataset_kind']
            parameters.append(Parameter(id=i, name=var or ds, vars=rows, dataset=datasets[ds], counts=counts, unitset=var is None))

        fmap = {
            'LID': 'ID',
            'Language': 'Name',
            'ISO639_3': 'ISO639P3code',
            'Glottocode': None,
            'Latitude': None,
            'Longitude': None,
        }
        t = args.writer.cldf.add_component(
            'LanguageTable',
            {"name": "Source", "separator": "; ", "propertyUrl": "http://cldf.clld.org/v1.0/terms.rdf#source"},
            *list(iter_cols(datasets['Register'][1], fmap)))
        t.common_props['dc:description'] = datasets['Register'][1]['description']
        args.writer.cldf.add_component('ContributionTable', 'dataset_kind')
        args.writer.cldf.add_component(
            'ParameterTable', 'module', 'submodule', 'kind', 'datatype',
            {"name": 'dim', "datatype": "integer"},
            {"name": "typespec", "datatype": "json"},
            {"name": "unitset", "datatype": "boolean"},
            {"name": "multivalued", "datatype": "boolean"},
            {"name": "dataset", "propertyUrl": "http://cldf.clld.org/v1.0/terms.rdf#contributionReference"}
        )
        args.writer.cldf.add_component('CodeTable')

        for name, (_, md) in datasets.items():
            args.writer.objects['ContributionTable'].append(dict(
                ID=name, Name=name, Description=md['description'], dataset_kind=md['dataset_kind'],
            ))

        for row in jsonlib.load(data_path(datasets['Register'][0])):
            obj = {
                fmap.get(k) or k: None if v == 'NA' else v for k, v in row.items()
            }
            obj['Source'] = l2src.pop(str(obj['ID']), [])
            args.writer.objects['LanguageTable'].append(obj)

        for p in [pp for pp in parameters if pp.dataset_id != 'Register']:
            desc = p.md['description']
            if p.datatype == 'table':
                # There can be nested tables now!
                #assert not any(vv['data'] == 'table' for vv in p.fields.values()), str(p.name)
                # FIXME: add field name, and value description
                desc = desc + '\n\n' + '\n'.join(vv['description'] for kk, vv in p.fields.items())
            args.writer.objects['ParameterTable'].append(dict(
                ID=str(p.id),
                Name=p.name,
                Description=desc,
                module=p.md['modules'],
                dataset=p.dataset_id,
                kind=p.md['kind'],
                datatype=p.md['data'],
                dim=len(p.fields) if p.datatype == 'table' else 1,
                typespec=DTYPES.get(p.datatype, p.datatype),
                multivalued=p.multivalued,
                unitset=p.unitset,
            ))
            #print(p.name, p.data)
            for code in p.iter_codes():
                args.writer.objects['CodeTable'].append(code)

            for val in p.iter_values():
                args.writer.objects['ValueTable'].append(val)