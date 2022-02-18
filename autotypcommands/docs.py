"""

"""
import itertools
import subprocess

from cldfbench_autotypcldf import Dataset


def map(p):
    args = [
        'parameters={}'.format(p['ID']),
        'pacific-centered',
        'padding-left=10',
        'padding-right=10',
        'padding-top=20',
        'padding-bottom=20',
        'width=12',
        'height=8',
        'markersize=15',
    ]
    return '![]({}.jpg?{}#cldfviz.map)'.format(p['Name'], '&'.join(args))


def run(args):
    ds = Dataset()
    cldf = ds.cldf_reader()

    for dataset, params in itertools.groupby(
        sorted(cldf['ParameterTable'], key=lambda r: r['dataset']),
        lambda r: r['dataset'],
    ):
        params = list(params)
        if params[0]['unitset']:
            continue
        if dataset == 'Alienability':
            continue
        args.log.info('{} ...'.format(dataset))
        md = ['# [](ContributionTable?__template__=property.md#cldf:{})'.format(dataset)]
        md.append('\n[](ContributionTable?__template__=property.md&property=Description#cldf:{})'.format(dataset))
        for p in params:
            md.append('\n## [](ParameterTable#cldf:{})'.format(p['ID']))
            if not p['multivalued'] and (p['typespec'] != '"json"'):
                md.append('\n' + map(p))
        md.append('')
        mdpath = ds.dir.joinpath('docs', 'templates', '{}.md'.format(dataset))
        mdpath.write_text('\n'.join(md), encoding='utf8')

        # Now call cldfbench cldfviz.text --text-file ... --output ... cldf/StructureDataset...
        subprocess.check_call([
            'cldfbench', 'cldfviz.text',
            '--text-file', str(mdpath),
            '--output', str(ds.dir.joinpath('docs', '{}'.format(dataset), 'README.md')),
            str(ds.cldf_dir / 'StructureDataset-metadata.json'),
        ])
        args.log.info('... done')
