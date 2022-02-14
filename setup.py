from setuptools import setup


setup(
    name='cldfbench_autotypcldf',
    py_modules=['cldfbench_autotypcldf'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'cldfbench.dataset': [
            'autotypcldf=cldfbench_autotypcldf:Dataset',
        ],
        'cldfbench.commands': [
            'autotyp=autotypcommands',
        ]
    },
    install_requires=[
        'pyyaml',
        'cldfbench',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
