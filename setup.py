from setuptools import setup, find_packages
from os.path import join
from os import path


this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

about = {}
with open(join(this_directory, 'piperoni', '__version__.py'), 'r') as f:
    exec(f.read(), about)


setup(
    name="piperoni",
    # Update this in piperoni/__version__.py
    version=about['__version__'],
    url='http://github.com/CitrineInformatics/piperoni',
    description="Citrine Informatics ETL pipeline",
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Citrine Informatics',
    packages=find_packages(),
    install_requires=[
        "pyyaml<6,>=5.4",
        "pandas>=1.2,<2",
        "xlrd==1.2.0",
        "dagre-py==0.1.6"
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
)
