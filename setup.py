from setuptools import setup

setup(
    name='CorpusForge',
    version='0.0.1',    
    description='Corpus enrichment, processing, and formatting in Python',
    url='https://github.com/{my_github}',
    author='Samuel Hollands',
    author_email='shollands1@sheffield.ac.uk',
    license='GNU GENERAL PUBLIC LICENSE',
    packages=['CorpusForge'],
    install_requires=['spacy',
                      'pymusas',                     
                      'https://github.com/UCREL/pymusas-models/releases/download/en_dual_none_contextual-0.3.3/en_dual_none_contextual-0.3.3-py3-none-any.whl',
                      'humanize',
                      'numpy',
                      'pandas',
                      'openpyxl',
                      'tqdm',
                      'bs4',
                      'lxml',
                      'scikit-learn'],

    classifiers=[
        'Development Status :: 2 - Testing',
        'Intended Audience :: Science/Research',
        'License :: GNU GENERAL PUBLIC LICENSE',  
        'Operating System :: MacOS :: Linux',        
        'Programming Language :: Python :: 3.9',
    ],
)