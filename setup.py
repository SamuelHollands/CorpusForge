from setuptools import setup
from setuptools.command.install import install
from setuptools.command.develop import develop

class InstallCommand(install):
    def run(self):
        # Run the default installation first
        install.run(self)

        # Download the Spacy model during the installation
        self.execute(_post_install, (self.install_lib,),
                     msg="Running post-install task")

class DevelopCommand(develop):
    def run(self):
        # Run the default develop command first
        develop.run(self)

        # Download the Spacy model during the development installation
        self.execute(_post_install, (self.install_lib,),
                     msg="Running post-install task")

def _post_install(install_lib):
    import spacy
    spacy.cli.download("en_core_web_trf")

setup(
    name='CorpusForge',
    version='0.0.2',    
    description='Corpus enrichment, processing, and formatting in Python',
    url='https://github.com/hannaschmueck/CorpusForge',
    author='Samuel Hollands, Hanna Schmueck',
    author_email='shollands1@sheffield.ac.uk, h.schmueck@lancaster.ac.uk',
    license='GNU GENERAL PUBLIC LICENSE',
    packages=['CorpusForge'],
    install_requires=['spacy',
                      'pymusas',                     
                      'en_dual_none_contextual @ git+ssh://git@github.com/UCREL/pymusas-models/releases/download/en_dual_none_contextual-0.3.3/en_dual_none_contextual-0.3.3-py3-none-any.whl',
                      'humanize',
                      'numpy',
                      'pandas',
                      'openpyxl',
                      'tqdm',
                      'bs4',
                      'lxml',
                      'scikit-learn'],
    setup_requires=[
        'spacy',
    ],
    cmdclass={
        'install': InstallCommand,
        'develop': DevelopCommand,
    },

    classifiers=[
        'Development Status :: 2 - Testing',
        'Intended Audience :: Science/Research',
        'License :: GNU GENERAL PUBLIC LICENSE',  
        'Operating System :: MacOS :: Linux',        
        'Programming Language :: Python :: 3.9',
    ],
)