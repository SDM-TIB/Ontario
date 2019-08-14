#!/usr/bin/env python3

from distutils.core import setup
from setuptools import find_packages

setup(name='ontario',
      version='0.3',
      description='Ontario - Ontology-based architecture for semantic data lakes',
      author='Kemele M. Endris',
      author_email='kemele.endris@gmail.com',
      url='https://github.com/SDM-TIB/Ontario',
      scripts=['scripts/runExperiment.py', 'run_query.py', 'scripts/runOntarioExp.sh', 'scripts/run_dief_experiment.py'],
      packages=find_packages(exclude=['docs']),
      install_requires=["ply",
                        "flask",
                        "requests",
                        'hdfs',
                        'pyspark',
                        'pymongo',
                        'mysql-connector-python',
                        'neo4j-driver',
                        'rdflib',
                        'networkx'],
      include_package_data=True,
      license='GNU/GPL v2'
      )
