#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='GoogleScholarMap',
      version='1.0',
      description='A collection of Python scripts to generate a map of countries from which a scholar is cited.',
      author='Pedro Sequeira',
      author_email='pedrodbs@gmail.com',
      url='',
      packages=find_packages(),
      scripts=[
      ],
      install_requires=[
          'email2country',
          'geopy',
          'geotext',
          'IP2Location',
          'jsonpickle',
          'pandas',
          'requests',
          'scholarly',
          'tqdm',
      ],
      extras_require={
      },
      zip_safe=True
      )
