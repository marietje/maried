#!/usr/bin/env python

from setuptools import setup

setup(name='maried',
      version='0.1.0a1',
      description='MarieD music daemon',
      author='Bas Westerbaan',
      author_email='bas@westerbaan.name',
      url='http://github.com/bwesterb/maried/',
      packages=['maried'],
      zip_safe=False,
      package_dir={'maried': 'src'},
      install_requires = ['docutils>=0.3',
			  'mirte>=0.1.0a1',
			  'sarah>=0.1.0a1']
      )
