from distutils.core import setup
from setuptools import find_packages

setup(name='CatFacts',
      version='0.1',
      description='Presentation Cat Facts',
      author='Gianmaria Genetlici',
      author_email='gianmaria.genetlici@gmail.com',
      url='https://github.com/jean-n92/pytest-mock-presentation',
      packages=find_packages(),
      requires=['requirements.txt'],
      entry_points = {'console_scripts': ['cat-facts=neon.main:main']}
     )