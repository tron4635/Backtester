from setuptools import setup
from Cython.Build import cythonize

setup(
    name='backtester',
    ext_modules=cythonize("backtester.pyx"),
    zip_safe=False,
)
