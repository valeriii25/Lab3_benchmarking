from setuptools import setup, find_packages

setup(
    name='lab3-benchmarking',
    version='0.0.1',
    author='Valeria Lapshina',
    author_email='valapshina@edu.hse.ru',
    packages=find_packages(),
    install_requires=[
        'psycopg2',
        'sqlalchemy',
        'pandas',
        'duckdb'
    ],
)
