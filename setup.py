from setuptools import setup, find_packages

setup(
    name="tecno_etl",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pandas>=1.5.0",
        "numpy>=1.22.0",
        "openpyxl>=3.1.0",
        "pyspark>=3.4.0",
        "pytest>=7.0.0",
    ],
    python_requires=">=3.9",
    author="Franco",
    author_email="franco18min@github.com",
    description="Pipeline ETL para datos de TecnoMundo",
)