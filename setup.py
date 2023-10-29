# Defind dependencies for Dagster to install
from setuptools import find_packages, setup

setup(
    name="bitcoin-api",
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "matplotlib",
    ],
    extras_require={"dev": ["dagster-webserver"]},
)
