from setuptools import setup, find_packages
import subprocess
import os

def get_version():
    """Get base version (without environment suffix)"""
    return "0.1.0"

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="prediction-markets-agent",
    version=get_version(),
    author="BD_Harold",
    description="A Python pipeline for collecting and analyzing market data from Polymarket",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/vittorioapi91/PredictionMarketsAgent",
    packages=find_packages(),
    package_dir={"": "."},
    py_modules=[
        "src.main",
        "src.get_open_markets",
        "src.pipeline_all_poly",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "prediction-markets-agent=src.main:main",
            "get-open-markets=src.get_open_markets:main",
            "pipeline-all-poly=src.pipeline_all_poly:main",
        ],
    },
)
