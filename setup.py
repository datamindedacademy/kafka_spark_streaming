#!/usr/bin/env python
from pathlib import Path

from setuptools import find_packages, setup

setup(
    name="Apache Kafka and Spark Structured Streaming",
    version="1.0.0",
    description='Code accompanying the course "Data Pipeline Part 2 at Data ScienceTech Institute"',
    long_description=(Path(__file__).parent / "README.md").read_text(),
    long_description_content_type="text/markdown",
    author="Ana Escobar",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3 :: Only",
    ],
    keywords="kafka and spark structured streaming training exercises",
    packages=find_packages(include=["exercises"]),
    python_requires=">=3.5, <4",
)
