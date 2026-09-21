from setuptools import setup, find_packages

setup(
    name="tokenrelay",
    version="1.0.0",
    description="Token-based subagent communication protocol for Hermes delegate_task",
    packages=find_packages(),
    python_requires=">=3.10",
    entry_points={
        "console_scripts": [
            "tokenrelay=src.cli:main",
        ],
    },
)
