from setuptools import setup

setup(
    name="ulltr",
    version="1.0.0",
    description="ULLTR: Ultra-Low Latency Direct-Redis Unix Domain Socket Client",
    py_modules=["market_data_client"],
    install_requires=[
        "redis",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)
