from setuptools import setup, find_packages

setup(
    name="simple-snowflake-mcp",
    version="0.2.0",
    description="Simple Snowflake MCP Server to work behind a corporate proxy",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Yann Barraud",
    author_email="yann@barraud.io",
    url="https://github.com/YannBrrd/simple_snowflake_mcp",
    license="MIT",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.9",
    install_requires=[
        "mcp>=1.10.1",
        "snowflake-connector-python",
        "python-dotenv",
        "pydantic"
    ],
    keywords=["snowflake", "mcp", "server", "proxy"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "console_scripts": [
            "simple-snowflake-mcp=simple_snowflake_mcp:main"
        ]
    },
)
