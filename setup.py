import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyhdbsql",
    version="0.0.6",
    author="Thorsten Hapke",
    author_email="thorsten.hapke@sap.com",
    description="Python based sql console",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/thhapke/pyhdbsql",
    keywords = ['hana, hdbcli'],
    #packages=setuptools.find_packages(),
    packages=["pyhdbsql"],
    install_requires=[
        'pyyaml','hdbcli'
    ],
    include_package_data=True,
    entry_points={
        'console_scripts': [
            "pyhdbsql = pyhdbsql.console:main"
        ]
    },
    classifiers=[
    	'Programming Language :: Python :: 3.6',
    	'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)

