from setuptools import find_packages, setup

setup(
    name="civic_vote_scraper",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "beautifulsoup4>=4.12.3",
        "pdfplumber>=0.11.4",
        "requests>=2.32.3",
    ],
)
