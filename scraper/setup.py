from setuptools import find_packages, setup

setup(
    name="civic_vote_scraper",
    version="0.1.0",
    packages=find_packages(),
    package_data={
        "fppc700extract": ["layouts/*.json"],
    },
    install_requires=[
        "beautifulsoup4>=4.12.3",
        "pdfplumber>=0.11.4",
        "playwright>=1.0",
        "PyQt5>=5.15",
        "pypdf>=5.0",
        "requests>=2.32.3",
    ],
)
