from setuptools import find_packages, setup

setup(
    name="news_etl_analytics",
    packages=find_packages(exclude=["news_etl_analytics_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
