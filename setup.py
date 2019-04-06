import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setuptools.setup(
    name="decimex",
    version="0.0.1",
    install_requires=requirements,
    author="Michael Zion",
    author_email="noisleahcim@gmail.com",
    description="Decimex's common modules",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/decimex/common-modules",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
