import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="common-modules",
    version="0.0.1",
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
