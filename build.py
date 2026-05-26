import os, psr.factory, PyInstaller.__main__

pkg_path = os.path.dirname(psr.factory.__file__)

excludes = [
    "matplotlib",
    "scipy",
    "PIL",
    "Pillow",
    "IPython",
    "jupyter",
    "jupyter_client",
    "jupyter_core",
    "notebook",
    "nbconvert",
    "nbformat",
    "pytest",
    "sphinx",
    "docutils",
    "setuptools",
    "distutils",
    "lib2to3",
    "test",
    "unittest",
    "xmlrpc",
    "pdb",
    "sqlite3",
    "curses",
    "traitlets",
    "psutil",
    "fsspec",
    "fastparquet",
]

args = [
    "--onefile",
    "--add-binary", f"{pkg_path};psr/factory",
]

for mod in excludes:
    args += ["--exclude-module", mod]

args.append("main.py")

PyInstaller.__main__.run(args)