import os, psr.factory, PyInstaller.__main__

pkg_path = os.path.dirname(psr.factory.__file__)

PyInstaller.__main__.run([
    "--onefile",
    "--add-binary", f"{pkg_path};psr/factory",
    "main.py",
])