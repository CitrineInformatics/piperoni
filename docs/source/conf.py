# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
from datetime import datetime
from piperoni.__version__ import __version__
today = datetime.today()

# sys.path.insert(0, os.path.abspath("../.."))
# this_directory = path.abspath(path.dirname(__file__))

# about = {}
# with open(join(this_directory, '..', '..', 'piperoni', '__version__.py'), 'r') as f:
#     exec(f.read(), about)

# -- Project information -----------------------------------------------------

project = "Piperoni"
copyright = "{}, Citrine Informatics".format(str(today.year))
author = "Lenore Kubie, Emre Sevgen, Ventura Rivera, Maxwell Dylla, Xavi Lynn"

# The full version, including alpha/beta/rc tags
release = __version__


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ["sphinx.ext.autodoc", "sphinx.ext.coverage", "numpydoc"]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "alabaster"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

autodoc_mock_imports = (
    []
)  # autodoc_mock_imports allows Spyinx to ignore any external modules listed in the array

html_favicon = "_static/favicon.png"
html_theme_options = {
    "logo": "logo.png",
    "github_user": "CitrineInformatics",
    "github_repo": "piperoni",
    "sidebar_collapse": False,
    "head_font_family": ["Barlow", "Helvetica", "Arial", "Sans-Serif"],
    "font_family": ["Lusitana", "Times New Roman", "serif"],
    "page_width": "1024px",  # default is 940 https://github.com/bitprophet/alabaster/blob/master/alabaster/theme.conf#L29
    "sidebar_width": "250px",  # default is 220 https://github.com/bitprophet/alabaster/blob/master/alabaster/theme.conf#L38
}
