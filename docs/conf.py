import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

project = "dagster-ncsa"
author = "Ben Galewsky"
extensions = ["sphinx.ext.autodoc", "sphinx.ext.napoleon", "sphinx.ext.viewcode"]
templates_path = ["_templates"]
exclude_patterns = []
html_theme = "furo"
master_doc = "index"
