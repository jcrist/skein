import alabaster
import skein

# Project settings
project = 'Skein'
copyright = '2018, Jim Crist'
author = 'Jim Crist'
release = version = skein.__version__

source_suffix = '.rst'
master_doc = 'index'
language = None
pygments_style = 'sphinx'
exclude_patterns = []

# Sphinx Extensions
extensions = ['sphinx.ext.autodoc', 'numpydoc', 'sphinxcontrib.autoprogram']

numpydoc_show_class_members = False

# Sphinx Theme
html_theme = 'alabaster'
html_theme_path = [alabaster.get_path()]
templates_path = ['_templates']
html_static_path = ['_static']
html_theme_options = {
    'description': 'Simple tool for deploying jobs on Apache YARN',
    'github_button': True,
    'github_count': False,
    'github_user': 'jcrist',
    'github_repo': 'skein',
    'travis_button': True,
    'show_powered_by': False,
    'page_width': '960px',
    'sidebar_width': '200px',
    'code_font_size': '0.8em'
}
html_sidebars = {
    '**': ['about.html',
           'navigation.html',
           'help.html',
           'searchbox.html']
}
