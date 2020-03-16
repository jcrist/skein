import alabaster
import skein

# Project settings
project = 'Skein'
copyright = '2018, Jim Crist-Harif'
author = 'Jim Crist-Harif'
release = version = skein.__version__

source_suffix = '.rst'
master_doc = 'index'
language = None
pygments_style = 'sphinx'
exclude_patterns = []

# Sphinx Extensions
extensions = ['sphinx.ext.autodoc', 'sphinx.ext.extlinks', 'numpydoc',
              'sphinxcontrib.autoprogram']

numpydoc_show_class_members = False

extlinks = {
    'issue': ('https://github.com/jcrist/skein/issues/%s', 'Issue #'),
    'pr': ('https://github.com/jcrist/skein/pull/%s', 'PR #')
}

# Sphinx Theme
html_theme = 'alabaster'
html_theme_path = [alabaster.get_path()]
templates_path = ['_templates']
html_static_path = ['_static']
html_favicon = '_static/favicon.ico'
html_theme_options = {
    'logo': 'logo.svg',
    'description': 'A tool and library for easily deploying applications on Apache YARN',
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
