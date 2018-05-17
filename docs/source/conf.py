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
extensions = ['sphinx.ext.autodoc', 'sphinx.ext.intersphinx', 'numpydoc',
              'sphinxcontrib.autoprogram']

numpydoc_show_class_members = False

intersphinx_mapping = {
    'dask': ('http://dask.pydata.org/en/latest/',
             'http://dask.pydata.org/en/latest/objects.inv'),
    'distributed': ('http://distributed.readthedocs.io/en/latest/',
                    'http://distributed.readthedocs.io/en/latest/objects.inv')}


# Sphinx Theme
html_theme = 'alabaster'
html_theme_path = [alabaster.get_path()]
templates_path = ['_templates']
html_static_path = []
html_theme_options = {
    'description': 'Simple tool for deploying jobs on Apache YARN',
    'github_button': True,
    'github_count': False,
    'github_user': 'jcrist',
    'github_repo': 'skein',
    'travis_button': True,
    'show_powered_by': False
}
html_sidebars = {
    '**': ['about.html',
           'navigation.html',
           'help.html',
           'searchbox.html']
}
