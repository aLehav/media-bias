"""Module for checkboxs that control running specific portins of the notebook."""
import ipywidgets as widgets

def generate_checkbox(value, description):
    """Make a checkbox"""
    return widgets.Checkbox(
        value=value,
        description=description,
        disabled=False, indent=False
    )

local_widgets = {
    'purge_db': generate_checkbox(
        value=False,
        description='Purge DB (DANGEROUS)'
    ),
    'create_db': generate_checkbox(
        value=False,
        description='Create DB (DANGEROUS)'
    ),
    'scrape_wiki': generate_checkbox(
        value=True,
        description='Scrape wiki'
    ),
    'scrape_amcha': generate_checkbox(
        value=True,
        description='Scrape AMCHA'
    ),
    'manually_pair_amcha_to_wiki': generate_checkbox(
        value=False,
        description='Manually pair AMCHA and wiki names'
    ),
    'gcs': generate_checkbox(
        value=True,
        description='Search newspaper link for matched schools'
    ),
    'manually_verify_links': generate_checkbox(
        value=False,
        description='Manually verify link/name/newspaper matches'
    ),
    'set_wordpress_status': generate_checkbox(
        value=True,
        description='Check if newspaper sites are made with wordpress'
    ),
    'insert_article_links': generate_checkbox(
        value=False,
        description='Insert article urls'
    ),
    'set_filter_status': generate_checkbox(
        value=False,
        description='Filter article urls to label likely links'
    ),
}

def widget_value(key):
    """Get the value of a given checkbox"""
    return local_widgets[key].value
