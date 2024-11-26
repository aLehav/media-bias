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
        description='Scrape Wiki'
    ),
    'scrape_amcha': generate_checkbox(
        value=True,
        description='Scrape AMCHA'
    ),
    'manually_pair_amcha_to_wiki': generate_checkbox(
        value=False,
        description='Manually Pair AMCHA and Wiki Names'
    ),
    'gcs': generate_checkbox(
        value=True,
        description='Search Newspaper Link for Matched Schools'
    ),
    'manually_verify_links': generate_checkbox(
        value=False,
        description='Manually Verify Link/Name/Newspaper Matches'
    ),
    'set_wordpress_status': generate_checkbox(
        value=True,
        description='Check if newspaper sites are made with wordpress'
    ),
    'insert_article_links': generate_checkbox(
        value=False,
        description='Insert Article URLs'
    )
}

def widget_value(key):
    """Get the value of a given checkbox"""
    return local_widgets[key].value
