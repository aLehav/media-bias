"""Module for checkboxs that control running specific portins of the notebook."""
import ipywidgets as widgets

def generate_checkbox(value, description):
    """Make a checkbox"""
    return widgets.Checkbox(
        value=value,
        description=description,
        disabled=False, indent=False
    )

checkboxes = {
    'purge_db': generate_checkbox(
        value=False,
        description='Purge DB (DANGEROUS)'
    ),
    'create_db': generate_checkbox(
        value=False,
        description='Create DB (DANGEROUS)'
    ),
    'wiki_spider': generate_checkbox(
        value=False,
        description='Scrape School and Newspaper Names'
    ),
    'amcha_uni_spider': generate_checkbox(
        value=False,
        description='Scrape and Verify School Name from Amcha'
    ),
    'amcha_incident_spider': generate_checkbox(
        value=False,
        description='Scrape Amcha Incidents'
    ),
    'gcs_runner': generate_checkbox(
        value=True,
        description='Run GCS to Get Newspaper Candidate Links'
    ),
}

def checkbox_value(key):
    """Get the value of a given checkbox"""
    return checkboxes[key].value
