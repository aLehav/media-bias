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
    'scraping_step': widgets.RadioButtons(
        options=['None',
                 'Wikipedia College Newspaper Scraping',
                 'Amcha University Name Scraping',
                 'Amcha Incident Scraping'],
        value='None',
        description='Revise a portion of the database.',
        disabled=False, indent=False
    ),
    'gcs_runner': generate_checkbox(
        value=False,
        description='Run GCS to Get Newspaper Candidate Links'
    ),
}

def widget_value(key):
    """Get the value of a given checkbox"""
    return local_widgets[key].value
