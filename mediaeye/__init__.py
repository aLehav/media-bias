"""
Antisemitism Project package
==============
This package includes modules for scraping, processing, and hate detection.
"""

__version__ = "1.0.0"

from .postgres import DBConn
from .newspaper_enricher import NewspaperEnricher
from .article_enricher import ArticleEnricher
