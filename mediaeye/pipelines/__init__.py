"""Scrapy pipelines"""

from .wiki_pipeline import WikiPipeline
from .amcha_pipelines import AmchaIncidentPipeline, AmchaUniPipeline
from .article_pipelines import ArticlePipeline, ArticleInsertPipeline
