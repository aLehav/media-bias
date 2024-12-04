"""Module containing scrapy Items for pipelines and spiders to use."""

from scrapy.item import Item, Field

class WikiItem(Item):
    """Class for wikipedia student newspaper entries"""

    school_name = Field()
    newspaper_name = Field()
    link = Field()

class AmchaUniItem(Item):
    """Class for amcha entries on the university tab"""

    name = Field()
    link = Field()

class IncidentItem(Item):
    """
    Class to take amcha incidents from requests and forward them through pipelines to be inserted 
    to the incidents DB
    """

    amcha_web_id = Field()
    origin_link = Field()
    raw_fields = Field()
    
class ArticleItem(Item):
    """Class for aritlce items"""

    id = Field()
    link = Field()
    content = Field()
    processed_article = Field()
    processing_method = Field()
    author = Field()
    title = Field()
    date_written = Field()
