from scrapy.item import Item, Field

class WikiItem(Item):
    school_name = Field()
    newspaper_name = Field()
    link = Field()

class AmchaUniItem(Item):
    name = Field()
    link = Field()

class IncidentItem(Item):
    amcha_web_id = Field()
    school = Field()
    date = Field()
    unassigned_fields = Field()