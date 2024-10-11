# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field

class ArchiveScreenshotItem(Item):
    college = Field()
    archive_link = Field()
    archive_screenshot = Field()

class NewspaperScreenshotItem(Item):
    college = Field()
    newspaper = Field()
    newspaper_link = Field()
    newspaper_screenshot = Field()

class ScreenshotItem(Item):
    college = Field()
    newspaper = Field()
    newspaper_link = Field()
    archive_link = Field()
    newspaper_screenshot = Field()
    archive_screenshot = Field()


