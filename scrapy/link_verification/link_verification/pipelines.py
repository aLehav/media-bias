# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from link_verification.items import ArchiveScreenshotItem, ScreenshotItem, NewspaperScreenshotItem
from scrapy.exceptions import DropItem
from collections import defaultdict

class AggregatePipeline:
    def __init__(self) -> None:
        self.screenshot_items = {}

    def process_item(self, item, spider):
        college = item['college']

        if college in self.screenshot_items:
            self.screenshot_items[college].update(**item)
            return self.screenshot_items.pop(college)
        else:
            self.screenshot_items[college] = ScreenshotItem(**item)
            raise DropItem(f"Waiting for other screenshot for {college}")
        
    def close_spider(self, spider):
        if self.partial_items:
            raise RuntimeError("Some partial items uncleared.")

class ScreenshotPipeline:
    def __init__(self) -> None:
        pass

    def process_item(self, item, spider):
        print(f"Processing item of type {type(item)} with college {item['college']} and newspaper {item['newspaper']}")