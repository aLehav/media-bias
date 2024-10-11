# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from link_verification.items import ArchiveScreenshotItem, ScreenshotItem, NewspaperScreenshotItem


class AggregatePipeline:
    def __init__(self) -> None:
        self.partial_items = {}

    def process_item(self, item, spider):
        college = item['college']

        if college in self.partial_items:
            if 'newspaper_link' in item:
                newspaper_item = item
                archive_item = self.partial_items.pop(college)
            else:
                archive_item = item
                newspaper_item = self.partial_items.pop(college)
            
            yield ScreenshotItem(
                college = newspaper_item['college'],
                newspaper = newspaper_item['newspaper'],
                newspaper_link = newspaper_item['newspaper_link'],
                newspaper_screenshot = newspaper_item['newspaper_screenshot'],
                archive_link = archive_item['archive_link'],
                archive_screenshot = archive_item['archive_screenshot']
            )
        else:
            self.partial_items[college] = item

class ScreenshotPipeline:
    def __init__(self) -> None:
        pass

    def process_item(self, item, spider):
        print(f"Processing item of type {type(item)} with college {item['college']} and newspaper {item['newspaper']}")