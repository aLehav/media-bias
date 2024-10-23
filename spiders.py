import scrapy
from .pipelines import WikiPipeline
from .items import WikiItem

class WikiSpider(scrapy.Spider):
    name = "wiki_spider"
    start_urls = ['https://en.wikipedia.org/wiki/List_of_college_and_university_student_newspapers_in_the_United_States']

    def parse(self, response):
        for li in response.xpath('//*[@id="mw-content-text"]/div[1]/ul/li'):
            item = WikiItem()
            item['link'] = self.start_urls[0]

            li_text = ''.join(li.xpath('.//text()').getall()).strip()
            li_text_parts = li_text.split(" – ", 1)

            if len(li_text_parts)>1:
                item['school_name'] = li_text_parts[0].strip()
                item['newspaper_name'] = li_text_parts[1].strip()
                self.logger.info(f"Yielding item: {item['school_name']} - {item['newspaper_name']}")
                yield item
            else:
                self.logger.info(f"Insufficient li entry for parts {li_text}")


