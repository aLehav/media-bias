from typing import Iterable
import scrapy
from scrapy.http import Response
from .pipelines import WikiPipeline, AmchaUniPipeline
from .items import WikiItem, AmchaUniItem
import re
import json
from urllib.parse import urlencode
import requests

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

class AmchaUniSpider(scrapy.Spider):
    name = "amcha_uni_spider"
    url = "https://us-east-1-renderer-read.knack.com/v1/scenes/scene_121/views/view_208/records" 

    params = { "callback": "jQuery17207918593901822866_1729722973867", "format": "both", "page": 1, "rows_per_page": 100, "sort_field": "field_38", "sort_order": "asc", "_": "1729722974015" } 

    headers = { "accept": "text/javascript, application/javascript, application/ecmascript, application/x-ecmascript, /; q=0.01", "accept-language": "en-US,en;q=0.9,he-IL;q=0.8,he;q=0.7", "priority": "u=1, i", "sec-fetch-dest": "empty", "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin", "x-knack-application-id": "5b39a57db48a6b2ec0cc0ded", "x-knack-new-builder": "true", "x-knack-rest-api-key": "renderer", "x-requested-with": "XMLHttpRequest", "Referer": "https://us-east-1-renderer-read.knack.com/api/xdc.html?xdm_e=https%3A%2F%2Famchainitiative.org&xdm_c=default5922&xdm_p=1", "Referrer-Policy": "strict-origin-when-cross-origin" }

    def response_to_dict(self, response) -> dict:
        json_data = re.search(r"jQuery\d+_\d+\s\=+\s'function'\s&&\sjQuery\d+_\d+(.*);", response.text).group(1)
        data = json.loads(json_data[1:-1])
        return data
    
    def start_requests(self):        
        response_1 = requests.get(self.url, headers=self.headers, params=self.params)
        page_1_data = self.response_to_dict(response_1)
        num_pages = page_1_data['total_pages']
        
        for page in range(1,num_pages+1):
            self.params.update({"page":page})
            query_string = urlencode(self.params)
            full_url = f"{self.url}?{query_string}"
            yield scrapy.Request(url=full_url, headers=self.headers, callback=self.parse, meta={'origin_url':full_url})

    def parse(self, response):
        response_dict = self.response_to_dict(response)
        schools = [item['field_38'] for item in response_dict['records']]
        for school in schools:
            yield AmchaUniItem(name=school, link=response.meta['origin_url'])
