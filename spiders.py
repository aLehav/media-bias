from typing import Iterable
import scrapy
from scrapy.http import Response
from .items import WikiItem, AmchaUniItem, IncidentItem
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

    params = { 
        "callback": "jQuery17207918593901822866_1729722973867", 
        "format": "both", 
        "page": 1, 
        "rows_per_page": 100, 
        "sort_field": "field_38", 
        "sort_order": "asc", 
        "_": "1729722974015"
    } 

    headers = { 
        "accept": "text/javascript, application/javascript, application/ecmascript, application/x-ecmascript, /; q=0.01", 
        "accept-language": "en-US,en;q=0.9,he-IL;q=0.8,he;q=0.7", 
        "priority": "u=1, i", 
        "sec-fetch-dest": "empty", 
        "sec-fetch-mode": "cors", 
        "sec-fetch-site": "same-origin", 
        "x-knack-application-id": "5b39a57db48a6b2ec0cc0ded", 
        "x-knack-new-builder": "true", 
        "x-knack-rest-api-key": "renderer", 
        "x-requested-with": "XMLHttpRequest", 
        "Referer": "https://us-east-1-renderer-read.knack.com/api/xdc.html?xdm_e=https%3A%2F%2Famchainitiative.org&xdm_c=default5922&xdm_p=1", 
        "Referrer-Policy": "strict-origin-when-cross-origin" 
    }

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
            yield scrapy.Request(url=full_url, headers=self.headers, callback=self.parse, meta={'origin_link':full_url})

    def parse(self, response):
        response_dict = self.response_to_dict(response)
        schools = [item['field_38'] for item in response_dict['records']]
        for school in schools:
            yield AmchaUniItem(name=school, link=response.meta['origin_link'])

class AmchaIncidentSpider(scrapy.Spider):
    name = "amcha_incident_spider"

    page_url = "https://us-east-1-renderer-read.knack.com/v1/scenes/scene_164/views/view_279/records" 

    page_params = {
        "callback": "jQuery172010054345929738351_1730158238902",
        "format": "both",
        "page": 1,
        "rows_per_page": 1000,
        "sort_field": "field_7",
        "sort_order": "desc",
        "_": "1730158604876"
    } 

    page_headers = {
        "accept": "text/javascript, application/javascript, application/ecmascript, application/x-ecmascript, */*; q=0.01",
        "accept-language": "en-US,en;q=0.9,he-IL;q=0.8,he;q=0.7",
        "priority": "u=1, i",
        "sec-ch-ua": "\"Chromium\";v=\"130\", \"Google Chrome\";v=\"130\", \"Not?A_Brand\";v=\"99\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "x-knack-application-id": "5b39a57db48a6b2ec0cc0ded",
        "x-knack-new-builder": "true",
        "x-knack-rest-api-key": "renderer",
        "x-requested-with": "XMLHttpRequest",
    }

    def get_incident_url(self, amcha_web_id):
        return f"https://us-east-1-renderer-read.knack.com/v1/scenes/scene_165/views/view_280/records/{amcha_web_id}" 

    incident_params = {
        "callback": "jQuery172010054345929738351_1730158368085",
        "scene_structure[]": "scene_119",
        "scene_structure[]": "scene_164",
        "_": "1730158368085"
    } 
    incident_query_string = urlencode(incident_params)

    incident_headers = {
        "accept": "text/javascript, application/javascript, application/ecmascript, application/x-ecmascript, */*; q=0.01",
        "accept-language": "en-US,en;q=0.9,he-IL;q=0.8,he;q=0.7",
        "priority": "u=1, i",
        "sec-ch-ua": "\"Chromium\";v=\"130\", \"Google Chrome\";v=\"130\", \"Not?A_Brand\";v=\"99\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "x-knack-application-id": "5b39a57db48a6b2ec0cc0ded",
        "x-knack-new-builder": "true",
        "x-knack-rest-api-key": "renderer",
        "x-requested-with": "XMLHttpRequest",
    }

    def response_to_dict(self, response) -> dict:
        json_data = re.search(r"jQuery\d+_\d+\s\=+\s'function'\s&&\sjQuery\d+_\d+(.*);", response.text).group(1)
        data = json.loads(json_data[1:-1])
        return data
    
    def start_requests(self):        
        response_1 = requests.get(self.page_url, headers=self.page_headers, params=self.page_params)
        page_1_data = self.response_to_dict(response_1)
        num_pages = page_1_data['total_pages']
        
        for page in range(1,num_pages+1):
            self.page_params.update({"page":page})
            query_string = urlencode(self.page_params)
            full_url = f"{self.page_url}?{query_string}"
            yield scrapy.Request(url=full_url, headers=self.page_headers, callback=self.parse_page, meta={'page_link':full_url})

    def parse_page(self, response):
        response_dict = self.response_to_dict(response)
        ids = [item['id'] for item in response_dict['records']]
        for idx, id in enumerate(ids):
            full_url = f"{self.get_incident_url(id)}?{self.incident_query_string}"
            yield scrapy.Request(url=full_url, headers=self.incident_headers, callback=self.parse_incident, meta={'origin_link': full_url, 'amcha_web_id': id})
            if idx > 100:
                break

    def parse_incident(self, response):
        response_dict = self.response_to_dict(response)
        item = IncidentItem(amcha_web_id=response.meta['amcha_web_id'], unassigned_fields=response_dict)
        yield item