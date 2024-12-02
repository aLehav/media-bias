"""Module containing scrapy spiders"""

import re
import json
from urllib.parse import urlencode
import pandas as pd
import logging
import requests
import scrapy
from scrapy.utils.log import configure_logging
from .postgres import DBConn
from .items import WikiItem, AmchaUniItem, IncidentItem, ArticleItem
from .article_extractor import ArticleExtractor

configure_logging({"LOG_LEVEL":"INFO"})
logging.getLogger('scrapy').propagate = False

class WikiSpider(scrapy.Spider):
    """Spider that scrapes the wikipedia list of college student newspapers"""
    name = "wiki_spider"
    custom_settings = {
        "ITEM_PIPELINES": {"media_bias.pipelines.WikiPipeline": 100},
    }
    start_urls = ['https://en.wikipedia.org/' \
                  'wiki/List_of_college_and_university_student_newspapers_in_the_United_States']

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

def response_to_dict(response) -> dict:
    """Convert a jQuery response to a dict"""
    json_data = re.search(r"jQuery\d+_\d+\s\=+\s'function'\s&&\sjQuery\d+_\d+(.*);",
                            response.text).group(1)
    data = json.loads(json_data[1:-1])
    return data

class AmchaUniSpider(scrapy.Spider):
    """Spider that scrapes the amcha university list"""
    name = "amcha_uni_spider"
    custom_settings = {
        'ITEM_PIPELINES':{'media_bias.pipelines.AmchaUniPipeline': 100},
    }
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
        "accept": "text/javascript, application/javascript, " \
            "application/ecmascript, application/x-ecmascript, /; q=0.01", 
        "accept-language": "en-US,en;q=0.9,he-IL;q=0.8,he;q=0.7", 
        "priority": "u=1, i", 
        "sec-fetch-dest": "empty", 
        "sec-fetch-mode": "cors", 
        "sec-fetch-site": "same-origin", 
        "x-knack-application-id": "5b39a57db48a6b2ec0cc0ded", 
        "x-knack-new-builder": "true", 
        "x-knack-rest-api-key": "renderer", 
        "x-requested-with": "XMLHttpRequest", 
        "Referer": "https://us-east-1-renderer-read.knack.com/api/" \
            "xdc.html?xdm_e=https%3A%2F%2Famchainitiative.org&xdm_c=default5922&xdm_p=1", 
        "Referrer-Policy": "strict-origin-when-cross-origin" 
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.manual_verification_stop = kwargs.get('manual_verification_stop', False)
        return spider
    
    def __init__(self, *args,  manual_verification_stop=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.manual_verification_stop = manual_verification_stop

    def start_requests(self):
        response_1 = requests.get(self.url, headers=self.headers, params=self.params, timeout=120)
        page_1_data = response_to_dict(response_1)
        num_pages = page_1_data['total_pages']

        for page in range(1,num_pages+1):
            self.params.update({"page":page})
            query_string = urlencode(self.params)
            full_url = f"{self.url}?{query_string}"
            yield scrapy.Request(url=full_url,
                                 headers=self.headers,
                                 callback=self.parse,
                                 meta={'origin_link':full_url})

    def parse(self, response):
        response_dict = response_to_dict(response)
        schools = [item['field_38'] for item in response_dict['records']]
        for school in schools:
            yield AmchaUniItem(name=school, link=response.meta['origin_link'])

class AmchaIncidentSpider(scrapy.Spider):
    """Spider that scrapes the amcha incident list"""
    name = "amcha_incident_spider"
    custom_settings = {
        'ITEM_PIPELINES':{'media_bias.pipelines.AmchaIncidentPipeline': 100},
        'AUTOTHROTTLE_ENABLED': True
    }
    page_url = "https://us-east-1-renderer-read.knack.com/v1/scenes/scene_164/" \
        "views/view_279/records"

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
        "accept": "text/javascript, application/javascript, " \
            "application/ecmascript, application/x-ecmascript, */*; q=0.01",
        "accept-language": "en-US,en;q=0.9,he-IL;q=0.8,he;q=0.7",
        "priority": "u=1, i",
        "sec-ch-ua": "\"Chromium\";v=\"130\", \"Google Chrome\";v=\"130\"," \
            " \"Not?A_Brand\";v=\"99\"",
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
        """Generate incident url from amcha_web_id"""
        return "https://us-east-1-renderer-read.knack.com/v1/scenes/scene_165/views/" \
            f"view_280/records/{amcha_web_id}" 

    incident_params = {
        "callback": "jQuery172010054345929738351_1730158368085",
        # "scene_structure[]": "scene_119",
        "scene_structure[]": "scene_164",
        "_": "1730158368085"
    }
    incident_query_string = urlencode(incident_params)

    incident_headers = {
        "accept": "text/javascript, application/javascript, application/ecmascript, " \
            "application/x-ecmascript, */*; q=0.01",
        "accept-language": "en-US,en;q=0.9,he-IL;q=0.8,he;q=0.7",
        "priority": "u=1, i",
        "sec-ch-ua": "\"Chromium\";v=\"130\", \"Google Chrome\";v=\"130\"," \
             " \"Not?A_Brand\";v=\"99\"",
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

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.dbconn = DBConn()
        self.conn = self.dbconn.connection
        self.cur = self.dbconn.cur
        self.cur.execute("""
            SELECT amcha_web_id FROM incidents
        """)
        self.known_incidents = set((incident_row[0] for incident_row in self.cur.fetchall()))

    def start_requests(self):
        response_1 = requests.get(self.page_url,
                                  headers=self.page_headers,
                                  params=self.page_params,
                                  timeout=120)
        page_1_data = response_to_dict(response_1)
        num_pages = page_1_data['total_pages']

        for page in range(1,num_pages+1):
            self.page_params.update({"page":page})
            query_string = urlencode(self.page_params)
            full_url = f"{self.page_url}?{query_string}"
            yield scrapy.Request(url=full_url,
                                 headers=self.page_headers,
                                 callback=self.parse,
                                 meta={'page_link':full_url})

    def parse(self, response):
        response_dict = response_to_dict(response)
        ids = set((item['id'] for item in response_dict['records']))
        ids -= self.known_incidents
        for incident_id in ids:
            full_url = f"{self.get_incident_url(incident_id)}?{self.incident_query_string}"
            yield scrapy.Request(url=full_url,
                                 headers=self.incident_headers,
                                 callback=self.parse_incident,
                                 meta={'origin_link': full_url, 'amcha_web_id': incident_id})

    def parse_incident(self, response):
        """Pass incident requests onto the item pipelines"""
        response_dict = response_to_dict(response)
        item = IncidentItem(amcha_web_id=response.meta['amcha_web_id'],
                            raw_fields=response_dict,
                            origin_link=response.meta['origin_link'])
        yield item

class ArticleSpider(scrapy.Spider):
    """Spider that scrapes articles"""
    name = "article_spider"
    custom_settings = {
        "ITEM_PIPELINES": {"media_bias.pipelines.ArticlePipeline": 100},
        "AUTOTHROTTLE_ENABLED": True,
        "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "CONCURRENT_REQUESTS_PER_IP": 8,
        "COOKIES_ENABLED": False,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 3,
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "DEFAULT_REQUEST_HEADERS": {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en',
        },
    }

    def __init__(self, *args,  n=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.n = n
        self.dbconn = DBConn()
        self.conn = self.dbconn.connection
        self.cur = self.dbconn.cur
        query_select = """SELECT id, link FROM articles 
        WHERE content IS NULL 
        AND filter_status = 'article'
        AND is_filtered IS TRUE"""
        self.article_df = pd.read_sql(query_select, self.conn)
        if self.n:
            self.article_df = self.article_df.sample(n=10, random_state=42)

        
        for handler in logging.root.handlers:
            if handler.level == logging.NOTSET:
                logging.root.removeHandler(handler)
                print(f"Removed handler: {handler}")

    def start_requests(self):
        for _, row in self.article_df.iterrows():
            article_id = row['id']
            link = row['link']
            yield scrapy.Request(
                url=link, 
                callback=self.parse, 
                meta={'id': article_id}, 
                dont_filter=True
            )

    def parse(self, response):
        if response.status == 200:
            article_id = response.meta['id']
            link = response.url
            content = response.text
            item = ArticleItem(
                id=article_id,
                link=link,
                content=content,
            )
            ArticleExtractor.set_fields(item, response)
            yield item
        else:
            self.logger.warning(f"Failed to fetch {response.url} with status {response.status}")