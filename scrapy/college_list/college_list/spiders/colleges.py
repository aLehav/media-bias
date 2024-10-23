import scrapy
import pandas as pd
from scrapy_splash import SplashRequest 
import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../..'))
sys.path.append(project_root)

from media_bias import config

class CollegeSpider(scrapy.Spider):
    name = "colleges"
    start_urls = []
    headers = {'User-Agent':'crawl'}
    def start_requests(self):
        url = 'https://www.4icu.org/us/a-z/'
        yield scrapy.Request(
            url,
            callback=self.parse,
            headers={'User-Agent':'crawl'},
            meta={'dont_obey_robotstxt': True} 
        )

    def parse(self, response):
        rows = response.xpath('//tr')
        
        data = []
        queue = set()
        for row in rows:
            rank = row.xpath('td[1]/b/text()').get()
            college = row.xpath('td[2]/a/text()').get()
            city = row.xpath('td[3]/text()').get()
            
            if rank and college and city:
                data.append({
                    'rank': rank,
                    'college': college,
                    'city': city
                })
                queue.add(college)
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        self.save_df(df)
        self.save_queue(queue)

    def save_df(self, df):
        # Save the DataFrame to a CSV file
        df.to_csv(config.colleges_dfs_path / "00_colleges.csv", index=False)
        self.log("DF saved successfully.")

    def save_queue(self, queue):
        # Create a queue of colleges to be processed for the next step
        pd.Series(list(queue)).to_csv(config.queues_path / "00_colleges.csv", index=False, header=False, encoding='utf-8')
        self.log("Queue saved successfully.")