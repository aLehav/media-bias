import scrapy
import pandas as pd
from scrapy_splash import SplashRequest 
import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..'))
sys.path.append(project_root)

from media_bias import config

class CollegeSpider(scrapy.Spider):
    name = "colleges"
    start_urls = []
    headers = {'User-Agent':'crawl'}
    def start_requests(self):
        url = 'https://www.4icu.org/us/a-z/'
        yield SplashRequest(
            url,
            callback=self.parse,
            headers={'User-Agent':'crawl'},
            meta={'dont_obey_robotstxt': True} 
        )

    def parse(self, response):
        rows = response.xpath('//tr')
        
        data = []
        for row in rows:
            rank = row.xpath('td[1]/b/text()').get()
            college = row.xpath('td[2]/a/text()').get()
            city = row.xpath('td[3]/text()').get()
            
            if rank and college:
                data.append({
                    'rank': rank,
                    'college': college,
                    'city': city
                })
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        self.save_to_csv(df)

    def save_to_csv(self, df):
        # Save the DataFrame to a CSV file
        df.to_csv(config.data_path / "colleges_dfs/00_college.csv", index=False)
        self.log("CSV saved successfully.")