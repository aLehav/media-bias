import scrapy
from slugify import slugify
import base64
import pandas as pd
from scrapy_splash import SplashRequest 
from scrapy_media_bias.items import ArchiveScreenshotItem, NewspaperScreenshotItem
import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..'))
sys.path.append(project_root)

from media_bias import config
from media_bias.queue_processing import save_queue, load_queue

class NewspaperScreenshotSpider(scrapy.Spider):
    name = "newspaper_screenshot"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queue = load_queue(config.queues_path / "01_newspapers.csv")
        self.newspapers = pd.read_csv(config.colleges_dfs_path / "01_newspapers.csv")
        self.newspapers = self.newspapers[self.newspapers['college'].isin(self.queue)]
        self.newspapers_df = pd.read_csv(config.colleges_dfs_path / 
        "02_verified_newspapers.csv")
        self.start_urls = self.newspapers['links']

    
    def start_requests(self):
        for _, row in self.newspapers.iterrows():
            college = row['college']
            newspaper = slugify(row['newspaper'])
            newspaper_link = row['newspaper_link']
            archive_link = row['archive_link']

            yield SplashRequest(
                url = newspaper_link,
                callback = self.parse_newspaper,
                endpoint = 'render.json',
                args = {
                    'html': 1,
                    'png': 1,
                    'width': 100,
                    'wait':0.5
                },
                meta = {
                    'college':college,
                    'newspaper':newspaper,
                    'newspaper_link':newspaper_link
                }
            )

            yield SplashRequest(
                url = archive_link,
                callback = self.parse_archive,
                endpoint = 'render.json',
                args = {
                    'html': 1,
                    'png': 1,
                    'width': 100,
                    'wait':0.5
                },
                meta = {
                    'college':college,
                    'archive_link':archive_link
                }
            )
    
    def parse_newspaper(self, response):
        yield NewspaperScreenshotItem(
            college = response.meta['college'],
            newspaper = response.meta['newspaper'],
            newspaper_link = response.meta['newspaper_link'],
            newspaper_screenshot = response.data['png']
        )

    def parse_archive(self, response):
        yield ArchiveScreenshotItem(
            college = response.meta['college'],
            archive_link = response.meta['archive_link'],
            archive_screenshot = response.data['png']
        ) 
        

    # https://pypi.org/project/python-slugify/
    # https://scrapeops.io/python-scrapy-playbook/scrapy-splash/#5-take-screenshot