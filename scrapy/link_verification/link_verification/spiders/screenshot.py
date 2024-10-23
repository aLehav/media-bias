import scrapy
from slugify import slugify
import base64
import pandas as pd
from scrapy_splash import SplashRequest 
from link_verification.items import ArchiveScreenshotItem, NewspaperScreenshotItem, ScreenshotItem
import sys
from scrapy.robotstxt import ProtegoRobotParser
import os
from scrapy.exceptions import IgnoreRequest
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../..'))
sys.path.append(project_root)

from media_bias import config
from media_bias.queue_processing import save_queue, load_queue

class NewspaperScreenshotSpider(scrapy.Spider):
    name = "verify"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queue = load_queue(config.queues_path / "01_newspapers.csv")
        self.newspapers = pd.read_csv(config.colleges_dfs_path / "01_newspapers.csv")
        self.newspapers = self.newspapers[self.newspapers['college'].isin(self.queue)]
        self.screenshot_items = {}
    
    def start_requests(self):
        for _, row in self.newspapers.iterrows():
            college = slugify(row['college'])
            print(college)
            newspaper = slugify(row['newspaper'])
            newspaper_link = row['newspaper_link']
            archive_link = row['archive_link']

            self.screenshot_items[college] = ScreenshotItem()

            yield SplashRequest(
                url = newspaper_link,
                callback = self.parse,
                endpoint = 'render.json',
                args = {
                    'html': 1,
                    'png': 1,
                    'width': 1024,
                    'wait':2,
                },
                meta = {
                    'college':college,
                    'newspaper':newspaper,
                    'source':'newspaper'
                }
            )
                
            yield SplashRequest(
                url = archive_link,
                callback = self.parse,
                endpoint = 'render.json',
                args = {
                    'html': 1,
                    'png': 1,
                    'width': 1024,
                    'wait':2,
                },
                meta = {
                    'college':college,
                    'newspaper':newspaper,
                    'source':'archive'
                }
            )
                
    
    def parse(self, response):
        dir = config.screenshots_path / response.meta['college']
        dir.mkdir(parents=True, exist_ok=True)
        path = dir / (response.meta['source'] + '.png')
        img_data = base64.b64decode(response.data['png'])
        with open(path, 'wb') as f:
            f.write(img_data)