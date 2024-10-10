import scrapy
import pandas as pd
from scrapy_splash import SplashRequest 
import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..'))
sys.path.append(project_root)

from media_bias import config

class NewspaperScreenshotSpider(scrapy.Spider):
    name = "newspaper_screenshot"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.df = pd.read_csv(colleges_dfs_path / "02_verified_newspapers.csv")
    
    def start_requests(self):
        
        return 

    # https://pypi.org/project/python-slugify/
    # https://scrapeops.io/python-scrapy-playbook/scrapy-splash/#5-take-screenshot