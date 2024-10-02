# media-bias
Campus media bias project

# College names
List from [uniRank](https://www.4icu.org/us/a-z/)
- `cd` into `media_bias/scrapy_media_bias`
- Open a terminal and run  
  ```bash
  docker run -it -p 8050:8050 --rm scrapinghub/splash
  ```
- Open a command prompt, activate adl env, and run 
  ```bash
  scrapy crawl colleges
  ```
- Manually delete the last row of the csv

