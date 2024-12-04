from datetime import date, datetime
from mediaeye.items import ArticleItem
from mediaeye.postgres import DBConn

class ArticlePipeline:
    """Pipeline to scrape and process articles"""
    total_count = 0

    def __init__(self) -> None:
        self.dbconn = DBConn()
        self.cur = self.dbconn.cur

    def close_spider(self, spider) -> None:
        """Close DB conn and log number of examples"""
        self.dbconn.close(commit=True)
        spider.logger.info(f"\n\nTotal number of examples: {self.total_count}")

    def process_item(self, item: ArticleItem, spider):
        """Process raw fields into DB columns, 
        attempt to get the school from schools DB associated,
        and finally insert the incident into the incidents DB"""
        now = datetime.now()
        today = date.today()
        try:
            self.cur.execute("""
                UPDATE articles
                SET content = %s, processed_article = %s, time_processed = %s, 
                             processing_method = %s, author = %s, title = %s, 
                             date_written = %s, date_scraped = %s
                WHERE id = %s
                RETURNING id;
            """, (item['content'],item['processed_article'],now,
                  item['processing_method'], item['author'], item['title'], 
                  item['date_written'], today,
                  item['id']))
            
            self.dbconn.commit()

            spider.logger.info(f"Article {item['link']} inserted.")
        except Exception as e:
            print(f"An error occurred: {e}")
            self.dbconn.rollback()

        self.total_count += 1