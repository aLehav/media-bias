from datetime import date, datetime
from advertools import url_to_df
import pandas as pd
from psycopg2.extras import execute_values
from mediaeye.items import ArticleItem, ArticleInsertItem
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

class ArticleInsertPipeline:
    """Pipeline to scrape and insert articles"""

    def __init__(self) -> None:
        self.dbconn = DBConn()
        self.cur = self.dbconn.cur

    def close_spider(self, spider) -> None:
        """Close DB conn and log number of examples"""
        self.dbconn.close(commit=True)

    def process_item(self, item: ArticleInsertItem, spider):
        """Process raw fields into DB columns, 
        attempt to get the school from schools DB associated,
        and finally insert the incident into the incidents DB"""
        now = datetime.now()
        sitemap_df = item['df']
        if (sitemap_df is not None) and ('loc' in sitemap_df.columns):
            sitemap_df = sitemap_df.dropna(subset=['loc'])
            url_df = url_to_df(sitemap_df['loc'])
            if (url_df is not None) and ('dir_1' in url_df.columns):
                article_df = pd.merge(sitemap_df, url_df, left_on='loc', right_on='url', how='inner')
                try:
                    for col in ['lastmod','dir_2','dir_3','dir_4','dir_5']:
                        if col not in article_df.columns:
                            article_df[col] = None
                    article_df['lastmod'] = article_df['lastmod'].replace({pd.NaT: None})
                    article_df = article_df.drop_duplicates(subset='url', keep='first')
                    subset_article_df = article_df[['url','lastmod','sitemap','dir_1','dir_2','dir_3','dir_4','dir_5','last_dir']]
                    article_tuples = list(subset_article_df.itertuples(index=False))
                    enriched_article_tuples = [(item['school_id'], item['newspaper_id'], *t) for t in article_tuples]
                    execute_values(self.cur, """
                        INSERT INTO articles
                        (school_id, newspaper_id, link, lastmod, origin_link, dir_1, dir_2, dir_3, dir_4, dir_5, last_dir)
                        VALUES %s
                        ON CONFLICT (link) DO NOTHING                        
                    """, enriched_article_tuples)
                    self.cur.execute("""
                        UPDATE newspapers
                        SET time_last_scraped = %s
                        WHERE id = %s
                    """, (now, item['newspaper_id']))
                    self.dbconn.commit()
                    spider.logger.info(f"{len(enriched_article_tuples)} links updated/added for newspaper id {item['newspaper_id']}")
                except Exception as e:
                    self.dbconn.rollback()
                    spider.logger.info(f"An error occurred in ArticleInsertPipeline: {e}")