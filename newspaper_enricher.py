"""Module containing functions that enrich the data in newspapers table"""
from datetime import date, datetime
from time import sleep
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from xml.etree.ElementTree import ParseError
from advertools import url_to_df
import pandas as pd
from psycopg2.extras import execute_values
from tqdm.auto import tqdm
import requests
from .postgres import DBConn
from .gcs import GCS
from .sitemaps import sitemap_to_df
from .py_config import BW_API_KEY

GCS_SLEEP_DUR = 0.6

class NewspaperEnricher:
    """Class that enriches newspapers table with automatic and manual data"""
    def __init__(self) -> None:
        self.dbconn = DBConn()
        self.cur = self.dbconn.cur
        self.gcs = GCS()

    @staticmethod
    def _is_wordpress(response):
        """
        Returns a boolean result saying if the response of the BW api denotes a given site is a WP site.
        """
        groups = response.json().get('groups')
        if groups is None: 
            return False
        return any(group['name'] in ['widgets', 'framework', 'hosting'] and
            any(category['name'] in ['wordpress-plugins', 'wordpress-theme', 'wordpress-hosting']
                for category in group['categories']) for group in groups)
    
    @staticmethod
    def _get_base_url(url):
        """Given a url, return the base url using urllib"""
        parsed_url = urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}/"
        return base_url

    @staticmethod
    def _get_article_urls(url):
        """
        For a given newspaper url, find all possible article urls
        """
        base_url = NewspaperEnricher._get_base_url(url)
        try:
            sitemap_df = sitemap_to_df(base_url + "robots.txt")
        except (ValueError, HTTPError, URLError, ParseError):
            try:
                sitemap_df = sitemap_to_df(base_url + "sitemap.xml")
                if sitemap_df is None:
                    return None
            except (ValueError, HTTPError, URLError, ParseError) as e:
                print("Exception occurred in get_sitemaps: ",e)
                return None
        if not 'loc' in sitemap_df.columns:
            return None
        sitemap_df = sitemap_df.dropna(subset=['loc'])
        url_df = url_to_df(sitemap_df['loc'])
        merged_df = pd.merge(sitemap_df, url_df, left_on='loc', right_on='url', how='inner')
        return merged_df

    def insert_links(self, n=None):
        """Insert up to n links for schools that do not have a link attribute."""
        self.cur.execute("""
            SELECT n.id, n.name, s.name
            FROM newspapers n
            JOIN schools s ON n.school_id = s.id
            WHERE n.link IS NULL
        """)
        # Slice first n entries
        newspapers = self.cur.fetchall()
        if n is not None:
            newspapers = newspapers[:n]
        for row in tqdm(newspapers):
            # row entries in order are n.id, n.name, s.name
            link = self.gcs.school_and_newspaper_to_link(row[1], row[2])
            # sleep to not overuse api
            sleep(GCS_SLEEP_DUR)
            if link:
                today = date.today()
                try:

                    self.cur.execute("""
                        UPDATE newspapers
                        SET link = %s, date_link_scraped = %s
                        WHERE id = %s
                    """, (link, today, row[0]))

                    self.dbconn.commit()
                    print(f"{row[1]} {row[2]} added link {link}")
                except Exception as e:
                    self.dbconn.rollback()
                    print(f"An error occurred: {e}")
            else:
                print(f"{row[1]} {row[2]} not fetching GCS results.")

    def verify_links(self):
        """Manually verify that given links match up for schools and newspapers"""
        self.cur.execute("""
            SELECT n.id, n.name, n.link, s.name, n.link_is_accurate
            FROM newspapers n
            JOIN schools s ON n.school_id = s.id
            WHERE n.link IS NOT NULL AND n.link_is_accurate IS NULL
        """)
        # Slice first n entries
        newspapers = self.cur.fetchall()
        for row in newspapers:
            print("Validate" \
                  f"\n\tLink: {row[2]}" \
                    f"\n\tSchool: {row[3]}" \
                        f"\n\tNewspaper: {row[1]}", flush=True)
            user_input = input("Input 1 if all match, 0 if they don't." \
                  " If you're tired of matching, input -1.")
            user_input = int(user_input)
            if user_input < 0 or 1 < user_input:
                break
            try:
                self.cur.execute("""
                    UPDATE newspapers
                    SET link_is_accurate = %s
                    WHERE id = %s
                """, (bool(user_input), row[0]))

                self.dbconn.commit()
                print(f"{row[1]} {row[2]} validated")
            except Exception as e:
                self.dbconn.rollback()
                print(f"An error occurred: {e}")

    def set_wordpress_status(self, n=None):
        """
        Use the builtwith api to determine if a given newspaper is written in wordpress

        Arguments:
        n: number of newspapers
        """
        self.cur.execute("""
            SELECT id, link
            FROM newspapers
            WHERE link IS NOT NULL AND is_wordpress IS NULL
        """)
        # Slice first n entries
        newspapers = self.cur.fetchall()
        if n is not None:
            newspapers = newspapers[:n]
        for row in tqdm(newspapers):
            try:
                r = requests.get("https://api.builtwith.com/free1/api.json", 
                                 params={"KEY":BW_API_KEY, "LOOKUP":row[1]}, timeout=5)
                wordpress = self._is_wordpress(r)
                self.cur.execute("""
                    UPDATE newspapers
                    SET is_wordpress = %s
                    WHERE id = %s
                """, (wordpress, row[0]))

                self.dbconn.commit()
                print(f"{row[1]} wordpress status set")
            except Exception as e:
                self.dbconn.rollback()
                print(f"An error occurred: {e}")
            sleep(1)

    def insert_article_urls(self, n=None, start_index=0, wordpress_only=True):
        """
        For first n newspapers, get all urls and insert them to the db
        """
        if wordpress_only:
            self.cur.execute("""
                SELECT id, school_id, link
                FROM newspapers
                WHERE link_is_accurate IS TRUE AND is_wordpress IS TRUE
            """)
        else:
            self.cur.execute("""
                SELECT id, school_id, link
                FROM newspapers
                WHERE link_is_accurate IS TRUE
                AND school_id = %s OR school_id = %s OR school_id = %s
            """, (2937, 2925, 2926))
        newspapers = self.cur.fetchall()
        if start_index is not None:
            newspapers = newspapers[start_index:]
        if n is not None:
            newspapers = newspapers[:n]
        for row in tqdm(newspapers):
            now = datetime.now()
            article_df = self._get_article_urls(row[2])
            if article_df is not None:
                try:
                    for col in ['lastmod','dir_2','dir_3','dir_4','dir_5']:
                        if col not in article_df.columns:
                            article_df[col] = None
                    article_df['lastmod'] = article_df['lastmod'].replace({pd.NaT: None})
                    article_df = article_df.drop_duplicates(subset='url', keep='first')
                    subset_article_df = article_df[['url','lastmod','sitemap','dir_1','dir_2','dir_3','dir_4','dir_5','last_dir']]
                    article_tuples = list(subset_article_df.itertuples(index=False))
                    enriched_article_tuples = [(row[1], row[0], *t) for t in article_tuples]
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
                    """, (now, row[0]))
                    self.dbconn.commit()
                    print(f"{len(enriched_article_tuples)} articles added for {row[2]}")
                except Exception as e:
                    self.dbconn.rollback()
                    print(f"An error occurred: {e}")

            



