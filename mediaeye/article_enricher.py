"""Module containing functions that enrich the data in articles table"""
from datetime import datetime
import pandas as pd
from psycopg2.extras import execute_values
from mediaeye.postgres import DBConn

class ArticleEnricher:
    """Class that enriches articles table with automatic and manual data"""
    
    tag_dirs = set(["tag"])
    staff_dirs = set(["staff_name","author","staff_profile","authors"])
    ad_dirs = set(["ads","sponsored"])
    article_dirs = set()
    article_dirs.update((str(year) for year in range(1970,2025))) # Add years
    # TODO: check the following dirs: 'people','index.php','p','ac','af'
    # TODO: continue categorizing, stopped at index 100.
    article_dirs.update(("news","sports","uncategorized","archives","news-stories","opinion","articles","sports-stories","features","article","blog","lifestyles","opinions-stories","culture","flipbook_page","category","campus-news","post","life_and_culture-stories","lifestyle","uganews","featured","multimedia","variety","athensnews","people","new-blog","local","arts_and_entertainment","viewpoint","library","index.php","blogs","arts-life","views","arts","the_companion","p","arts-and-life","special-sections","academics","scene","buzz-stories","stories","opinions","entertainment","perspective","feature","arts-and-culture","gameday","event","cops","campus","story_segment","reviews","ac","top-stories","archive","af","offices","funnies","eat-drink","section"))

    def __init__(self) -> None:
        self.dbconn = DBConn()
        self.cur = self.dbconn.cur

    @classmethod
    def _get_filter_status(cls, **kwargs):
        """Returns tuple containing filter_status and is_filtered"""
        if "dir_1" in kwargs:
            dir_1 = kwargs["dir_1"]
            if dir_1 in cls.tag_dirs:
                return "tag", True
            if dir_1 in cls.ad_dirs:
                return "ad", True
            if dir_1 in cls.staff_dirs:
                return "staff", True
            if dir_1 in cls.article_dirs:
                return "article", True
            return "article", False
        return "missing dir_1", False     

    def apply_filter_status(self):
        """Applies filter status to all rows in the articles table and updates the database."""
        query_select = "SELECT id, dir_1 FROM articles"
        query_update = """
            UPDATE articles AS a
            SET filter_status = b.filter_status,
                is_filtered = b.is_filtered,
                time_filtered = b.time_filtered
            FROM (VALUES %s) AS b(id, filter_status, is_filtered, time_filtered)
            WHERE a.id = b.id
        """

        try:
            # Fetch all rows into a DataFrame
            df = pd.read_sql(query_select, self.dbconn.connection)

            # Process rows to compute filter_status and is_filtered
            df["filter_status"], df["is_filtered"] = zip(
                *df["dir_1"].apply(lambda dir_1: self._get_filter_status(dir_1=dir_1))
            )
            df["time_filtered"] = datetime.now()

            # Prepare the data for bulk update
            update_data = df[["id", "filter_status", "is_filtered", "time_filtered"]].values.tolist()

            # Perform bulk update
            execute_values(self.cur, query_update, update_data)

            # Commit changes
            self.dbconn.commit()
        except Exception as e:
            self.dbconn.rollback()
            print(f"Error: {e}")
