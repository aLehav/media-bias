from datetime import date
from psycopg2 import errors
from psycopg2.errorcodes import UNIQUE_VIOLATION
from mediaeye.postgres import DBConn
from mediaeye.items import WikiItem

class WikiPipeline:
    """
    Pipeline processing entries from the wikipedia page for college student newspapers.
    Takes in the newspaper entries from the spider as WikiItems and adds them to the DB.
    """
    def __init__(self):
        self.dbconn = DBConn()
        self.cur = self.dbconn.cur

    def close_spider(self, spider):
        """Called on spider closing"""
        self.dbconn.close(commit=True)

    def process_item(self, item: WikiItem, spider):
        """Insert WikiItems into schools DB"""
        today = date.today()
        try:
            # Insert into schools table
            self.cur.execute("""
                INSERT INTO schools (name, origin_link, date_scraped)
                VALUES (%s, %s, %s) 
                ON CONFLICT (name) DO UPDATE
                SET name = EXCLUDED.name
                RETURNING id
            """, (item['school_name'], item['link'], today))
            school_id = self.cur.fetchone()[0]  # Fetch the school ID returned
            # Insert into newspapers table using the retrieved school_id
            self.cur.execute("""
                INSERT INTO newspapers (school_id, name, origin_link, date_scraped)
                VALUES (%s, %s, %s, %s)  
                ON CONFLICT (school_id, name) DO NOTHING
            """, (school_id, item['newspaper_name'], item['link'], today))
            # If transaction block succeeds, commit.
            self.dbconn.commit()
        except errors.lookup(UNIQUE_VIOLATION) as e:
            # Rollback the transaction in case of a unique constraint violation
            self.dbconn.rollback()
            spider.logger.info(f"Duplicate entry for school name: {item['school_name']}" \
                               f"or newspaper name: {item['newspaper_name']} - {e}")
        except Exception as e:
            # Rollback the transaction for any other exception
            self.dbconn.rollback()
            spider.logger.error(f"An error occurred: {e}")

        return item