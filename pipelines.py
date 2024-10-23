from .postgres import DBConn
from .items import WikiItem
from datetime import date
from psycopg2 import errors
from psycopg2.errorcodes import UNIQUE_VIOLATION

class WikiPipeline:
    def open_spider(self, spider) -> None:
        self.dbconn = DBConn()
        self.conn = self.dbconn.connection
        self.cur = self.dbconn.cur

    def close_spider(self, spider):
        self.dbconn.close(commit=True)

    def process_item(self, item: WikiItem, spider):
        today = date.today()
    
        try:
            # Insert into schools table
            self.cur.execute("""
                INSERT INTO schools (name, origin_link, date_scraped)
                VALUES (%s, %s, %s) 
                RETURNING id
            """, (item['school_name'], item['link'], today))

            school_id = self.cur.fetchone()[0]  # Fetch the school ID returned

            # Insert into newspapers table using the retrieved school_id
            self.cur.execute("""
                INSERT INTO newspapers (school_id, name, origin_link, date_scraped)
                VALUES (%s, %s, %s, %s)  
            """, (school_id, item['newspaper_name'], item['link'], today))

            # If transaction block succeeds, commit.
            self.conn.commit()

        except errors.lookup(UNIQUE_VIOLATION) as e:
            self.conn.rollback()  # Rollback the transaction in case of a unique constraint violation
            spider.logger.info(f"Duplicate entry for school name: {item['school_name']} or newspaper name: {item['newspaper_name']} - {e}")

        except Exception as e:
            # Rollback the transaction for any other exception
            self.conn.rollback()
            spider.logger.error(f"An error occurred: {e}")
        
        return item
        
