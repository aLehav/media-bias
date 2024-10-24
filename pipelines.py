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
            self.dbconn.commit()

        except errors.lookup(UNIQUE_VIOLATION) as e:
            self.conn.rollback()  # Rollback the transaction in case of a unique constraint violation
            spider.logger.info(f"Duplicate entry for school name: {item['school_name']} or newspaper name: {item['newspaper_name']} - {e}")

        except Exception as e:
            # Rollback the transaction for any other exception
            self.conn.rollback()
            spider.logger.error(f"An error occurred: {e}")
        
        return item
        
class AmchaUniPipeline:
    manual_verification_stop = False

    def open_spider(self, spider) -> None:
        self.dbconn = DBConn()
        self.conn = self.dbconn.connection
        self.cur = self.dbconn.cur
        self.cur.execute("""
            SELECT name, amcha_name FROM schools
        """)
        self.school_rows = self.cur.fetchall()
        self.known_amcha_names = set((school_row[1] for school_row in self.school_rows))
        self.known_amcha_names.remove(None)
        self.unpaired_schools = [school_row[0] for school_row in self.school_rows if school_row[1] is None]

    def close_spider(self, spider):
        self.dbconn.close(commit=True)

    def process_item(self, item: WikiItem, spider):
        today = date.today()
        amcha_name = item['name']
        origin_link = item['link']

        if amcha_name in self.known_amcha_names:
            print(f"Match for {amcha_name} already found.")
            return None
        
        school_name = None
        if amcha_name in self.unpaired_schools:
            print(f"Exact match found for {amcha_name} found.")
            school_name = amcha_name
        else:
            if self.manual_verification_stop:
                print(f"No exact match found for {amcha_name}. Left unmatched at verifier's request.")
            else:
                print(f"Attempting to find a rough match found for {amcha_name}:")
                words_in_amcha_name = amcha_name.replace(",","").split(" ")
                stripped_words = [word.strip() for word in words_in_amcha_name]
                common_words = ["University","College","of","the","Community", "State"]
                words_in_amcha_name = [word for word in stripped_words if word not in common_words]
                if len(words_in_amcha_name) == 0: return
                unpaired_matches = [school for school in self.unpaired_schools if any(word in school for word in words_in_amcha_name)]

            
                if len(unpaired_matches) == 0:
                    print(f"No rough matches found for {amcha_name}.")
                else:
                    matches_list = "\n".join([f"{i}. {match}" for i, match in enumerate(unpaired_matches, 1)])

                    print(f"\tRough matches for {amcha_name}:\n{matches_list}", flush=True)

                    user_input = input(f"Input index of best {amcha_name} match. If you're tired of matching, input -1. If you can't find a match, input 0 to skip.")
                    idx = int(user_input)
                    if idx < 0:
                        print("Manual verification turned off.")
                        self.manual_verification_stop = True
                    elif (idx > 0) and (idx < len(unpaired_matches)):
                        school_name = unpaired_matches[idx-1]
                        print(f"Closest match: {unpaired_matches[idx-1]}")
        
        if school_name:
            try:
                self.cur.execute("""
                    UPDATE schools
                    SET amcha_name = %s, amcha_origin_link = %s, amcha_date_scraped = %s
                    WHERE name = %s
                """, (amcha_name, origin_link, today, school_name))
                self.dbconn.commit()
                self.unpaired_schools.remove(school_name)
            except Exception as e:
                # Rollback the transaction for any other exception
                self.conn.rollback()
                spider.logger.error(f"An error occurred: {e}")
        # print(item['name'])