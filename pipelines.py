from .postgres import DBConn
from .items import WikiItem, IncidentItem
from .py_config import data_path
from datetime import date
from psycopg2 import errors
from psycopg2.errorcodes import UNIQUE_VIOLATION
from collections import defaultdict
import json

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
            spider.logger.info(f"Match for {amcha_name} already found.")
            return None
        
        school_name = None
        if amcha_name in self.unpaired_schools:
            spider.logger.info(f"Exact match found for {amcha_name} found.")
            school_name = amcha_name
        else:
            if self.manual_verification_stop:
                spider.logger.info(f"No exact match found for {amcha_name}. Left unmatched at verifier's request.")
            else:
                spider.logger.info(f"Attempting to find a rough match found for {amcha_name}:")
                words_in_amcha_name = amcha_name.replace(",","").split(" ")
                common_words = ["University","College","of","the","Community", "State", "", " "]
                stripped_words = [word.strip() for word in words_in_amcha_name]
                words_in_amcha_name = [word for word in stripped_words if word not in common_words]
                spider.logger.info(f"Words in amcha name: {words_in_amcha_name}")
                if len(words_in_amcha_name) == 0: 
                    unpaired_matches = []
                else:
                    unpaired_matches = [school for school in self.unpaired_schools if any(word in school.split(" ") for word in words_in_amcha_name)]
            
                if len(unpaired_matches) == 0:
                    spider.logger.info(f"No rough matches found for {amcha_name}. Keeping unmatched.")
                    school_name = "UNMATCHED"
                else:
                    matches_list = "\n".join([f"{i}. {match}" for i, match in enumerate(unpaired_matches, 1)])

                    spider.logger.info(f"\tRough matches for {amcha_name}:\n{matches_list}")

                    user_input = input(f"Input index of best {amcha_name} match. If you're tired of matching, input -1. If you can't find a match, input 0 to skip.")
                    idx = int(user_input)
                    if idx < 0:
                        spider.logger.info("Manual verification turned off.")
                        self.manual_verification_stop = True
                    elif idx == 0:
                        school_name = "UNMATCHED"
                    elif (idx > 0) and (idx <= len(unpaired_matches)):
                        school_name = unpaired_matches[idx-1]
                        spider.logger.info(f"Closest match: {unpaired_matches[idx-1]}")
                    else:
                        spider.logger.info("Invalid index.")
        
        if school_name:
            try:
                if school_name == "UNMATCHED":
                    self.cur.execute("""
                        INSERT INTO schools (amcha_name, amcha_origin_link, amcha_date_scraped, amcha_name_skipped)
                        VALUES  (%s, %s, %s, %s)
                    """, (amcha_name, origin_link, today, True))
                    self.dbconn.commit()
                else:
                    self.cur.execute("""
                        UPDATE schools
                        SET amcha_name = %s, amcha_origin_link = %s, amcha_date_scraped = %s
                        WHERE name = %s
                    """, (amcha_name, origin_link, today, school_name))
                    self.dbconn.commit()
                    spider.logger.info("Updated schools for scraped name.")
                    self.unpaired_schools.remove(school_name)
            except Exception as e:
                # Rollback the transaction for any other exception
                self.conn.rollback()
                spider.logger.error(f"An error occurred: {e}")

class AmchaIncidentPipeline:
    total_count = 0
    unassigned_field_examples = defaultdict(list)
    
    def unassigned_fields_processor(self, fields):
        field_mapping = {
            # "options": ["TARGETING JEWISH STUDENTS AND STAFF", "ANTISEMITIC EXPRESSION"]
            # Single text val
            '5': {
                'Category': lambda f: f,
            },
            # "options": ["PHYSICAL ASSAULT","DISCRIMINATION","DESTRUCTION OF JEWISH PROPERTY","GENOCIDAL EXPRESSION","SUPPRESSION OF SPEECH/MOVEMENT/ASSEMBLY","BULLYING","DENIGRATION","HISTORICAL","CONDONING TERRORISM","DENYING JEWS SELF-DETERMINATION","DEMONIZATION","BDS ACTIVITY"]
            # List of text vals
            '6_raw': {
                'Classification': lambda f: f,
            },
            '7': {
                'Date': lambda f: f,
            },
            '8_raw': {
                'Description': lambda f: f,
            },
            '36_raw': {
                'University_id': lambda f: f[0]['id'] if f else None,
                'University': lambda f: f[0]['identifier'] if f else None,
            },
            '50_raw': {
                'Photos': lambda f: f['url'] if type(f)!=str else None,
            },
            # "options": [ "PASSED", "FAILED" , ""], text
            '77_raw': {
                'BDS_Vote': lambda f: f,
            },
            '177_raw': {
                'University_Response': lambda f: f,
            }
        }
        
        abc = {}
        
        for field_key, output in field_mapping.items():
            if f"field_{field_key}" in fields:
                for output_key, value_func in output.items():
                    abc[output_key] = value_func(fields[f"field_{field_key}"])


        handled_suffixes = ['5_raw',
                            '6',
                            '7_raw',
                            '8',
                            '36',
                            '50','50:thumb_2','50:thumb_3',
                            '77',
                            '151','151_raw','160.field_151','160.field_151_raw'
                            '177']
        
        # Temporary
        for field in fields:
            if field[:6] == "field_": 
                if (field[6:] not in field_mapping.keys()) and (field[6:] not in handled_suffixes):
                    self.unassigned_field_examples[field].append(fields[field])

        return abc
    
    def open_spider(self, spider) -> None:
        self.dbconn = DBConn()
        self.conn = self.dbconn.connection
        self.cur = self.dbconn.cur

    def close_spider(self, spider) -> None:
        self.dbconn.close(commit=True)
        spider.logger.info(f"\n\nTotal number of examples: {self.total_count}")
        with open(data_path / "unassigned_examples.json", "w") as f:
            json.dump(self.unassigned_field_examples, f, indent=2)
        spider.logger.info(f"\n\nUnique fields not handled and their examples:\n\t{self.unassigned_field_examples}")

    def process_item(self, item: IncidentItem, spider):
        today = date.today()
        abc = self.unassigned_fields_processor(item['unassigned_fields'])
        spider.logger.info(f"ABC: {abc}")
        id = item['amcha_web_id']
        self.total_count += 1

