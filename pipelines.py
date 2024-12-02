"""Module for scrapy pipelines"""

from datetime import date, datetime
from collections import defaultdict
from psycopg2 import errors
from psycopg2.errorcodes import UNIQUE_VIOLATION

from .postgres import DBConn
from .items import WikiItem, AmchaUniItem, IncidentItem, ArticleItem

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

class AmchaUniPipeline:
    """Takes in amcha university entries, looks at existing wikipedia university entries, 
    and attempts to match them up."""

    def __init__(self) -> None:
        self.dbconn = DBConn()
        self.cur = self.dbconn.cur
        self.manual_verification_stop = False
        self.school_rows = None
        self.known_amcha_names = set()
        self.unpaired_schools = None


    def open_spider(self, spider) -> None:
        """Connect to DB on spider opening"""
        self.manual_verification_stop = getattr(spider, 'manual_verification_stop', False)
        self.cur.execute("""
            SELECT name, amcha_name FROM schools
        """)
        self.school_rows = self.cur.fetchall()
        self.known_amcha_names.update((school_row[1] for school_row in self.school_rows))
        self.known_amcha_names.remove(None)
        self.unpaired_schools = [school_row[0] for school_row in self.school_rows
                                 if school_row[1] is None]

    def close_spider(self, spider):
        """Close DB connection"""
        self.dbconn.close(commit=True)

    def process_item(self, item: AmchaUniItem, spider):
        """Find a matching wikipedia school entry for a given amcha item"""
        amcha_name = item['name']
        origin_link = item['link']
        if amcha_name in self.known_amcha_names:
            spider.logger.info(f"Match for {amcha_name} already found.")
        else:
            self.process_new_item(spider, amcha_name, origin_link)

    def process_new_item(self, spider, amcha_name, origin_link):
        """Find a match for a new amcha item"""
        if amcha_name in self.unpaired_schools:
            spider.logger.info(f"Exact match found for {amcha_name} found.")
            self.update_schools_db(spider, amcha_name, origin_link, school_name=amcha_name)
        elif self.manual_verification_stop:
            spider.logger.info(f"No exact match found for {amcha_name}." \
                                " Left unmatched at verifier's request.")
        else:
            self.process_rough_matches(spider, amcha_name, origin_link)

    def process_rough_matches(self, spider, amcha_name, origin_link):
        """Attempt to find rough matches for an item"""
        candidates = self.generate_rough_matches(spider, amcha_name)
        if len(candidates) == 0:
            spider.logger.info(f"No rough matches found for {amcha_name}." \
                                " Keeping unmatched.")
            self.update_schools_db(spider, amcha_name, origin_link, matched=False)
        else:
            matches_list = "\n".join([f"{i}. {match}"
                                        for i, match in enumerate(candidates, 1)])

            spider.logger.info(f"\tRough matches for {amcha_name}:\n{matches_list}")

            user_input = input(f"Input index of best {amcha_name} match." \
                                " If you're tired of matching, input -1." \
                                " If you can't find a match, input 0 to skip.")
            idx = int(user_input)
            if idx < 0:
                spider.logger.info("Manual verification turned off.")
                self.manual_verification_stop = True
            elif idx == 0:
                self.update_schools_db(spider, amcha_name, origin_link, matched=False)
            elif 0 < idx <= len(candidates):
                school_name = candidates[idx-1]
                spider.logger.info(f"Closest match: {candidates[idx-1]}")
                self.update_schools_db(spider, amcha_name, origin_link, school_name=school_name)
            else:
                spider.logger.info("Invalid index.")

    def generate_rough_matches(self, spider, amcha_name):
        """Find university names in the wikipedia name base that roughly match the
        amcha name. Current scheme gets individual words and filters out common ones,
        then performing look up"""
        spider.logger.info(f"Attempting to find a rough match found for {amcha_name}:")
        # Get words in name
        words_in_amcha_name = amcha_name.replace(",","").split(" ")
        common_words = ["University","College","of","the","Community", "State", "", " "]
        # Strip and filter out common words
        stripped_words = [word.strip() for word in words_in_amcha_name]
        words_in_amcha_name = [word for word in stripped_words if word not in common_words]
        spider.logger.info(f"Words in amcha name: {words_in_amcha_name}")
        # If no words left, return no candidates
        if len(words_in_amcha_name) == 0:
            return []
        # Otherwise return all candidates with at least one common word.
        candidates = [school for school in self.unpaired_schools if
                        any(word in school.split(" ") for word in words_in_amcha_name)]
        return candidates


    def update_schools_db(self, spider, amcha_name, origin_link, school_name=None, matched=True):
        """Update the schools DB either by adding a new entry for unmatched data
        or updating an existing matched entry"""
        today = date.today()
        try:
            if matched:
                self.cur.execute("""
                    UPDATE schools
                    SET amcha_name = %s, amcha_origin_link = %s, amcha_date_scraped = %s
                    WHERE name = %s
                """, (amcha_name, origin_link, today, school_name))
                self.dbconn.commit()
                spider.logger.info("Updated schools for scraped name.")
                self.unpaired_schools.remove(school_name)
            else:
                self.cur.execute("""
                    INSERT INTO schools (amcha_name, amcha_origin_link, amcha_date_scraped, amcha_name_skipped)
                    VALUES  (%s, %s, %s, %s)
                """, (amcha_name, origin_link, today, True))
                self.dbconn.commit()
        except Exception as e:
            # Rollback the transaction for any other exception
            self.dbconn.rollback()
            spider.logger.error(f"An error occurred: {e}")

class AmchaIncidentPipeline:
    """Pipeline to take in amcha incident data and insert it into the DB"""
    total_count = 0
    unassigned_field_examples = defaultdict(list)

    def to_db_col_name(self, option: str):
        """Convert an option in the amcha forms to one matching our table names
        via lowercasing and substituting special chars with underscores"""
        processed_option = option.lower().replace(" ","_").replace("/","_").replace("-","_")
        return processed_option

    def raw_fields_processor(self, fields):
        """Process raw fields from the response into DB cols"""
        def category_mapping(f: str):
            if self.to_db_col_name(f) == 'targeting_jewish_students_and_staff':
                return {
                    'targeting_jewish_students_and_staff': True,
                    'antisemitic_expression': False,
                    'category_raw': f
                }
            if self.to_db_col_name(f) == 'antisemitic_expression':
                return {
                    'targeting_jewish_students_and_staff': False,
                    'antisemitic_expression': True,
                    'category_raw': f
                }
            raise ValueError(f"Category mapping with category {self.to_db_col_name(f)}")

        def classification_mapping(f: list):
            classification_dict = {
                'physical_assault': False, 
                'discrimination': False, 
                'destruction_of_jewish_property': False, 
                'genocidal_expression': False, 
                'suppression_of_speech_movement_assembly': False, 
                'bullying': False, 
                'denigration': False, 
                'historical': False, 
                'condoning_terrorism': False, 
                'denying_jews_self_determination': False, 
                'demonization': False, 
                'bds_activity': False,
                'classification_raw': str(f)
            }
            for classification in f:
                classification_dict[self.to_db_col_name(classification)] = True
            return classification_dict

        def bds_vote_passed_mapping(f: str):
            mapping = {
                "PASSED": True,
                "FAILED": False,
                "": None
            }
            if f in mapping:
                return {'bds_vote_passed': mapping[f]}
            raise ValueError(f"Unexpected bds_vote_passed val: {f}")

        field_mapping = {
            '5': category_mapping,
            '6_raw': classification_mapping,
            '7': lambda f: {'date_occurred': f},
            '8_raw': lambda f: {'description': f},
            '36_raw': lambda f: {
                'school_web_id': f[0]['id'] if f else None,
                'school_name': f[0]['identifier'] if f else None,
            },
            '50_raw': lambda f: {
                'photos_link': f['url'] if not isinstance(f, str) else None,
            },
            '77_raw': bds_vote_passed_mapping,
            '177_raw': lambda f: {'school_response': f if len(f) else None},
        }

        mapped_fields = {}

        for field_key, output_func in field_mapping.items():
            if f"field_{field_key}" in fields:
                mapped_fields.update(output_func(fields[f"field_{field_key}"]))

        return mapped_fields

    def __init__(self) -> None:
        self.dbconn = DBConn()
        self.cur = self.dbconn.cur

    def close_spider(self, spider) -> None:
        """Close DB conn and log number of examples"""
        self.dbconn.close(commit=True)
        spider.logger.info(f"\n\nTotal number of examples: {self.total_count}")

    def process_item(self, item: IncidentItem, spider):
        """Process raw fields into DB columns, 
        attempt to get the school from schools DB associated,
        and finally insert the incident into the incidents DB"""
        today = date.today()
        mapped_dict = self.raw_fields_processor(item['raw_fields'])
        if 'school_web_id' not in mapped_dict:
            raise RuntimeError(f"Entry with no school_web_id: {mapped_dict}")

        mapped_dict.update({
            'date_scraped': today,
            'origin_link': item['origin_link'],
            'amcha_web_id': item['amcha_web_id'],
        })
        try:
            self.cur.execute("""
                UPDATE schools
                SET amcha_web_id = %s
                WHERE amcha_name = %s
                RETURNING id;
            """, (mapped_dict['school_web_id'],mapped_dict['school_name']))
            school_id = self.cur.fetchone()[0]
            mapped_dict['school_id'] = school_id

            cols = ', '.join(mapped_dict.keys())
            placeholders = ', '.join(['%s'] * len(mapped_dict))
            sql = f"INSERT INTO incidents ({cols}) VALUES ({placeholders})"
            self.cur.execute(sql, tuple(mapped_dict.values()))

            self.dbconn.commit()

            spider.logger.info(f"Incident for {mapped_dict['school_name']}" +
                               f" on {mapped_dict['date_occurred']} inserted.")
        except Exception as e:
            print(f"An error occurred: {e}")
            self.dbconn.rollback()

        self.total_count += 1

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