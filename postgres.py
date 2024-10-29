from .py_config import POSTGRES_DATA
import psycopg2

class DBConn:
    table_fields = {
        'schools':"""
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            city VARCHAR(255),
            origin_link VARCHAR(255),
            date_scraped DATE,
            amcha_name VARCHAR(255),
            amcha_origin_link VARCHAR(255),
            amcha_date_scraped DATE,
            amcha_name_skipped BOOLEAN DEFAULT FALSE,
            amcha_web_id VARCHAR(127),
            UNIQUE (name)
        """,
        'newspapers':"""
            id SERIAL PRIMARY KEY,
            school_id INTEGER REFERENCES schools(id) ON DELETE RESTRICT,
            name VARCHAR(255),
            origin_link VARCHAR(255),
            date_scraped DATE,
            link VARCHAR(255),
            archive_link VARCHAR(255),
            scraping_method INTEGER,
            time_last_scraped TIMESTAMP,
            UNIQUE (school_id, name)
        """,
        'articles':"""
            id SERIAL PRIMARY KEY,
            school_id INTEGER REFERENCES schools(id) ON DELETE RESTRICT,
            newspaper_id INTEGER REFERENCES newspapers(id) ON DELETE RESTRICT,
            origin_link VARCHAR(255),
            date_scraped DATE,
            link VARCHAR(255),
            raw_text TEXT,
            processed_article TEXT,
            time_processed TIMESTAMP,
            processing_method INTEGER,
            author TEXT,
            title TEXT,
            date_written DATE,
            UNIQUE (link)
        """,
        'incidents':"""
            id SERIAL PRIMARY KEY,
            school_id INTEGER REFERENCES schools(id) ON DELETE RESTRICT,
            origin_link VARCHAR(255),
            date_scraped DATE,
            date_occurred DATE,
            description TEXT,
            amcha_web_id VARCHAR(127),
            category_raw VARCHAR(127),
            classification_raw VARCHAR(255),
            school_web_id VARCHAR(127)
            school_name VARCHAR(255)
            photos_link VARCHAR(255)
            bds_vote_passed BOOLEAN
            school_response TEXT
            targeting_jewish_students_and_staff BOOLEAN
            antisemitic_expression BOOLEAN
            physical_assault BOOLEAN
            discrimination BOOLEAN
            destruction_of_jewish_property BOOLEAN
            genocidal_expression BOOLEAN
            suppression_of_speech_movement_assembly BOOLEAN
            bullying BOOLEAN
            denigration BOOLEAN
            historical BOOLEAN
            condoning_terrorism BOOLEAN
            denying_jews_self_determination BOOLEAN
            demonization BOOLEAN
            bds_activity BOOLEAN
            UNIQUE (amcha_web_id)
        """
    }
    def __init__(self) -> None:
        self.connection = psycopg2.connect(**POSTGRES_DATA)
        self.cur = self.connection.cursor()

    def print_all_table_names(self):
        self.cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        tables = self.cur.fetchall()
        for table in tables:
            print(table[0])

    def create_table(self, key):
        self.cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {key} (
            {DBConn.table_fields[key]}
        )
        """)

    def create_all_tables(self):
        self.create_table('schools')
        self.create_table('newspapers')
        self.create_table('articles')
        self.create_table('incidents')

    def drop_table(self, key):
        self.cur.execute(f"""
        DROP TABLE IF EXISTS {key} CASCADE
        """)

    def purge(self):
        self.drop_table('incidents')
        self.drop_table('articles')
        self.drop_table('newspapers')
        self.drop_table('schools')

    def commit(self):
        self.connection.commit()

    def close(self, commit=False):
        if commit:
            self.commit()
        if self.cur:
            self.cur.close()
        if self.connection:
            self.connection.close()