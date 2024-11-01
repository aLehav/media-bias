"""Module containing functions that enrich the data in newspapers table"""
from datetime import date
from .postgres import DBConn
from .gcs import GCS

class NewspaperEnricher:
    """Class that enriches newspapers table with automatic and manual data"""
    def __init__(self) -> None:
        self.dbconn = DBConn()
        self.cur = self.dbconn.cur
        self.gcs = GCS()

    def insert_n_links(self, n):
        """Insert up to n links for schools that do not have a link attribute."""
        self.cur.execute("""
            SELECT n.id, n.name, s.name
            FROM newspapers n
            JOIN schools s ON n.school_id = s.id
            WHERE n.link IS NULL
        """)
        # Slice first n entries
        first_n_newspapers = self.cur.fetchall()[:n]
        for row in first_n_newspapers:
            # row entries in order are n.id, n.name, s.name
            link = self.gcs.school_and_newspaper_to_link(row[1], row[2])
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