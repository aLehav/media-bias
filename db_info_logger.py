"""Moduler for logging information about the DB"""
from .postgres import DBConn

def count_entries(conn, table, condition=None, params=None):
    """Helper function to count entries in the database."""
    query = f"SELECT COUNT(*) FROM {table}"
    if condition:
        query += f" WHERE {condition}"
    conn.cur.execute(query, params or ())
    count = conn.cur.fetchall()
    return count[0][0]

def log():
    """Log general information about the DB."""
    conn = DBConn()
    n_schools = count_entries(conn, 'schools')

    n_amcha_entries = count_entries(conn, 'schools', 'amcha_name IS NOT NULL')

    n_wiki_entries = count_entries(conn, 'schools', 'name IS NOT NULL')

    n_joint_entries = count_entries(conn, 'schools', 'name IS NOT NULL AND amcha_name IS NOT NULL')

    n_link_entries = count_entries(conn, 'newspapers', 'link IS NOT NULL')

    n_inspected_entries = count_entries(conn, 'newspapers', 'link_is_accurate IS NOT NULL')

    n_accurate_entries = count_entries(conn, 'newspapers', 'link_is_accurate IS TRUE')

    n_wordpress_entries = count_entries(conn, 'newspapers', 'link_is_accurate IS TRUE AND is_wordpress IS TRUE')

    n_incidents = count_entries(conn, 'incidents')

    n_article_links = count_entries(conn, 'articles')
    conn.close()
    print(f"There are {n_schools:,} total school DB entries." +
          f"\n\t   {n_wiki_entries:,} schools have a wiki name, and so have a newspaper from the wiki page in the newspapers DB." +
          f"\n\t   {n_amcha_entries:,} schools have an amcha name, and so can have hate crime stats gathered." +
          f"\n\t   {n_joint_entries:,} schools have both wiki and amcha name." +
          f"\n\t   {n_link_entries:,} newspapers have a link searched." +
          f"\n\t   {n_inspected_entries:,} newspapers have had this link manually inspected." +
          f"\n\t   {n_accurate_entries:,} schools have a correct newspaper name and base link." +
          f"\n\t   {n_wordpress_entries:,} schools have a wordpress page." +
          f"\nThere are {n_incidents:,} total incident DB entries." +
          f"\nThere are {n_article_links:,} total article link DB entries.")