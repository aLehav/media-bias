"""Moduler for logging information about the DB"""
from .postgres import DBConn

def log():
    """Log general information about the DB."""
    conn = DBConn()
    def count_entries(table, condition=None, params=None):
        """Helper function to count entries in the database."""
        query = f"SELECT COUNT(*) FROM {table}"
        if condition:
            query += f" WHERE {condition}"
        conn.cur.execute(query, params or ())
        count = conn.cur.fetchone()
        return count[0]
    
    # Gather data
    n_schools = count_entries('schools')
    n_amcha_entries = count_entries('schools', 'amcha_name IS NOT NULL')
    n_wiki_entries = count_entries('schools', 'name IS NOT NULL')
    n_joint_entries = count_entries('schools', 'name IS NOT NULL AND amcha_name IS NOT NULL')
    n_link_entries = count_entries('newspapers', 'link IS NOT NULL')
    n_inspected_entries = count_entries('newspapers', 'link_is_accurate IS NOT NULL')
    n_accurate_entries = count_entries('newspapers', 'link_is_accurate IS TRUE')
    n_wordpress_entries = count_entries('newspapers', 'link_is_accurate IS TRUE AND is_wordpress IS TRUE')
    n_incidents = count_entries('incidents')
    n_physical_assault_incidents = count_entries('incidents', "physical_assault IS TRUE")
    n_links = count_entries('articles')
    n_nonarticle_links = count_entries('articles', "filter_status != 'article' AND is_filtered IS TRUE")
    n_article_links = count_entries('articles', "filter_status = 'article' AND is_filtered IS TRUE")
    n_content_articles = count_entries('articles', "content IS NOT NULL")
    n_israel_articles = count_entries('articles', "title ILIKE %s", ("%Israel%",))
    
    conn.close()
    
    # Calculate padding
    max_width = len(f"{max(n_schools, n_amcha_entries, n_wiki_entries, n_joint_entries, n_link_entries, n_inspected_entries, n_accurate_entries, n_wordpress_entries, n_incidents, n_links, n_nonarticle_links, n_article_links, n_content_articles, n_israel_articles):,}")
    
    # Print with aligned numbers
    print("Schools and Newspapers\n" + "-"*20 + "\n" +
          f"{n_schools:>{max_width},} total school DB entries.\n"
          f"{n_wiki_entries:>{max_width},} schools have a wiki name, and so have a newspaper from the wiki page in the newspapers DB.\n" +
          f"{n_amcha_entries:>{max_width},} schools have an amcha name, and so can have hate crime stats gathered.\n" +
          f"{n_joint_entries:>{max_width},} schools have both wiki and amcha name.\n" +
          f"{n_link_entries:>{max_width},} newspapers have a link searched.\n" +
          f"{n_inspected_entries:>{max_width},} newspapers have had this link manually inspected.\n" +
          f"{n_accurate_entries:>{max_width},} schools have a correct newspaper name and base link.\n" +
          f"{n_wordpress_entries:>{max_width},} accurate schools have a wordpress page.\n" +
          "-"*20 + "\n" + "Incidents\n" + "-"*20 + "\n" +
          f"{n_incidents:>{max_width},} total incident DB entries.\n" +
          f"{n_physical_assault_incidents:>{max_width},} incidents of physical assault.\n" +
          "-"*20 + "\n" + "Articles\n" + "-"*20 + "\n" +
          f"{n_links:>{max_width},} total article link DB entries.\n" +
          f"{n_nonarticle_links:>{max_width},} links are not articles.\n" +
          f"{n_article_links:>{max_width},} links are very likely articles.\n" +
          f"{n_content_articles:>{max_width},} articles have content.\n" +
          f"{n_israel_articles:>{max_width},} titles include 'Israel'.")