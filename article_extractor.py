# article_info_extractor.py
from datetime import date, datetime
import re
import time
import json
import requests
from bs4 import BeautifulSoup
from .items import ArticleItem

class ArticleExtractor:
    """
    Class to extract information (author, date, content) from a newspaper article given its link.
    """
    def __init__(self) -> None:
        pass

    @staticmethod
    def get_date(soup):
        """
        Extracts the publication date of the newspaper article from the BeautifulSoup object.

        Args:
            soup (BeautifulSoup): The BeautifulSoup object of the article's HTML.

        Returns:
            str: The publication date in 'YYYY-MM-DD' format.
        """
        # Define a list of possible date-related meta field names
        date_field_names = ['date', 'pubdate', 'article:published_time', 'og:article:published_time']

        # Loop through the possible field names and attempt to extract the date from meta fields
        for field_name in date_field_names:
            date_tag = soup.find('meta', {'name': field_name})
            if date_tag:
                date_str = date_tag.get('content')
                try:
                    # Parse the date string into a datetime object
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                    return date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    pass  # Continue to the next field if parsing fails

       # Define patterns for date extraction
        date_patterns = [
            (r"\b(\d{4}-\d{2}-\d{2})\b", '%Y-%m-%d'),  # YYYY-MM-DD
            (r"\b(\d{4}/\d{2}/\d{2})\b", '%Y/%m/%d'),  # YYYY/MM/DD
            (r"\b(\d{2}/\d{2}/\d{4})\b", '%m/%d/%Y'),  # MM/DD/YYYY
            (r"\b(\d{1,2} \w+ \d{4})\b", '%B %d, %Y'),  # Month Day, YYYY
            (r"\b(\w+ \d{1,2}, \d{4})\b", '%B %d, %Y'),  # Month Day, YYYY
            (r"\b(\w+\.\s\d{1,2},\s\d{4})\b", '%B. %d, %Y'),  # Month. Day, YYYY
            (r"\b(\w+\s\d{1,2},\s\d{4})\b", '%B %d, %Y'),  # Month Day, YYYY
        ]

        # Extract date from different patterns in the HTML content
        for pattern, date_format in date_patterns:
            date_match = re.search(pattern, str(soup))
            if date_match:
                date_str = date_match.group(0)
                try:
                    # Parse the date string into a datetime object
                    date_obj = datetime.strptime(date_str, date_format)
                    return date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    pass  # Continue to the next pattern if parsing fails

        # If no valid date is found in the meta fields or content, return "No date found"
        return None

    
    @staticmethod
    def get_author(soup):
        """
        Extracts the author of the newspaper article from the BeautifulSoup object.

        Args:
            soup (BeautifulSoup): The BeautifulSoup object of the article's HTML.

        Returns:
            str: The author's name or "Unknown" if not found.
        """
         # Define patterns for author extraction
        author_patterns = [
            (r"\bBy\s+([\w\s]+)\b", '%s'),  # By Author Name
            (r"\bAuthor:\s+([\w\s]+)\b", '%s'),  # Author: Author Name
        ]

        # Extract author from different patterns in the HTML content
        for pattern, author_format in author_patterns:
            author_match = re.search(pattern, str(soup.get_text()))
            if author_match:
                author_name = author_match.group(1)
                return author_format % author_name.strip()

        # If no valid author is found in the content, check for author tags
        author_tag = soup.find('meta', {'name': 'author'})
        if author_tag:
            return author_tag.get('content').strip()
        
        # If still no author is found, try finding all tags and IDs containing "author"
        # author_candidates = []
        # for tag in soup.find_all(True):
        #     if 'author' in tag.get('id', '').lower() or 'author' in tag.get('class', []):
        #         author_candidates.append(tag.get_text())

        # if author_candidates:
        #     # Choose the longest text among candidates as the author name
        #     author_name = max(author_candidates, key=len)
        #     return author_name.strip()

        # If still no author is found, return "Unknown"
        return None

    @staticmethod
    def get_article(soup):
        """
        Extracts the article content of the newspaper article from the BeautifulSoup object.

        Args:
            soup (BeautifulSoup): The BeautifulSoup object of the article's HTML.

        Returns:
            str: The article's content as a plain text string or "No content found" if not found.
        """
        content_tags = soup.find_all('p')  # Get all <p> tags
        article = '\n\n'.join(tag.get_text().strip() for tag in content_tags) if content_tags else None
        return article
    
    @staticmethod
    def get_title(soup):
        """
        Extracts the content of the newspaper article from the BeautifulSoup object.

        Args:
            soup (BeautifulSoup): The BeautifulSoup object of the article's HTML.

        Returns:
            str: The article's content as a plain text string or "No content found" if not found.
        """
        # TODO: Improve Implementation
        # Find the title tag and extract its text
        title_tag = soup.find('title')
        if title_tag:
            return title_tag.text.strip()
        
        return None

    @staticmethod
    def set_fields(item: ArticleItem, response):
        """Set fields given an item object and its soup"""
        soup = BeautifulSoup(response.text, 'html.parser')
        item['author'] = ArticleExtractor.get_author(soup)
        item['date_written'] = ArticleExtractor.get_date(soup)
        item['processed_article'] = ArticleExtractor.get_article(soup)
        item['title'] = ArticleExtractor.get_title(soup)
        item['processing_method'] = 1

    @staticmethod
    def get_fields(article_link):
        try:
            start_time = time.time()
            response = requests.get(article_link, timeout=60)
            end_time = time.time()
            request_time = end_time - start_time

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')

                start_time = time.time()
                author = ArticleExtractor.get_author(soup)
                end_time = time.time()
                author_extraction_time = end_time - start_time

                start_time = time.time()
                current_date = ArticleExtractor.get_date(soup)
                end_time = time.time()
                date_extraction_time = end_time - start_time

                start_time = time.time()
                content = ArticleExtractor.get_article(soup)
                end_time = time.time()
                content_extraction_time = end_time - start_time

                start_time = time.time()
                title = ArticleExtractor.get_title(soup)
                end_time = time.time()
                title_extraction_time = end_time - start_time

                total_time = sum((request_time, author_extraction_time, date_extraction_time, content_extraction_time, title_extraction_time))

                print(f"Run took {total_time:.4f}")

                return author, current_date, content, title
            else:
                return "Unknown", date.today().strftime('%Y-%m-%d'), f"RESPONSE CODE: {response.status_code}\n No content found", "Unknown"
        except Exception as e:
            print(e)
            return "Unknown", date.today().strftime('%Y-%m-%d'), "Error fetching content", "Unknown"