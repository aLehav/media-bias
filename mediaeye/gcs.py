"""Module providing an interface to the GCS"""

import requests
from .py_config import GCS_DATA

class GCS:
    """A class for making requests to the Google Custom Search API"""
    def __init__(self) -> None:
        self.url = "https://customsearch.googleapis.com/customsearch/v1"
        self.params = {
            key: GCS_DATA[key]
            for key in ['key','cx','safe','lr']
        }

    def make_request(self, q, num):
        """
        Make a request to the Google Custom Search API.

        Args:
            q (str): The search query.
            num (int): Number of results per request.

        Returns:
            dict: The API response data.
        """
        self.params.update({
            "q": q,
            "num": num,
        })
        response = requests.get(self.url, params=self.params, timeout=120)
        if response.status_code == 200:
            data = response.json()
            return data
        print("Request failed with status code:", response.status_code)
        return None

    def school_and_newspaper_to_link(self, school, newspaper):
        """Get the top link for a given school newspaper combo"""
        q = f"{school} {newspaper} newspaper"
        data = self.make_request(q, GCS_DATA['num_newspaper_results'])
        if data:
            newspapers = data.get('items', [])
            link = newspapers[0]['link']
            return link
        return None