import requests
from . import config

class GCSRequester:
    """
    A class for making requests to the Google Custom Search API.
    """

    def __init__(self):
        """
        Initialize the GCSRequester with API configuration.

        Args:
            config_path (str): Path to the YAML configuration file.
        """
        config_data = config.gcs_api_request
        self.params = config_data['params']
        self.url = "https://customsearch.googleapis.com/customsearch/v1"
        self.num_newspaper_results = config_data['num_newspaper_results']
        self.num_archive_results = config_data['num_archive_results']

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

        response = requests.get(self.url, params=self.params)

        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print("Request failed with status code:", response.status_code)
            return None

    def school_to_newspapers(self, school):
        """
        Search for school-related newspapers using the Google Custom Search API.

        Args:
            school (str): The school used in the search query.
            num (int): Number of results per request.

        Returns:
            list: List of dictionaries containing search results.
        """
        # TODO: Ablation comparison of queries, additional query restrictions
        
        q = f"{school} student newspaper"
        data = self.make_request(q, self.num_newspaper_results)
        if data:
            return data.get('items', [])
        else:
            return []


    def newspaper_to_archive(self, newspaper):
        """
        Search for newspaper archives using the Google Custom Search API.

        Args:
            newspaper (str): The newspaper used in the search query.
            num (int): Number of results per request.

        Returns:
            list: List of dictionaries containing search results.
        """
        # TODO: Ablation comparison of queries, additional query restrictions

        q = f"Archives for {newspaper}"
        data = self.make_request(q, self.num_archive_results)
        if data:
            return data.get('items', [])
        else:
            return []

