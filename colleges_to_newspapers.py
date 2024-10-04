from .gcs_requester import GCSRequester
from . import config
import numpy as np
import pandas as pd
from math import floor

class CollegesToNewspapers():
    """
    A class for taking in a csv of colleges, for now assume the list also contains college rankings. It then uses the GCSRequester object to search for likely student newspapers and likely archive links for those papers. It then gneerates a df where each row is a college and info about the most likely newspaper is added to the columns. 
    
    For more newspapers, add it by creating multiple columns per university which can then be joined at analysis time. This is because if we get to a point where we've exhausted all good newspapers for each school's first choice, we're doing well enough to be willing to allow this to bottleneck us a bit.
    """
    def __init__(self) -> None:
        """
        Load the gcs requester and list of newspapers, making one if not present.
        """
        self.gcs_requester = GCSRequester()
        self.save_path = config.colleges_dfs_path / "01_newspapers.csv"
        if not self.save_path.is_file():
            self.df = self.create_new_cols_and_save()
        else:
            self.df = pd.read_csv(self.save_path)

    def create_new_cols_and_save(self) -> pd.DataFrame:
        """
        In the case where there is no existing newspapers df, create and save a blank one. These can then be loaded and gcs querying performed. This is done because the gcs api has a free 100-search-a-day limit.
        """
        df = pd.read_csv(config.colleges_dfs_path / "00_colleges.csv").assign(gcs_performed = False, newspaper_link="", newspaper="", archive_link="")
        df.to_csv(self.save_path, index=False)
        return df
    
    def print_processing_stats(self, subset_df):
        total_rows = len(self.df)
        rows_with_gcs_performed = self.df['gcs_performed'].sum()  # Assumes gcs_performed is 0 or 1
        rows_without_gcs_performed = total_rows - rows_with_gcs_performed
        rows_to_process = len(subset_df)

        print(f"Total rows in DataFrame: {total_rows}")
        print(f"Rows with 'gcs_performed': {rows_with_gcs_performed}")
        print(f"Rows without 'gcs_performed': {rows_without_gcs_performed}")
        print(f"Rows to be processed: {rows_to_process}")

    def run_gcs_queries_and_save(self, num_queries, verbose=0):
        """
        Given num_queries, fill in that number of schools' fields for gcs_performed, newspaper_link, newspaper_name, and archive_link.
        """
        num_schools_to_query = floor(num_queries / config.gcs_api_request["num_archive_results"] / config.gcs_api_request["num_newspaper_results"] / 2)

        # Subset the DataFrame
        subset_df = self.df[self.df['gcs_performed'] == False].head(num_schools_to_query)

        if verbose:
            self.print_processing_stats(subset_df)

        # Define a function to perform the operations on each row
        def process_row(row):
            newspaper = self.gcs_requester.school_to_newspapers(row["college"])[0]
            newspaper_name = newspaper['title']
            archive_link = self.gcs_requester.newspaper_to_archive(newspaper_name)
            
            if not archive_link:
                raise ValueError("Newspaper_to_archive failed. If error 429, Google API is out of tokens.")
            
            # Perform a single assignment with a tuple
            row[['archive_link', 'newspaper_link', 'newspaper', 'gcs_performed']] = (
                archive_link[0]['link'], 
                newspaper['link'], 
                newspaper_name, 
                True
            )
            
            return row
        
        # Apply the function to the DataFrame
        subset_df = subset_df.apply(process_row, axis=1)

        # Bulk assign the updated values back to the original DataFrame
        self.df.loc[subset_df.index, ['archive_link', 'newspaper_link', 'newspaper', 'gcs_performed']] = subset_df[['archive_link', 'newspaper_link', 'newspaper', 'gcs_performed']]

        self.df.to_csv(self.save_path, index=False)