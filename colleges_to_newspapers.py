from .gcs_requester import GCSRequester
from . import config
from .queue_processing import save_queue, load_queue
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
        self.colleges = pd.read_csv(config.colleges_dfs_path / "00_colleges.csv")
        self.queue = load_queue(config.queues_path / "00_colleges.csv")
        self.df_save_path = config.colleges_dfs_path / "01_newspapers.csv"
        if not self.df_save_path.is_file():
            self.df = self.create_new_cols_and_save()
        else:
            self.df = pd.read_csv(self.df_save_path)

        self.newspaper_queue_save_path = config.queues_path / "01_newspapers.csv"
        if self.newspaper_queue_save_path.is_file():
            self.newspaper_queue = load_queue(self.newspaper_queue_save_path)
        else:
            self.newspaper_queue = set()
            

    def create_new_cols_and_save(self) -> pd.DataFrame:
        """
        In the case where there is no existing newspapers df, create and save a blank one. These can then be loaded and gcs querying performed. This is done because the gcs api has a free 100-search-a-day limit.
        """
        df = pd.read_csv(config.colleges_dfs_path / "00_colleges.csv", nrows=0).assign(gcs_performed = False, newspaper_link="", newspaper="", archive_link="")
        df.to_csv(self.df_save_path, index=False)
        return df
    
    def print_processing_stats(self, subset_df):
        total_rows = len(pd.read_csv(config.colleges_dfs_path / "00_colleges.csv"))
        rows_without_gcs_performed = len(self.queue)
        rows_with_gcs_performed = total_rows - rows_without_gcs_performed
        
        rows_to_process = len(subset_df)

        print(f"Total number of colleges: {total_rows}")
        print(f"Colleges with 'gcs_performed': {rows_with_gcs_performed}")
        print(f"Colleges without 'gcs_performed': {rows_without_gcs_performed}")
        print(f"Colleges to be processed: {rows_to_process}")        

    def run_gcs_queries_and_save(self, num_queries, verbose=0):
        """
        Given num_queries, fill in that number of schools' fields for gcs_performed, newspaper_link, newspaper_name, and archive_link.
        """
        # If queue is empty nothing to do
        if len(self.queue) == 0: return
            
        num_schools_to_query = floor(num_queries / (config.gcs_api_request["num_newspaper_results"] * (1 + config.gcs_api_request["num_archive_results"])))

        # Subset the DataFrame
        schools_to_process = []
        for i in range(num_schools_to_query):
            if self.queue:
                schools_to_process.append(self.queue.pop())

        # Update newspaper queue
        self.newspaper_queue.update(schools_to_process)

        # Subset old df and add new cols
        colleges_df = self.colleges[self.colleges['college'].isin(schools_to_process)]
        colleges_df = colleges_df.assign(gcs_performed = False, newspaper_link="", newspaper="", archive_link="")

        if verbose: self.print_processing_stats(colleges_df)

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
        processed_df = colleges_df.apply(process_row, axis=1)

        # Concatenate to existing df
        self.df = pd.concat([self.df, processed_df], ignore_index=True)

        # Save the new version of the df, queue, and newspaper queue
        self.df.to_csv(self.df_save_path, index=False)
        save_queue(config.queues_path / "00_colleges.csv", self.queue)
        save_queue(self.newspaper_queue_save_path, self.newspaper_queue)

if __name__ == "__main__":
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument("-n", "--num_queries", type=int, help="num of gcs queries to execute to update the table")
    parser.add_argument("-v", "--verbose", default=0, type=int, help="verbosity (0 or 1)")
    args = parser.parse_args()
    num_queries = args['num_queries']
    verbose = args['verbose']

    colleges_to_newspapers = CollegesToNewspapers()
    colleges_to_newspapers.run_gcs_queries_and_save(num_queries, verbose)