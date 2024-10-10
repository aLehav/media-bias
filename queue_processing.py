import pandas as pd

def save_queue(path, queue: set):
    pd.Series(list(queue)).to_csv(path, index=False, header=False, encoding='utf-8')

def load_queue(path) -> set:
    queue_df = pd.read_csv(path, encoding='utf-8', header=None)
    return set(queue_df[0].to_list())