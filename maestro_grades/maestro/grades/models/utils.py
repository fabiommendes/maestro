import pandas as pd


def clean_empty(dic):
    """
    Remove null values.
    """
    return {k: v for k, v in dic.items() if not pd.isna(v) and v != ""}
