import pandas as pd
from shapely import wkb

def process_wkb_column(df, wkb_column, new_column_name='point'):
    """
    Process a binary WKB (Well-Known Binary) column in a DataFrame and create a new column with the geometry objects.
    
    Parameters:
    df (pandas.DataFrame): Input DataFrame
    wkb_column (str): Name of the column containing binary WKB data
    new_column_name (str): Name for the new column containing geometry objects (default: 'point')
    
    Returns:
    pandas.DataFrame: DataFrame with the new point column
    """
    def convert_wkb(wkb_data):
        try:
            return wkb.loads(bytes(wkb_data))
        except Exception as e:
            print(f"Error processing WKB data: {e}")
            return None
    
    # Create a copy of the DataFrame to avoid modifying the original
    result_df = df.copy()
    
    # Apply the conversion function to the WKB column
    result_df[new_column_name] = result_df[wkb_column].apply(convert_wkb)
    
    return result_df