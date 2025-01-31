import pandas as pd
from geolib import geohash

def process_points_column(df, points_column, new_column_name='geohash', precision=7):
    """
    Process Shapely Point column in a DataFrame and create a new column with the calculated geohashes.
    
    Parameters:
    df (pandas.DataFrame): Input DataFrame
    points_column (str): Name of the column containing cooridinate point data
    new_column_name (str): Name for the new column containing geometry objects (default: 'geohash')
    
    Returns:
    pandas.DataFrame: DataFrame with the new geohash column
    """
    def convert_points(point_data):
        try:
            return geohash.encode(point_data.y,point_data.x,precision)
        except Exception as e:
            print(f"Error processing Point data: {e}")
            return None
    
    # Create a copy of the DataFrame to avoid modifying the original
    result_df = df.copy()
    
    # Apply the conversion function to the WKB column
    result_df[new_column_name] = result_df[points_column].apply(convert_points)
    
    return result_df