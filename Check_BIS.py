# -*- coding: utf-8 -*-
"""
Created on Fri Jun  7 11:48:16 2024

@author: LENOVO
"""

import os
import pandas as pd
import numpy as np
from typing import List, Tuple, Dict, Union

def process_csv_data(csv_file_path: str, chunk_size: int, sqi: float) -> Tuple[bool, Union[np.ndarray, None]]:
    """
    Processes CSV data to filter and extract BIS and SQI columns.
    
    Parameters:
    csv_file_path (str): Path to the CSV file.
    chunk_size (int): Number of rows to process at a time.
    sqi (float): Threshold for SQI values.
    
    Returns:
    tuple: (bool, np.ndarray) where bool indicates if an error occurred and np.ndarray contains the processed data.
    """
    try:
        encoding = 'utf-8'
        chunks = pd.read_csv(csv_file_path, chunksize=chunk_size, encoding=encoding, low_memory=False,
                             on_bad_lines ='skip', encoding_errors = 'ignore')
        all_column_values = []
        
        print("Processing: ", csv_file_path)
        for chunk in chunks:
            if 'BIS' in chunk.columns and 'SQI' in chunk.columns:
                column_values = chunk[['BIS', 'SQI']]
                
                column_values = column_values.dropna()
                column_values = column_values.loc[column_values['BIS'] != '-']
                column_values['BIS'] = pd.to_numeric(column_values['BIS'], errors='coerce')
                column_values['SQI'] = pd.to_numeric(column_values['SQI'], errors='coerce')
                column_values.reset_index(drop=True, inplace=True)
                column_values = column_values.dropna()
                
                column_values = column_values.loc[column_values['SQI'] >= sqi]
                
                
                column_values.reset_index(drop=True, inplace=True)
                all_column_values.extend(column_values.to_records(index=False))
        
        all_column_values = np.array(all_column_values, dtype=[('BIS', 'f8'), ('SQI', 'f8')])
        np.set_printoptions(threshold= np.inf)
        return False, all_column_values

    except Exception as e:
        print(f"Error processing CSV file {csv_file_path}: {str(e)}")
        return True, None 
    

def process_folder(parent_folder: str) -> List[str]:
    """
    Processes all CSV files in a folder and its subfolders.
    
    Parameters:
    parent_folder (str): Path to the parent folder.
    
    Returns:
    list: List of paths to CSV files.
    """
    paths = []
    
    # Iterate through all files in the folder and its subfolders
    for foldername, subfolders, filenames in os.walk(parent_folder):        
        for filename in filenames:
            if filename.endswith('.csv'):
                csv_file_path = os.path.join(foldername, filename)
                paths.append(csv_file_path)
    return paths


def check_bis(path: str, bis_range: Tuple[int, int], sqi_threshold: float, time: int, continous: bool = False) -> Tuple[Union[bool, None], Union[int, None]]:
    """
    Checks BIS values in the CSV data for the specified range and duration.
    
    Parameters:
    path (str): Path to the CSV file.
    bis_range (tuple): Range of BIS values to check.
    sqi_threshold (float): SQI threshold.
    time (int): Minimum continuous duration of BIS values in the range.
    continous (bool): If True, checks for continuous duration. Otherwise, checks total count.
    
    Returns:
    tuple: (bool, int) where bool indicates if BIS values are within the range for the specified duration, and int is the count.
    """
    error_occurred, bis_sqi = process_csv_data(path, chunk_size = 10000, sqi=sqi_threshold)
    if error_occurred:
        return None, None
    if continous:
        count = 0 
        temp_count = 0 
        for i in bis_sqi['BIS']:
            if bis_range[0] <= i <= bis_range[1]:
                temp_count += 1
            else:
                temp_count = 0
            if temp_count > count:
                count = temp_count
    else:
        count = sum(bis_range[0] <= x <= bis_range[1] for x in bis_sqi['BIS'] if x != 0)
    if count > time:
        return True, count
    return False, count
    
def find_bis_range(folder: str, bis_range: Tuple[int, int], sqi_threshold: float, time: int, continous: bool) -> Tuple[List[str], Dict[str, int]]:
    """
    Finds files with BIS values in the specified range for the given duration.
    
    Parameters:
    folder (str): Path to the folder containing CSV files.
    bis_range (tuple): Range of BIS values to check.
    sqi_threshold (float): SQI threshold.
    time (int): Minimum continuous duration of BIS values in the range.
    continous (bool): If True, checks for continuous duration. Otherwise, checks total count.
    
    Returns:
    tuple: (list, dict) where list contains paths with errors, and dict contains paths with BIS values within the range.
    """
    paths = process_folder(folder)
    
    bis_below = {}
    errors = []
    
    for path in paths:
        below, count = check_bis(path=path, bis_range=bis_range, sqi_threshold=sqi_threshold, time=time, continous=True)
        if below:
            bis_below[path] = count
        if below == None:
            errors.append(path) 
            continue
        # print(f'{path} has bis below 20 {count} times')
        
    with open(r"E:\DoA Project sid\bis_below_errors.txt", "w") as output:
        output.write(str(errors))
    return errors, bis_below
    
def save(errors: List[str], bis_below: Dict[str, int], file_path: str) -> None:
    """
    Saves errors and BIS below dictionary to a file.
    
    Parameters:
    errors (list): List of error paths.
    bis_below (dict): Dictionary with paths and counts of BIS values within the range.
    file_path (str): Path to the output file.
    """
    try:
        with open(file_path, 'w') as file:
            file.write("Errors:\n")
            for error in errors:
                file.write(f"{error}\n")
            
            file.write("\nBIS Below Dictionary:\n")
            for key, value in bis_below.items():
                file.write(f"{key}: {value}\n")
                
        print(f"Data successfully written to {file_path}")
    except Exception as e:
        print(f"An error occurred while writing to the file: {e}")
        
        
def main():
    """
    Main function to execute the BIS range check and save the results.
    """
    sqi_threshold = 40
    bis_range = [20,40]
    time = 60
    continous = True
    save_file_path = r"E:\DoA Project sid\programs\check bis\test.txt"

    folder = r"E:\DoA Project sid\Extracted data\mar10-april6_2024"

    errors ,bis_below = find_bis_range(folder=folder, bis_range=bis_range, sqi_threshold=sqi_threshold, time=time, continous=continous)
    print(bis_below)
    print(errors)
    save(errors, bis_below, save_file_path)

if __name__ == '__main__':
    main()