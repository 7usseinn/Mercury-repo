import pandas as pd

def remove_duplicates(df, subset=None):
        """
        Remove duplicate rows from a DataFrame.
        
        :param df: DataFrame
        :param subset: column labels to consider for identifying duplicates
        :return: DataFrame without duplicates
        """
        return df.drop_duplicates(subset=subset)
    
def handle_missing_values(df, method='drop'):
        """
        Handle missing values in a DataFrame.
        
        :param df: DataFrame
        :param method: 'drop' to remove rows with NaN, 'fill' to fill NaN with mean
        :return: DataFrame after handling missing values
        """
        if method == 'drop':
            return df.dropna()
        elif method == 'fill':
            return df.fillna(df.mean())
    
    
def filter_data(df, condition):
        """
        Filter rows based on a condition.
        
        :param df: DataFrame
        :param condition: Condition to filter rows
        :return: Filtered DataFrame
        """
        return df[condition]

def sort_data(df, by, ascending=True):
        """
        Sort DataFrame.
        
        :param df: DataFrame
        :param by: Column name or list of column names
        :param ascending

        : Whether to sort in ascending order or not. Default is True.
        :return: Sorted DataFrame
        """
        return df.sort_values(by=by, ascending=ascending)
    
def rename_columns(df, columns):
        """
        Rename columns of a DataFrame.
        
        :param df: DataFrame
        :param columns: Dictionary containing old column names as keys and new column names as values
        :return: DataFrame with renamed columns
        """
        return df.rename(columns=columns)

def group_data(df, by, aggregates):
        """
        Group by operations on a DataFrame.
        
        :param df: DataFrame
        :param by: Column name or list of column names to group by
        :param aggregates: Dictionary of operations to perform on grouped data
        :return: Grouped DataFrame
        """
        return df.groupby(by).agg(aggregates)

def convert_data_types(df, columns):
        """
        Convert data types of specific columns in a DataFrame.
        
        :param df: DataFrame
        :param columns: Dictionary with column names as keys and desired data types as values
        :return: DataFrame with converted data types
        """
        return df.astype(columns)

def set_index(df, column):
        """
        Set a column as index for the DataFrame.
        
        :param df: DataFrame
        :param column: Column name
        :return: DataFrame with new index
        """
        return df.set_index(column)

def reset_index(df, drop=False):
        """
        Reset the index of a DataFrame.
        
        :param df: DataFrame
        :param drop: Whether to drop the current index column. Default is False.
        :return: DataFrame with reset index
        """
        return df.reset_index(drop=drop)
    
def pivot_data(df, index, columns, values, aggfunc='mean'):
        """
        Create a pivot table.
        
        :param df: DataFrame
        :param index: Column to use as the pivot table index
        :param columns: Column to use as the pivot table columns
        :param values: Column(s) to aggregate
        :param aggfunc: Aggregation function. Default is 'mean'.
        :return: Pivot table as a DataFrame
        """
        return pd.pivot_table(df, index=index, columns=columns, values=values, aggfunc=aggfunc)        
