# Module for geocoding matched land registry postcodes
import pandas as pd
import numpy as np
from numpy import nan
import itertools
import csv
import logging
import time

def get_landreg_for_geocoding(land_registry_df, codex, name_col, geo_cols):
	# Select elements within land_registry_df that have been matched
	landreg_geo = land_registry_df.loc[land_registry_df[name_col].isin(codex[name_col])]
	landreg_geo = landreg_geo.reindex(columns = geo_cols)
	return landreg_geo

# Given location of missing post code entry, searches nearby entries for same addresses to fill postcode from
def suggest_entry_for_missing_val(df, column_to_fill, columns_matched_on):
	old_indices = df.index
	df.set_index(np.arange(df.shape[0]), inplace=True)
	df['SUGGESTED_'+column_to_fill]=df[column_to_fill]
	for row_index,row in df.iterrows():
	# Selects the elements of DataFrame row with missing entry in 'column_to_fill' to be matched against other rows
		if pd.isnull(row[column_to_fill]):
			elements_to_compare = row.reindex(columns_matched_on)
			# Selects elements of neighbouring entries for comparison. Break stops loop once match is found.
			for i in range(max(0,row_index-2),row_index) + range(min(len(df),row_index+1), min(len(df),row_index+3)):	
				comparator = (df.ix[i]).reindex(columns_matched_on)
				if elements_to_compare.equals(comparator):
					df.ix[row_index,'SUGGESTED_'+column_to_fill]= df.ix[i,column_to_fill]
					break
	df.set_index(old_indices, inplace=True)
	return df

#Not currently used
def suggest_missing_entry_values(df, column_to_fill, columns_matched_on):
	old_indices = df.index
	df.set_index(np.arange(df.shape[0]), inplace=True)
	df['SUGGESTED_'+column_to_fill]=df[column_to_fill]
	sub_df = df.loc[df[column_to_fill].isnull()] # This line does not work. Works whens used in separate function (see below) but position in this func is incorrect
	for row_index, row in sub_df.iterrows():
		elements_to_compare = row.reindex(columns_matched_on)
		# Selects elements of neighbouring entries for comparison. Break stops loop once match is found.
		for i in range(max(0,row_index-2),row_index) + range(min(len(df),row_index+1), min(len(df),row_index+3)):	
			comparator = (df.ix[i]).reindex(columns_matched_on)
			if elements_to_compare.equals(comparator):
				df.ix[row_index,'SUGGESTED_'+column_to_fill]= df.ix[i,column_to_fill]
				break
	df.set_index(old_indices, inplace=True)

def get_sub_df(df, col):
	sub_df = df.loc[df[col].isnull()]
	return sub_df
	
def pcd_clean(string):
	try:
		string=str(string)
		string=string.upper()
		string=string.replace(' ' ,'')
		return string
	except AttributeError:
		print('Attribute Error when cleaning the following input to pcd_clean: ' + string)
		return string
	except (UnicodeDecodeError, UnicodeEncodeError):
		print('Unicode Error when cleaning the input to pcd_clean')
		return string
	except:
		print('Unexpected error when cleaning string', sys.exc_info()[0])
		return string
		raise