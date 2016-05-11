###############################
#
# lr_match.py
#
# Library of data reformatting and string matching functions
# 	used for matching strings between two datasets.
#
##############################

import pandas as pd
import numpy as np
import fuzzywuzzy
from fuzzywuzzy import fuzz
import ocsipy
import logging
import random

##############################
#
# get_unique_elems_and_freqs()
#
# From df_in input DataFrame returns DataFrame of the unique elements
#	in elems_col and the number of occurences of those elements.
#
# If set_code == True a code is generated for each unique element
#
##############################
def get_unique_elems_and_freqs(df_in, elems_col, set_code=False):
	
	df = df_in.copy()
	
	# Creates series where unique elems are the index and frequency of each elem in the value
	elems_freq_series = df[elems_col].value_counts()
	elems_freq_series.name = 'frequency'
	elems_df = pd.DataFrame(elems_freq_series)
	
	# Use index of unique elems as values in elems col_to_match
	elems_df[elems_col] = elems_df.index.values
	elems_df.set_index(np.arange(elems_df.shape[0]), inplace=True)
	
	if set_code==True:
		elems_df[elems_col+'_code'] = np.arange(elems_df.shape[0])
		elems_df[elems_col+'_code'] = elems_df[elems_col+'_code'].map(lambda x : elems_col+'_'+str(x))
	
	return elems_df


##############################
#
# get_unique_elems_and_freqs_iteratively()
#
# Builds DataFrame of unique elems and frequencies from 
#	data spread accross several files. Can handle large
#	files without exceeding memory limit.
#
#	string cleaning should be moved to matching function
#
##############################
def get_unique_elems_and_freqs_iteratively(filename_list, elems_col, set_code = False):
	full_elems_df = pd.DataFrame(columns = ['total_frequency', elems_col])
	for filename in filename_list:
		lr_chuncker = pd.read_csv(lr_path, chunksize = 500000)
		for lr_chunk in lr_chuncker:
			lr_chunk['NON_PI_NAME'] = lr_chunk['NON_PI_NAME'].map(lambda x : ocsipy.clean_organisation_name(x) if pd.isnull(x)==False else x)
			lr_chunk_names = get_unique_elems_and_freqs(lr_chunk, 'NON_PI_NAME')
			full_elems_df = pd.merge(full_elems_df, lr_chunk_names, on = ['NON_PI_NAME'], how = 'outer')
			full_elems_df.fillna({'total_frequency':0, 'frequency':0}, inplace=True)
			full_elems_df['total_frequency']=full_elems_df['total_frequency']+full_elems_df['frequency']
			full_elems_df.drop('frequency', axis=1, inplace=True)
	full_elems_df.sort('total_frequency', ascending=False)
	return full_elems_df	

# Updates from V1: allows for input df to already contain a usable key or not
def get_key(df_in, column_to_key,  cleaning_function = None, code_col = None):
	df = df_in.copy()
	if cleaning_function != None:
		df[column_to_key] = df[column_to_key].map(lambda x:cleaning_function(x) if pd.isnull(x)==False else x)
	df.drop_duplicates([column_to_key], inplace=True)
	df = df.loc[df_in[column_to_key].notnull()]
	if code_col == None:
		df = df.reindex(columns = [column_to_key])
		df.set_index(np.arange(df.shape[0]), inplace=True)
		df[column_to_key+'_code'] = df.index.values
		df[column_to_key+'_code'] = df[column_to_key+'_code'].map(lambda x : column_to_key+'_'+str(x))
		df.set_index(column_to_key, inplace=True)
		dict = df[column_to_key+'_code'].to_dict()
	else:
		df = df.reindex(columns = [column_to_key, code_col])
		df.set_index(column_to_key, inplace=True)
		dict = df[code_col].to_dict()
	return dict



def name_lookup_code(df, match_col, code_col, key):
	if code_col not in df.columns:
		df[code_col] = np.nan
	df.loc[(df[code_col].isnull()) & (df[match_col].notnull()), code_col] = df.loc[(df[code_col].isnull()) & (df[match_col].notnull()), match_col].replace(key)

# Could add string cleaning in here, rather than it acting  on root data.
def direct_match(df, col_to_match, match_col, info_col, key):
	df.loc[(df[match_col].isnull()) & (df[col_to_match].isin(key.keys())), [match_col, info_col]] = [df.loc[(df[match_col].isnull()) & (df[col_to_match].isin(key.keys())), col_to_match], 'direct_match']

#######
# ISSUE: Comparison made with providers key. If df (lr_names) contains previous matches these will be rematched leading to duplication in codex
# Duplicates tricky to remove if info_col entry has different value.
def fuzzy_match(df, col_to_match, match_col, info_col, key, common_words, fuzzy_threshold):
	logging.info(fuzzy_threshold)
	unmatched_df = df.loc[(df[match_col].isnull())]
	key_names = key.keys()
	for row_index, row in unmatched_df.iterrows():
		name = unmatched_df.ix[row_index, col_to_match]
		s1 = clean_and_remove_for_fuzz_match(name, common_words)
		fuzzy_matches = []
		for key_name in key_names:
			s2 = clean_and_remove_for_fuzz_match(key_name, common_words)
			distance = fuzz.ratio(s1, s2)
			if distance > fuzzy_threshold: fuzzy_matches.append(key_name)
		if len(fuzzy_matches)==1:
			df.loc[row_index, [match_col, info_col]] = [fuzzy_matches[0], 'fuzzy_match_'+str(fuzzy_threshold)]
		# What to do when there is more than one possible fuzzy match?


def clean_and_remove_for_fuzz_match(string, words_to_remove):
	string = string.replace('\'','')
	string = string.replace('"','')
	str_list = string.split()
	str_list = list(set(str_list).difference(words_to_remove))
	string = ' '.join(str_list)
	return string
