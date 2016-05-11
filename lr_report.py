import pandas as pd
import numpy as np
import fuzzywuzzy
from fuzzywuzzy import fuzz
import ocsipy
import logging
import random
import sys

# Function for reporting land registry name matching process

def match_report(df, match_dict, freq_col, code_col, match_col, info_col):
	logging.debug('\n')
	logging.debug('True gives number of matched names within df after replacement: ')
	logging.debug(df[code_col].notnull().value_counts())
	logging.debug('Number of matched organisations, determined by number of unique rp_codes:')
	logging.debug(len(df.loc[df[code_col].notnull(), code_col].unique()))
	logging.debug('Types of matches mades')
	report_series = df[info_col].value_counts()
	logging.debug(report_series)
	logging.debug('Shape of resulting df:')
	logging.debug(df.shape)
	logging.debug('\n')
	logging.debug('Calculate proportion of LR entries matched')
	logging.debug('Total number of entries:')
	logging.debug(df[freq_col].sum())
	logging.debug('Number og matched entries:')
	logging.debug(df.loc[df[code_col].notnull(), freq_col].sum())
	logging.debug('Percentage:')
	logging.debug((df.loc[df[code_col].notnull(), freq_col].sum())/(df[freq_col].sum()))
	logging.debug('\n')
	logging.debug('Proportion of registered providers located:')
	logging.debug((len(df[match_col].isin(match_dict.keys())))/(len(match_dict.keys())))
	logging.debug('\n')

def match_reportV2(df, codex_with_incorrect_matches, match_dict, freq_col, code_col, match_col, info_col):
	#df = df_in.loc[df_in[info_col]!='INCORRECT_MATCH']
	codex = codex_with_incorrect_matches.loc[codex_with_incorrect_matches[info_col]!='INCORRECT_MATCH']
	logging.debug('\n')
	logging.debug('Total number of names:')
	logging.debug(df.shape[0])
	logging.debug('Number of matched names within df after replacement: ')
	logging.debug(codex.shape[0])
	logging.debug('Number of matched organisations, determined by number of unique registered providers:')
	logging.debug(len(codex[code_col].unique()))
	logging.debug('Number of entries flagged as incorrect matches:')
	logging.debug(codex_with_incorrect_matches[info_col].value_counts()['INCORRECT_MATCH'])
	logging.debug('Types of matches mades')
	logging.debug(codex[info_col].value_counts())
	logging.debug('\n')
	logging.debug('Calculate proportion of LR entries matched')
	logging.debug('Total number of entries:')
	logging.debug(df[freq_col].sum())
	logging.debug('Number og matched entries:')
	logging.debug(codex[freq_col].sum())
	logging.debug('Percentage:')
	logging.debug((codex[freq_col].sum())/(df[freq_col].sum()))
	logging.debug('\n')
	logging.debug('Number of registered providers located:')
	logging.debug(len(codex[match_col].unique()))
	logging.debug('Out of a total of: ')
	logging.debug(len(match_dict.keys()))
	logging.debug('\n')

def top_n_unmatched(df, n=5, code_col='matched_code', freq_col='total_frequency'):
	logging.debug(df.loc[df[code_col].isnull()].sort_index(by = freq_col)[-n:])
	return df.loc[df[code_col].isnull()].sort_index(by = freq_col)[-n:]

def threshold_unmatched(df, freq_threshold, code_col='matched_code', freq_col='total_frequency'):
	return df.loc[(df[code_col].isnull()) & (df[freq_col]>freq_threshold)].sort_index(by = freq_col)
	
def land_proportion_matched(df, code_col='match_code', freq_col='total_frequency'):
	number_properties_matched = df.loc[df[code_col].notnull(), freq_col].sum()
	total_properties = df[freq_col].sum()
	info = [['Number Properties Matched', 'Total Number Properties'], [number_properties_matched, total_properties]]
	return info

def quantile_unmatched(df_in, q, code_col, freq_col):
	df = df_in.sort_values(by=freq_col, ascending = False)
	sub_df = df.loc[df[code_col].isnull(), freq_col]
	threshold = (sub_df.sum())*q
	i=0
	running_total = 0
	while running_total < threshold:
		running_total = running_total + sub_df.iloc[i] # iloc because we are using a mixture of index and lable indexing
		i = i+1
	quantile_index = i-1
	selection = df[df[code_col].isnull()].iloc[0:i] # works, returns copy not view. no warning given,
	return selection

def get_unmatched_providers(matched_codes, rp_codes, codes_to_names_dict):
	unmatched_codes = list(set(rp_codes) - set(matched_codes))
	um_s = pd.Series(unmatched_codes)
	um_s.name = 'unmatched_codes'
	um_df = pd.DataFrame(um_s)
	um_df['unmatched_names']=um_df['unmatched_codes'].copy()
	um_df['unmatched_names'].replace(codes_to_names_dict, inplace=True)
	return um_df
	
def match_examples_by_match_type(df, how_matched_col, code_col, names, match_names, num_of_examples):
	df = df.reindex(columns = [names, match_names, code_col, how_matched_col])
	match_types = df[how_matched_col].unique()
	for match_type in match_types:
		if match_type == 'No Match':
			continue
		
		grouped = df.loc[df[how_matched_col] == match_type].groupby(code_col).filter(lambda x: len(x) >1).groupby(code_col)
		group_keys = grouped.groups.keys() # list of keys
		
		codes = random.sample(group_keys, min(num_of_examples, len(group_keys)))
		
		logging.debug('Example of '+ match_type + ' matches:')
		for code in codes:
			logging.debug(grouped.get_group(code))
		logging.debug('\n')

def match_examples_by_match_code(df, how_matched_col, code_col, names, match_names, num_of_examples):
	df = df.reindex(columns = [names, match_names, code_col, how_matched_col])
		
	grouped = df.groupby(code_col).filter(lambda x: len(x) >1).groupby(code_col)
	group_keys = grouped.groups.keys() # list of keys
	
	codes = random.sample(group_keys, min(num_of_examples, len(group_keys)))
	
	for code in codes:
		logging.debug(grouped.get_group(code))
	logging.debug('\n')

def n_most_frequent_words(df, col, n):
	# Get list of all words in name column
	word_array = []
	for index, item in df.loc[df[col].notnull(), col].iteritems():
		try:
			word_array = word_array + item.split()
		except:
			print item
	# Find most common elements in that list
	word_series = pd.Series(word_array)
	n_most_frequent = list(word_series.value_counts().iloc[0:n-1].index)
	return n_most_frequent
	
def geocode_report(codex, original_pcd_col, processed_pcd_col, lat_col = None):
	logging.debug('Shape of geo codex')
	logging.debug(codex.shape)

	logging.debug('True gives number of not null entires in original pcd col.')
	logging.debug((codex[original_pcd_col].notnull()).value_counts())

	logging.debug('True gives numbers of processed postcodes')
	logging.debug((codex[processed_pcd_col].notnull()).value_counts())
	if lat_col != None:
		logging.debug('True gives number of latitudes')
		logging.debug(((codex[lat_col].notnull()).value_counts()))
		logging.debug('Entries where postcode did not match with lat-long:')
		logging.debug(codex.loc[(codex[processed_pcd_col].notnull()) & (codex[lat_col].isnull())])
