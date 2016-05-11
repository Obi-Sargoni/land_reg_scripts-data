# providers_lookup version 3
# apply cleaning functions to landregistry and providers and run single matching process
# run fuzzy matching process to add to provider lookup
# be able to write and read lookup from excel file

import pandas as pd
import numpy as np
import fuzzywuzzy
from fuzzywuzzy import fuzz
import logging
import random
import os
import ocsipy
import lr_match
import landregreporting
import landreggeocoding
import landregcodex
import openpyxl
import time


###### INITIALISE LOGGING ######

logfile = 'full run match report 7b.txt'
logging.basicConfig(filename = logfile,filemode = 'w', level=logging.DEBUG)
#logging.disable(level = logging.DEBUG)
logging.debug('TITLE: User input matches')

################################


###### INITIALISE ORGANISATIONS TO LOOKUP ######

eng_providers_df = pd.read_csv('.\Land registry tables\List_of_Registered_Providers_at_2_February_2016.csv')
eng_providers_names = eng_providers_df.reindex(columns = ['Name of Registered Provider','RP Code'])
eng_providers_key = lr_match.get_key(eng_providers_df, 'Name of Registered Provider', ocsipy.clean_organisation_name, code_col = 'RP Code')

wlsh_providers_df = pd.read_csv('.\Land registry tables\welsh registered providers.csv')
wlsh_providers_names = wlsh_providers_df.reindex(columns = ['Name of Registered Social Landlord', 'Registration Number'])
wlsh_providers_key = lr_match.get_key(wlsh_providers_df, 'Name of Registered Social Landlord', ocsipy.clean_organisation_name, code_col = 'Registration Number')

providers_names_to_codes = dict(eng_providers_key)
providers_names_to_codes.update(wlsh_providers_key)

providers_codes_to_names = dict(zip(providers_names_to_codes.values(), providers_names_to_codes.keys()))

othercos = pd.read_csv('.\Other Companies\Fast Food, Bookies, Loans.csv')
othercos_key = lr_match.get_key(othercos, 'fast_food')

################################################

###### INITIALISE GEOCODING ######

geocode_file = '.\ONSPD_FEB_2016_csv\Data\ONS pcd to lat-long.csv'
pcd_to_ll_df = pd.read_csv(geocode_file)
pcd_to_ll_df['POSTCODE_CLEAN'] = pcd_to_ll_df['pcd'].map(lambda x:landreggeocoding.pcd_clean(x) if pd.isnull(x)==False else x) # can add map in later
pcd_to_ll_df = pcd_to_ll_df.reindex(columns = ['POSTCODE_CLEAN', 'lat', 'long'])

address_columns = [
	'STREET_NAME','STREET_NAME_2','LOCAL_NAME','LOCAL_NAME_2','TOWN_NAME','DISTRICT_NAME',
	'COUNTY_NAME','REGION']

postcode_columns = [
	'POSTCODE','POSTCODE_AREA','POSTCODE_DISTRICT','POSTCODE_SECTOR']

geo_columns = address_columns + ['POSTCODE'] + ['SUGGESTED_POSTCODE', 'POSTCODE_CLEAN', 'NON_PI_NAME']

geo_codex_columns = ['NON_PI_NAME', 'POSTCODE', 'SUGGESTED_POSTCODE']
geo_codex_filename = 'matched_postcodes from single full run codex 2.csv'

##################################

###### 	INITIALISE MATCH VARIABLES ######

names_codex_columns = ['matched_code', 'matched_name', 'number_properties', 'NON_PI_NAME', 'How_Matched', 'total_frequency']
common_words = [
	'THE', 'AND', 'OF', 'HOMES', 'HOUSING', 'ASSOCIATION', 'SOCIETY',
	'TRUST', 'CHARITY', 'DISTRICT', 'GROUP', 'ALMSHOUSE', 'ALMSHOUSES', 'CYFYNGEDIG'
	]

########################################

###### INITIALISE LANDREGISTRY ######

lr_dir = 'C:\Land Registry Data'
lr_dir_contents = os.listdir('C:\Land Registry Data')
lr_path_list = []
for filename in lr_dir_contents:
	lr_path = os.path.join(lr_dir, filename)
	lr_path_list.append(lr_path)

#####################################



#####################################
# run_matching_process()
#
# Reformats land_registry_names DataFrame to include columns used to record matches.
# 	Calls direct_match function. fuzzy_match called if fuzzy_threshold is set. Threshold
#	sets string simalarity score that counts as a match.
#
#####################################
def run_matching_process(land_registry_names, lookup_key,  fuzzy_threshold = 95):
	
	# Add cols to record matches if not already included in land_registry_names DataFrame
	match_cols = list(set(list(land_registry_names.columns) + ['matched_name', 'matched_code', 'How_Matched']))
	landreg_names_matched = land_registry_names.reindex(columns = match_cols)
	
	# Run direct_match
	lr_match.direct_match(landreg_names_matched, 'NON_PI_NAME', 'matched_name', 'How_Matched', lookup_key)
	
	# If fuzzy_threshold is given, run fuzzy_match
	if fuzzy_threshold != None:
		lr_match.fuzzy_match(landreg_names_matched, 'NON_PI_NAME', 'matched_name', 'How_Matched', lookup_key, common_words, fuzzy_threshold)
	lr_match.name_lookup_code(landreg_names_matched, 'matched_name', 'matched_code', lookup_key)
	return landreg_names_matched



#lr_names = lr_match.get_unique_elems_and_freqs_iteratively(lr_path_list, 'NON_PI_NAME')
#lr_names.to_csv('land registry names and frequencies 2.csv', index=False)

# NOTE LR NAMES NOW OUT DATED. NEED TO CREAT ANEW
lr_names = pd.read_csv('land registry names and frequencies 2.csv')

#lr_names_matched = run_matching_process(lr_names, providers_names_to_codes, fuzzy_threshold = 95)

names_codex_filename = 'names_codex from single full match 7.csv'
#names_codex = landregcodex.match_codex_from_df(lr_names_matched, names_codex_columns, 'matched_code', index_col = None, 'NON_PI_NAME')
#names_codex.to_csv(names_codex_filename)
names_codex = pd.read_csv(names_codex_filename)


full_unmatched = pd.read_csv('unmatched names from codex6.csv')
#full_unmatched['NON_PI_NAME'] = full_unmatched['NON_PI_NAME'].map(lambda x : ocsipy.clean_organisation_name(x) if pd.isnull(x)==False else x)

#new_matches = run_matching_process(full_unmatched, providers_names_to_codes, fuzzy_threshold = None)
new_matches = landregcodex.update_matches_with_codex(full_unmatched, names_codex)
new_matches_codex = landregcodex.match_codex_from_df(new_matches, names_codex_columns, 'matched_code', index_col=None, duplicate_col='NON_PI_NAME')

new_names_codex = landregcodex.update_codex_with_matches(names_codex, new_matches_codex)
#new_names_codex = landregcodex.match_codex_from_df(new_names_codex,names_codex_columns, 'matched_code', index_col=None, duplicate_col='NON_PI_NAME')
new_names_codex.to_csv('names_codex from single full match 7b.csv', index=False)


matched_codes = names_codex['matched_code'].unique()
unmatched_providers = landregreporting.get_unmatched_providers(matched_codes, providers_names_to_codes.values(), providers_codes_to_names)
unmatched_providers.to_csv('unmatched registered providers 7b.csv')

new_unmatched = new_matches.loc[~(new_matches['NON_PI_NAME'].isin(new_names_codex['NON_PI_NAME']))]
new_unmatched.to_csv('unmatched names from codex7b.csv', index=False)

landregreporting.match_reportV2(lr_names, new_names_codex, providers_names_to_codes, 'total_frequency', 'matched_code', 'matched_name', 'How_Matched')


# Uses names_codex, what if it hasn't been created?
def run_geocoding(df_to_geocode):
	df_to_geocode = landreggeocoding.suggest_entry_for_missing_val(df_to_geocode, 'POSTCODE', address_columns)
	#df_to_geocode['POSTCODE_CLEAN'] = df_to_geocode['SUGGESTED_POSTCODE'].map(lambda x:landreggeocoding.pcd_clean(x) if pd.isnull(x)==False else x)
	#df_to_geocode = pd.merge(df_to_geocode, pcd_to_ll_df, on = 'POSTCODE_CLEAN', how = 'left')
	return df_to_geocode


def run_geocoding_iteratively(filename_list, match_codex):
	geo_codex = pd.DataFrame()
	for filename in lr_dir_contents:
		lr_path = os.path.join(lr_dir, filename)
		lr_chunker = pd.read_csv(lr_path, chunksize = 100000)
		for lr_chunk in lr_chunker:
			lr_chunk['NON_PI_NAME'] = lr_chunk['NON_PI_NAME'].map(lambda x : ocsipy.clean_organisation_name(x) if pd.isnull(x)==False else x)
			lr_geo_chunk = landreggeocoding.get_landreg_for_geocoding(lr_chunk, match_codex, 'NON_PI_NAME', geo_columns)
			lr_geo_chunk = run_geocoding(lr_geo_chunk)
			geo_chunk_codex = landregcodex.match_codex_from_df(lr_geo_chunk, geo_codex_columns, 'SUGGESTED_POSTCODE')
			geo_codex = pd.concat([geo_codex, geo_chunk_codex], join = 'outer')
	return geo_codex

'''
geo_codex = run_geocoding_iteratively(lr_dir_contents, names_codex)
landregreporting.geocode_report(geo_codex, 'POSTCODE', 'SUGGESTED_POSTCODE')
logging.debug('Shape of final geo codex')
logging.debug(geo_codex.shape)
geo_codex.to_csv(geo_codex_filename)
'''
