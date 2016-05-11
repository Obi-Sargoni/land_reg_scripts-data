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
import sys
import ocsipy
import landregmatching
import landregreporting
import landreggeocoding
import landregcodex
import openpyxl


###### INITIALISE LOGGING ######
logfile = 'geotest 2.txt'
logging.basicConfig(filename = logfile,filemode = 'w', level=logging.DEBUG)
#logging.disable(level = logging.DEBUG)
logging.debug('TITLE: TESTING ALTERNATIVE PCD FILL FUNC')


###### INITIALISE ORGANISATIONS TO LOOKUP ######
providers_df = pd.read_csv('.\Land registry tables\List_of_Registered_Providers_at_2_February_2016.csv')
providers_names = providers_df.reindex(columns = ['Name of Registered Provider','RP Code'])
providers_key = landregmatching.get_key(providers_df, 'Name of Registered Provider', ocsipy.up_sspace_AMP_LTD, code_col = 'RP Code')

othercos = pd.read_csv('.\Other Companies\Fast Food, Bookies, Loans.csv')
othercos_key = landregmatching.get_key(othercos, 'fast_food')


###### INITIALISE GEOCODING ######
geocode_file = '.\ONSPD_FEB_2016_csv\Data\ONS pcd to lat-long.csv'
geocode_PC_LL_df = pd.read_csv(geocode_file)
geocode_PC_LL_df['POSTCODE_CLEAN'] = geocode_PC_LL_df['pcd'].map(lambda x:landreggeocoding.pcd_clean(x) if pd.isnull(x)==False else x) # can add map in later
geocode_PC_LL_df = geocode_PC_LL_df.reindex(columns = ['POSTCODE_CLEAN', 'lat', 'long'])

address_columns = [
	'STREET_NAME','STREET_NAME_2','LOCAL_NAME','LOCAL_NAME_2','TOWN_NAME','DISTRICT_NAME',
	'COUNTY_NAME','REGION','PTY_ADDR']

postcode_columns = [
	'POSTCODE','POSTCODE_AREA','POSTCODE_DISTRICT','POSTCODE_SECTOR']

geo_columns = address_columns + postcode_columns + ['SUGGESTED_POSTCODE', 'POSTCODE_CLEAN', 'NON_PI_NAME']

geo_codex_columns = ['NON_PI_NAME', 'POSTCODE_CLEAN', 'lat', 'long']
geo_codex_filename = 'matched_postcodes_latlong.xlsx'


###### 	INITIALISE MATCH VARIABLES ######
match_codex_filename = 'rp_codes_to_names.xlsx'
match_codex_columns = ['matched_code', 'NON_PI_NAME', 'matched_name', 'How_Matched']
common_words = [
	'LIMITED', 'THE', 'AND', 'OF', 'HOMES', 'HOUSING', 'ASSOCIATION', 'SOCIETY',
	'TRUST', 'CHARITY', 'DISTRICT', 'GROUP', 'ALMSHOUSE', 'ALMSHOUSES'
	]

###### INITIALISE LANDREGISTRY ######
land_registry_df = pd.read_csv('.\Land registry tables\Land registry test data.csv')
land_registry_df['NON_PI_NAME'] = land_registry_df['NON_PI_NAME'].map(lambda x : ocsipy.up_sspace_AMP_LTD(x) if pd.isnull(x)==False else x)
landreg_names = landregmatching.get_names_and_frequencies(land_registry_df, 'NON_PI_NAME')


def run_matching_process(land_registry_names, lookup_key, fuzzy_threshold = 95):
	landreg_names_matched = land_registry_names.copy()
	match_cols = list(land_registry_names.columns) + ['matched_name', 'matched_code', 'How_Matched']
	logging.debug('Number of unmatched entries before replacement: ')
	logging.debug(landreg_names_matched.loc[landreg_names_matched['matched_code'].isnull()].shape)
	landregmatching.direct_match(landreg_names_matched, 'NON_PI_NAME', 'matched_name', 'How_Matched', lookup_key)
	landregreporting.match_report(landreg_names_matched, 'matched_code', 'NON_PI_NAME', 'How_Matched')
	if fuzzy_threshold != None:
		landregmatching.fuzzy_match(landreg_names_matched, 'NON_PI_NAME', 'matched_name', 'How_Matched', lookup_key, common_words, fuzzy_threshold)
		logging.debug(landreg_names_matched.loc[landreg_names_matched['How_Matched']=='fuzzy_match_'+str(fuzzy_threshold), ['NON_PI_NAME', 'matched_name']])
		landregreporting.match_report(landreg_names_matched, 'matched_code', 'NON_PI_NAME', 'How_Matched')
	landregmatching.name_lookup_code(landreg_names_matched, 'matched_name', 'matched_code', lookup_key)
	landregreporting.match_report(landreg_names_matched, 'matched_code', 'NON_PI_NAME', 'How_Matched')
	return landreg_names_matched

#landreg_plus_matches = run_matching_process(landreg_names, providers_key, fuzzy_threshold = None)
#landregcodex.matches_to_excel_codex(landreg_plus_matches, match_codex_columns, 'matched_name', 'NON_PI_NAME', 'matched_code', match_codex_filename)

match_codex = landregcodex.get_codex_from_codex_file(match_codex_filename)
#updated_landreg_plus_matches = landregcodex.update_df_with_codex(landreg_names, match_codex, 'NON_PI_NAME', 'matched_name', 'matched_code', 'How_Matched')
#key_from_file = get_key_from_existing_codex_file(match_codex_filename, 'NON_PI_NAME', 'matched_code')


# Uses names_codex, what if it hasn't been created?
def run_geocoding():
	landreg_geo = landreggeocoding.get_landreg_for_geocoding(land_registry_df, match_codex, 'NON_PI_NAME', geo_columns)
	
	logging.debug('Before postcode fill function')
	landregreporting.geocode_report(landreg_geo, 'POSTCODE', 'POSTCODE_CLEAN')

	landreggeocoding.suggest_missing_entry_values(landreg_geo, 'POSTCODE', address_columns)
	landreg_geo['POSTCODE_CLEAN'] = landreg_geo['SUGGESTED_POSTCODE'].map(lambda x:landreggeocoding.pcd_clean(x) if pd.isnull(x)==False else x)

	logging.debug('After postcode fill function')
	landregreporting.geocode_report(landreg_geo, 'POSTCODE', 'POSTCODE_CLEAN')

	landreg_geo = pd.merge(landreg_geo, geocode_PC_LL_df, on = 'POSTCODE_CLEAN', how = 'left')

	logging.debug('After merge function')
	landregreporting.geocode_report(landreg_geo, 'POSTCODE', 'POSTCODE_CLEAN', 'lat')
	
	return landreg_geo
landreg_geo_matched = run_geocoding()
#landregcodex.matches_to_excel_codex(landreg_geo, geo_codex_columns, 'POSTCODE_CLEAN', 'POSTCODE_CLEAN', 'POSTCODE_CLEAN', geo_codex_filename)

