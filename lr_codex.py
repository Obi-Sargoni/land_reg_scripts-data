# function used to create and handle codex dataframes and codex files
import pandas as pd
import openpyxl

def match_codex_from_df(df_in, codex_columns, code_col, index_col=None, duplicate_col = None):
	df = df_in.loc[df_in[code_col].notnull()]
	df = df.reindex(columns = codex_columns)
	provider_frequencies = df['total_frequency'].groupby(df['matched_code']).sum()
	provider_frequencies_dict = provider_frequencies.to_dict()
	df['number_properties'] = df['matched_code'].copy().replace(provider_frequencies_dict)
	if duplicate_col != None:
		df.drop_duplicates([duplicate_col], inplace=True) # Do we need to specify a duplicate col, or just drop any dup. rows
	if index_col != None:
		df.set_index(index_col, inplace = True)
	df.sortlevel(0, inplace=True)
	return df

def matches_to_excel_codex(df_in, codex_columns, index_col, duplicate_col, code_col, filename):
	codex = match_codex_from_df(df_in, codex_columns, index_col, duplicate_col, code_col)
	codex_writer = pd.ExcelWriter(filename, engine = 'openpyxl')
	codex.to_excel(codex_writer, sheet_name = 'sheet1')
	# Add some formatting
	codex_writer.save()
	
def codex_from_excel(filename):
	codex_workbok = pd.ExcelFile(filename)
	df = pd.read_excel(codex_workbok, 'sheet1', index_col = 0)
	return df
	
def codex_to_excel(codex, filename):
	codex_writer = pd.ExcelWriter(filename, engine = 'openpyxl')
	names_codex.to_excel(codex_writer, sheet_name = 'sheet1')
	codex_writer.save()

# Might not want to use thia as it brings through a key without updating the matching_df.
def get_key_from_existing_codex_file(filename, key_col, code_col):
	codex_df = get_codex_from_codex_file(filename)
	key = landregmatching.get_key(codex_df, key_col, None, code_col)
	return key

def get_codex_from_codex_file(filename):
	codex_df = codex_from_excel(filename)
	codex_df.reset_index(level = 0, inplace = True)
	return codex_df

# Only brings in user input change if name is.t in landreg NON_PI_NAME list
def update_matches_with_codex(df_in, codex, name_col = 'NON_PI_NAME', match_col='matched_name', code_col='matched_code', info_col='How_Matched'):
	df = df_in.copy()
	cols = list(set(list(df.columns) + [match_col, code_col, info_col]))
	df = df.reindex(columns = cols)
	additional_matches = df.loc[(df[name_col].isin(codex[name_col])) & (df[match_col].isnull()), name_col]
	incorrect_matches = codex.loc[codex[info_col]=='INCORRECT_MATCH', name_col] # select the matches flagged as incorrect.
	print additional_matches
	print incorrect_matches
	for name in additional_matches:
		df.loc[df[name_col]==name, [match_col, code_col, info_col]] = [codex.loc[codex[name_col]==name, match_col].values[0], codex.loc[codex[name_col]==name, code_col].values[0], codex.loc[codex[name_col]==name, info_col].values[0]]
	return df

####
# NOTE FREQUENCY CALCULATOR CODE IS REPEATE. NOT NEEDED
def update_codex_with_matches(codex, codex_update):
	new_codex = pd.concat([codex, codex_update], join = 'outer')
	new_codex.drop_duplicates(inplace=True)
	# Drop entries with no frequency entry
	new_codex = new_codex.loc[new_codex['total_frequency'].notnull()]
	provider_frequencies = new_codex['total_frequency'].groupby(new_codex['matched_code']).sum()
	provider_frequencies_dict = provider_frequencies.to_dict()
	new_codex['number_properties'] = new_codex['matched_code'].copy().replace(provider_frequencies_dict)
	print 'Duplicated NON_PI_NAMEs'
	print new_codex['NON_PI_NAME'].duplicated().value_counts()
	return new_codex
