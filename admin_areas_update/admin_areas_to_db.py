import os
import pandas as pd
import sqlalchemy as sal
import admin_config as cfg

# Data cleaning
def cleanData(new_dataframe, location_id_dataframe, column_name_dataframe):
    # Find extra location_ids
    old_locs = set(location_id_dataframe['location_id'])
    new_locs = set(new_dataframe['location_id'])
    extra_locs = new_locs - old_locs

    # Remove extra rows
    output_dataframe = new_dataframe[~new_dataframe['location_id'].isin(extra_locs)]

    # Drop extra cols
    output_dataframe = output_dataframe.drop(['list_id', 'location_error'], axis=1)

    # Rename cols
    output_dataframe = output_dataframe.rename({'status_code':'location_status_id'}, axis=1)

    # Create id col
    output_dataframe.insert(0, 'id', output_dataframe.index)

    # Get missing cols
    old_cols = set(column_name_dataframe.columns.values.tolist())
    new_cols = set(output_dataframe.columns.values.tolist())
    missing_cols = old_cols - new_cols

    # Add missing cols
    for col in missing_cols:
        output_dataframe[col] = None

    # Reorder columns
    output_dataframe = output_dataframe[list(columnDataType().keys())]
    
    return(output_dataframe)

# Replaces (bool) True with (str) 'true' 
def boolConvert(clean_dataframe):
    mask = clean_dataframe.applymap(type) != bool
    d = {True: 'true', False: 'false'}
    clean_dataframe = clean_dataframe.where(mask, clean_dataframe.replace(d))

    return(clean_dataframe)

# Replace empty nvarchar with  {}
def cleanNvarchar(cleaned_dataframe):
    # Create a list of columns that contain NVARCHAR datatype
    nvarchar_cols = [k for k, v in columnDataType().items() if 'NVARCHAR' in str(v)]

    # Replace None values with {} in NVARCHAR columns
    for column in nvarchar_cols:
        cleaned_dataframe[column].fillna(r'{}', inplace=True)

    return(cleaned_dataframe)

# Save to db
def saveToDatabase(final_dataframe, tableName):
    final_dataframe.to_sql(f'{tableName}_new', conn, index=False, if_exists='replace', dtype=columnDataType())

# Returns dictionary with column names and data types
# Update this is there if column names change or are added
def columnDataType():
    db_schema = {
                'id'                                                 : sal.INTEGER,
                'location_id'                                        : sal.INTEGER,
                'location_status_id'                                 : sal.INTEGER,
                'tree_cover_loss_total_yearly'                       : sal.NVARCHAR(length=None),
                'tree_cover_loss_primary_forest_yearly'              : sal.NVARCHAR(length=None),
                'tree_cover_loss_peat_yearly'                        : sal.NVARCHAR(length=None),
                'tree_cover_loss_intact_forest_yearly'               : sal.NVARCHAR(length=None),
                'tree_cover_loss_protected_areas_yearly'             : sal.NVARCHAR(length=None),
                'tree_cover_loss_sea_landcover_yearly'               : sal.NVARCHAR(length=None),
                'tree_cover_loss_idn_landcover_yearly'               : sal.NVARCHAR(length=None),
                'tree_cover_loss_idn_primary_forest_yearly'          : sal.NVARCHAR(length=None),
                'tree_cover_loss_soy_yearly'                         : sal.NVARCHAR(length=None),
                'tree_cover_loss_idn_legal_yearly'                   : sal.NVARCHAR(length=None),
                'tree_cover_loss_idn_forest_moratorium_yearly'       : sal.NVARCHAR(length=None),
                'tree_cover_loss_prodes_yearly'                      : sal.NVARCHAR(length=None),
                'tree_cover_loss_prodes_wdpa_yearly'                 : sal.NVARCHAR(length=None),
                'tree_cover_loss_prodes_primary_forest_yearly'       : sal.NVARCHAR(length=None),
                'tree_cover_loss_brazil_biomes_yearly'               : sal.NVARCHAR(length=None),
                'tree_cover_extent_total'                            : sal.DECIMAL(18, 6),
                'tree_cover_extent_primary_forest'                   : sal.DECIMAL(18, 6),
                'tree_cover_extent_protected_areas'                  : sal.DECIMAL(18, 6),
                'tree_cover_extent_peat'                             : sal.DECIMAL(18, 6),
                'tree_cover_extent_intact_forest'                    : sal.DECIMAL(18, 6),
                'natural_habitat_primary'                            : sal.DECIMAL(18, 6),
                'natural_habitat_intact_forest'                      : sal.DECIMAL(18, 6),
                'natural_habitat_global_land_cover'                  : sal.NVARCHAR(length=None),
                'total_area'                                         : sal.DECIMAL(18, 6),
                'protected_areas_area'                               : sal.DECIMAL(18, 6),
                'critical_habitat_alliance_zero'                     : sal.DECIMAL(18, 6),
                'indigenous_people_land_rights'                      : sal.DECIMAL(18, 6),
                'peat_area'                                          : sal.DECIMAL(18, 6),
                'fires_counts_monthly'                               : sal.NVARCHAR(length=None),
                'soy_total_area'                                     : sal.NVARCHAR(length=None),
                'soy_pre2001_nonforest_area'                         : sal.DECIMAL(18, 6),
                'brazil_biomes'                                      : sal.NVARCHAR(length=None),
                'idn_legal_area'                                     : sal.NVARCHAR(length=None),
                'sea_landcover_area'                                 : sal.NVARCHAR(length=None),
                'idn_landcover_area'                                 : sal.NVARCHAR(length=None),
                'idn_forest_moratorium_area'                         : sal.DECIMAL(18, 6),
                'south_america_presence'                             : sal.NVARCHAR(length=None),
                'legal_amazon_presence'                              : sal.NVARCHAR(length=None),
                'brazil_biomes_presence'                             : sal.NVARCHAR(length=None),
                'cerrado_biome_presence'                             : sal.NVARCHAR(length=None),
                'southeast_asia_presence'                            : sal.NVARCHAR(length=None),
                'indonesia_presence'                                 : sal.NVARCHAR(length=None),
                'commodity_threat_deforestation_level'               : sal.NVARCHAR(length=None),
                'commodity_threat_peat_level'                        : sal.NVARCHAR(length=None),
                'commodity_threat_protected_areas_level'             : sal.NVARCHAR(length=None),
                'commodity_threat_fires_level'                       : sal.NVARCHAR(length=None),
                'commodity_threat_overall_level'                     : sal.NVARCHAR(length=None),
                'commodity_threat_overall_score'                     : sal.NVARCHAR(length=None),
                'commodity_value_forest_extent_level'                : sal.NVARCHAR(length=None),
                'commodity_value_peat_level'                         : sal.NVARCHAR(length=None),
                'commodity_value_protected_areas_level'              : sal.NVARCHAR(length=None),
                'commodity_value_overall_level'                      : sal.NVARCHAR(length=None),
                'commodity_value_overall_score'                      : sal.NVARCHAR(length=None),
                'commodity_value_forest_extent'                      : sal.NVARCHAR(length=None),
                'commodity_value_peat'                               : sal.NVARCHAR(length=None),
                'commodity_value_protected_areas'                    : sal.NVARCHAR(length=None),
                'commodity_mill_priority_level'                      : sal.NVARCHAR(length=None),
                'commodity_mill_priority_score'                      : sal.NVARCHAR(length=None),
                'commodity_threat_deforestation'                     : sal.NVARCHAR(length=None),
                'commodity_threat_peat'                              : sal.NVARCHAR(length=None),
                'commodity_threat_protected_areas'                   : sal.NVARCHAR(length=None),
                'commodity_threat_fires'                             : sal.NVARCHAR(length=None),
                'dissolved_data_location_info'                       : sal.NVARCHAR(length=None),
                'tree_cover_loss_arg_otbn_yearly'                    : sal.NVARCHAR(length=None),
                'arg_otbn_area'                                      : sal.NVARCHAR(length=None),
                'argentina_presence'                                 : sal.NVARCHAR(length=None)
                }
    return(db_schema)

# Performs dataframe operations
def main(connection, current_table, in_file):
    print(f'working on {current_table}')

    # Get original col names
    col_sql = f"SELECT TOP (1) * FROM dbo.[{current_table}]"
    col_df = pd.read_sql(col_sql, connection)
    print("loaded column name dataframe")

    # Get original location_ids
    loc_sql = f"SELECT [location_id] FROM dbo.[{current_table}]"
    loc_df = pd.read_sql(loc_sql, connection)
    print("loaded location_id dataframe")

    # Read new data from .tsv
    new_df = pd.read_csv(in_file, sep='\t')
    print("loaded new data dataframe")

    # Clean new_df
    clean_df = cleanData(new_df, loc_df, col_df)
    print("cleanData() complete")

    # Replace boolean values with string
    clean_df = boolConvert(clean_df)
    print("boolConvert() complete")

    # Replaces None values with {} in NVARCHAR data type cols
    clean_df = cleanNvarchar(clean_df)
    print("cleanNvarchar() complete")

    print("saving dataframe to database")
    saveToDatabase(clean_df, current_table)

if __name__ == "__main__":
    """ This is executed when run from the command line """

    engine = sal.create_engine(cfg.sal_string)
    conn = engine.connect()
    print("connected to db")
    
    file_path = os.path.join("data", "admin_area_results.csv")
    main(conn, 'analysis-administrativeAreas', file_path)

    print("done")