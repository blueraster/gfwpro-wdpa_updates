# save_to_database.py

import config
import pandas as pd
import sqlalchemy as sal
import name_conversion as name_conversion


def cleanData(df):
    """
    Performs column name mapping operations

    Args:
        df  : Dataframe containing unaltered output from geotrellis

    Return:
        Dataframe with columns matching Dec 2023 DB schema
    """

    # Drop extra cols
    df = df.drop(
        [
            "location_error",
            "tree_cover_loss_arg_otbn_yearly",
            "tree_cover_extent_primary_forest",
            "protected_areas_by_category_area",
            "landmark_by_category_area",
        ],
        axis=1,
    )

    # Rename cols
    df = df.rename(columns=name_conversion.analysis_cols_dict)

    # Add missing cols
    for col in list(name_conversion.analysis_schema_dict.keys()):
        if col not in df.columns:
            df[col] = None

    # Reorder columns
    df = df[list(name_conversion.analysis_schema_dict.keys())]

    return df


def boolConvert(df):
    """
    Converts boolean values to text equivalents

    Args:
        df  : Dataframe with boolean values and columns matching DB schema

    Return:
        Dataframe with boolean values replaced with text equivalents
    """

    mask = df.applymap(type) != bool
    d = {True: "true", False: "false"}
    df = df.where(mask, df.replace(d))

    return df


def nvarcharConvert(df):
    """
    Converts null data in columns that will be NVARCHAR to default "{}"

    Args:
        df  : Dataframe with colummns matching DB schema

    Return:
        Dataframe with no "NULL" values in cols that will be NVARCHAR in the DB
    """

    # Create a list of columns that contain NVARCHAR datatype
    nvarchar_cols = [
        k
        for k, v in name_conversion.analysis_schema_dict.items()
        if "NVARCHAR" in str(v)
    ]

    # Replace None values with {} in NVARCHAR columns
    for column in nvarchar_cols:
        df[column].fillna("{}", inplace=True)

    return df


if __name__ == "__main__":
    """This is executed when run from the command line"""

    # Connect to the database
    conn_string = config.SAL_STRING
    engine = sal.create_engine(conn_string)
    conn = engine.connect()
    print("Connected to DB")

    # Read csv to df
    df = pd.read_csv(config.INPUT_GDB_PATH, sep="\t")
    print("Read input data")

    # Clean input_df
    df = cleanData(df)
    print("Cleaned dataframe columns")

    # Replace boolean values with string
    df = boolConvert(df)
    print("Converted bool values")

    # Replace null nvarchar values with {}
    df = nvarcharConvert(df)
    print("Converted nvarchar values")

    # Save to database
    print("Saving dataframe to database")
    df.to_sql(
        "analysis-protectedAreas_import",
        conn,
        index=False,
        if_exists="replace",
        dtype=name_conversion.analysis_schema_dict,
    )

    print("Done")
