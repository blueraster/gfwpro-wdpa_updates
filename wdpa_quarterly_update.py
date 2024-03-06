import geopandas as gpd
import pyodbc
import logging
import pandas as pd
import config

import warnings

import logging
import sys

# Set up the basic configuration for logging to stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)

# Create a FileHandler to log messages to a file
file_handler = logging.FileHandler("logfile.txt")  # Specify the file name here

# Set the logging level for the file handler (if different from the basic configuration)
file_handler.setLevel(logging.INFO)

# Create a formatter for the file handler
file_formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s", datefmt="%y-%m-%d %H:%M:%S"
)

# Set the formatter for the file handler
file_handler.setFormatter(file_formatter)

# Add the file handler to the root logger
logging.getLogger().addHandler(file_handler)

# Suppress the specific UserWarning about geographic CRS
warnings.filterwarnings("ignore", category=UserWarning, module="shapely")


def wdpa_renameColumns(df):
    """
    Rename columns in a dataframe
    :param df: input dataframe
    :param rename_dict: dictionary of old column name to new column name
    :return: dataframe with renamed columns
    """

    # rename columns
    rename_dict = {
        "NAME": "Location Name",
        "GIS_AREA": "Size",
        "IUCN_CAT": "IUCN CAT",
        "DESIG_TYPE": "Designation Type",
        "DESIG_ENG": "Designation",
        "name_0": "Country",
        "name_1": "State",
        "name_2": "Sub State",
    }

    df = df.rename(columns=rename_dict)
    df["id"] = df["location_id"]  # also use location_id as id calculated from objectid

    # get centroid
    df["Longitude"] = df.centroid.x
    df["Latitude"] = df.centroid.y

    # df["Geometry"] = df.geometry.to_wkt(
    #     output_dimension=2
    # )
    df["Location Classification"] = ""

    return df


def loadGFWList(path, start, end, connection):

    gdf = gpd.read_file(path, driver="FileGDB", layer=0, rows=slice(start, end))

    gdf = wdpa_renameColumns(gdf)

    e = insert_geometry_WDPA(gdf, connection)

    return e


def insert_geometry_WDPA(gdf, connection_string):
    """
    Large list processing - insert new locations into locations table
    """

    table_name = "list-protectedAreas_import"
    analysis_table_name = "analysis-protectedAreas_import"
    columns = [
        "Location Name",
        "Latitude",
        "Longitude",
        "Country",
        "State",
        "Sub State",
        "Size",
        "IUCN CAT",
        "Designation Type",
        "Designation",
        "Location Classification",
        "gadmid",
        "globalid",
        "location_id",
        "id",
        "WDPAID",
        "WDPA_PID",
        "Geometry",
    ]

    locations_objectid_list = gdf["id"].tolist()

    # get database connection info from SSM

    connection = pyodbc.connect(connection_string)

    gdf.geometry = gdf.buffer(0)

    try:
        gdf["Geometry"] = gdf.geometry.to_wkt(
            output_dimension=2
        )  # convert geometry to wkt (2D)

        # select columns to keep (Geometry should be last in the list)
        common_columns = list(
            gdf.columns.intersection(set(columns))
        )  # selects columns that are in the list provided
        common_columns.sort(
            key="Geometry".__eq__
        )  # https://stackoverflow.com/questions/20320702/how-do-i-move-an-element-in-my-list-to-the-end-in-python
        new_locations = list(gdf[common_columns].itertuples(index=False, name=None))

        with connection:
            cursor = connection.cursor()
            cursor.fast_executemany = True
            column_name_string = ",".join([f'"{x}"' for x in common_columns])
            value_marks = ",".join(
                [f"?" for x in common_columns[:-1]]
            )  # ? per attribute minus 1 for geometry (special case)

            value_marks = value_marks + "," + "geometry::STGeomFromText(?, 4326)"
            sql = f"INSERT into [dbo].[{table_name}] ({column_name_string}) VALUES ({value_marks})"
            inputSizes = [None for c in common_columns[:-1]]
            inputSizes.append((pyodbc.SQL_WVARCHAR, 0, 0))
            cursor.setinputsizes(inputSizes)
            cursor.executemany(sql, new_locations)

        # insert location_ids into analysis table
        gdf["location_status_id"] = 1
        columns = ["location_id", "location_status_id"]
        t = list(gdf[columns].itertuples(index=False, name=None))

        with connection:
            cursor = connection.cursor()
            cursor.fast_executemany = True
            column_name_string = ",".join([f'"{x}"' for x in columns])
            value_marks = ",".join([f"?" for x in columns])
            sql = f"INSERT into [dbo].[{analysis_table_name}] ({column_name_string}) VALUES ({value_marks})"
            cursor.executemany(sql, t)

        logging.info(f"Adding {len(t)} empty rows to {analysis_table_name} finished")
        error_file = None
        return []
    except Exception as e:
        logging.exception(e)
        return locations_objectid_list  # return list of failed locations


if __name__ == "__main__":
    """This is executed when run from the command line"""
    fgdb = r"D:\data\WDPA_Quarterly_Update\data\2023_Q4\wdpa_2023_q4.gdb"
    connection = config.conn_string

    failed = []

    batch_size = 1000
    for i in range(0, 276000, batch_size):
        logging.info(f"Loading {i} to {i+batch_size}")
        try:
            e = loadGFWList(fgdb, i, i + batch_size, connection)
            if len(e) > 0:
                failed.extend(e)
                logging.info(f"Failed {len(e)}")
        except Exception as e:
            print(e)
            logging.exception(e)
            continue
