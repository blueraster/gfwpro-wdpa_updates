# modified from scrip from_DB_split_list.py to avoid needing to use the sql server. Instead geometry gets loaded from file geodatabase on disk
# 2023 GFW database export script

import os
import sys
import logging
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely import wkb

root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
root.addHandler(handler)


def createGrid(degrees=1.0):
    """
    Create a Geopandas dataframe fishnet grid
    :param degrees: spacing in decimal degrees
    :return: fishnet
    """
    ulx, uly, lrx, lry = -180, 90, 180, -90

    # resolution 1 degree grid
    xres = degrees
    yres = -degrees

    # half the resolution
    dx = xres / 2
    dy = yres / 2

    # center coordinates
    xx, yy = np.meshgrid(
        np.arange(ulx + dx, lrx + dx, xres),
        np.arange(uly + dy, lry + dy, yres),
    )

    wkts = []
    for x, y in zip(xx.ravel(), yy.ravel()):
        poly_wkt = f"POLYGON (({x - dx} {y - dy}, {x + dx} {y - dy}, {x + dx} {y + dy}, {x - dx} {y + dy}, {x - dx} {y - dy}))"
        wkts.append(poly_wkt)

    s = gpd.GeoSeries.from_wkt(wkts)
    df = gpd.GeoDataFrame(geometry=s, crs="EPSG:4326")
    df["grid"] = df.index
    return df


def loadFGDB(path):
    gdf = gpd.read_file(path, driver="FileGDB", layer=0).set_crs(4326)
    return gdf


def main(path_to_fgdb, list_id, save_geojson=False):
    """
    Main function to split a list of polygons into a grid
    :param path_to_fgdb: path to the input file geodatabase
    :param list_id: id of the gfwlist
    :param save_geojson:
    :return:
    """
    logging.info(f"loading {path_to_fgdb}")
    gdf = loadFGDB(path_to_fgdb)

    logging.info("loaded location_id dataframe")

    gdf["list_id"] = list_id

    # print(gdf.head())

    logging.info("creating in memory grid")
    grid = createGrid(degrees=1)

    logging.info("intersection with grid")
    gdf_intersection = gpd.overlay(
        gdf, grid, how="intersection", keep_geom_type=True, make_valid=True
    )
    print(gdf_intersection.head())
    columns = gdf_intersection.columns
    print(columns)

    logging.info("save geometry as wkb")
    gdf_intersection["geom"] = gdf_intersection.geometry.apply(
        wkb.dumps, hex=True
    )  # add in wkb

    logging.info("saving to csv")
    gdf_intersection[["list_id", "location_id", "geom"]].to_csv(
        "diced_protectedAreas.txt", sep="\t", index=False
    )

    if save_geojson == True:

        logging.info("saving to geojson")
        save_columns = ["list_id", "location_id", "geom", "geometry"]
        cols_to_drop = [c for c in columns if c not in save_columns]
        gdf_intersection = gdf_intersection.drop(columns=cols_to_drop)
        gdf_intersection.to_file("diced.geojson", driver="GeoJSON")

    return


if __name__ == "__main__":
    """This is executed when run from the command line"""

    main(
        r"D:\data\WDPA_Quarterly_Update\data\2023_Q4\wdpa_2023_q4.gdb",
        4,
        save_geojson=False,
    )

    logging.info("done")
