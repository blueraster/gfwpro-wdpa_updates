# modified from script from_DB_split_list.py to avoid needing to use the sql server. Instead geometry gets loaded from file geodatabase on disk
# 2023 GFW database export script

import sys
import fiona
import config
import logging
import numpy as np
import geopandas as gpd
from shapely import wkb
from pathlib import Path


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
    # Find and read poly layer
    poly_lyr = [l for l in fiona.listlayers(path) if "poly" in l][0]
    gdf = gpd.read_file(path, driver="FileGDB", layer=poly_lyr).set_crs(4326)
    return gdf


def main(path_to_fgdb, list_id):
    """
    Main function to split a list of polygons into a grid
    :param path_to_fgdb: path to the input file geodatabase
    :param list_id: id of the gfwlist
    :return:
    """
    logging.info(f"loading {path_to_fgdb}")
    gdf = loadFGDB(path_to_fgdb)

    logging.info("loaded location_id dataframe")

    gdf["list_id"] = list_id
    gdf["location_id"] = gdf.index

    undiced_out = str(Path(config.OUTPUT_DIR) / "undiced_out.txt")
    logging.info(f"saving to {undiced_out}")
    gdf.to_csv(undiced_out, sep="\t", index=False)

    # print(gdf.head())

    logging.info("creating in memory grid")
    grid = createGrid(degrees=1)

    logging.info("intersection with grid")
    gdf_intersection = gpd.overlay(
        gdf, grid, how="intersection", keep_geom_type=True, make_valid=True
    )

    logging.info("save geometry as wkb")
    gdf_intersection["geom"] = gdf_intersection.geometry.apply(
        wkb.dumps, hex=True
    )  # add in wkb

    diced_out = str(Path(config.OUTPUT_DIR) / "diced_out.txt")
    logging.info(f"saving to {diced_out}")
    gdf_intersection[["list_id", "location_id", "geom"]].to_csv(
        diced_out, sep="\t", index=False
    )

    return


if __name__ == "__main__":
    """This is executed when run from the command line"""

    main(path_to_fgdb=config.INPUT_GDB_PATH, list_id=4)

    logging.info("done")
