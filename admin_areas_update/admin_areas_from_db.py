import numpy as np
import pandas as pd
import geopandas as gpd
import sqlalchemy as sal
from shapely import wkb
import admin_config as cfg

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

def convertWKBforGPD(dataframe, columnName):
    """
    Converts pandas dataframe with well known binary into a geopandas dataframe

    Uses shapely wkb.loads to convert original geometry into geopandas format

    :param dataframe: pandas dataframe
    :param columnName: column name that contains the WKB
    :return: geopandas dataframe
    """
    try:
        dataframe["geometry"] = dataframe[columnName].apply(wkb.loads, hex=True)
        df = gpd.GeoDataFrame(dataframe, geometry="geometry").set_crs(4326)
    except Exception as e:
        print(e)
        raise ValueError(e)
    return df

def main(connection, current_table, save_geojson=False):
    """
    Main function to split a list of polygons into a grid
    :param filename:
    :param save_geojson:
    :return:
    """
    print(f'working on {current_table}')

    sql = f"SELECT location_id, CONVERT(VARCHAR(MAX), Geometry.STAsBinary(), 2) AS [geom] FROM dbo.[{current_table}]"
    df = pd.read_sql(sql, connection)
    print("loaded location_id dataframe")

    df['list_id'] = 1
    
    print("converting to geopandas")
    gdf = convertWKBforGPD(df, 'geom')
    gdf.drop(columns=['geom'], inplace=True)
    #print(gdf.head())

    print("creating in memory grid")
    grid = createGrid(degrees=1)

    print("intersection with grid")
    gdf_intersection = gpd.overlay(gdf, grid, how="intersection", keep_geom_type=True, make_valid=True)
    # print(gdf_intersection.head())
    # print(gdf_intersection.columns)

    print("save geometry as wkb")
    gdf_intersection["geom"] = gdf_intersection.geometry.apply(wkb.dumps, hex=True)  # add in wkb

    print("saving to csv")
    gdf_intersection[["list_id", "location_id", "geom"]].to_csv(f"diced_{current_table}", sep="\t", index=False)

    if save_geojson==True:
        print("saving to geojson")
        gdf_intersection.to_file("diced.geojson", driver="GeoJSON")

    return

if __name__ == "__main__":
    """ This is executed when run from the command line """
    
    engine = sal.create_engine(cfg.sal_string)
    conn = engine.connect()
    print("connected")

    # administrative areas only
    main(conn, "administrativeAreas")

    print("done")