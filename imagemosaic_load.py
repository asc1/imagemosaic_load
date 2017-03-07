import argparse
import glob
import datetime
import logging
import os.path
from osgeo import gdal
from osgeo import ogr

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)s:%(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S %Z')

log = logging.getLogger('imagemosaic_load')
log.addFilter(logging.Filter('imagemosaic_load'))


def process_granule(lyr, granule):
    log.debug("Processing granule %s", granule)
    if not os.path.isfile(granule):
        log.error("Unable to locate granule '%s'", granule)
        return False

    img = gdal.Open(granule)

    transform = img.GetGeoTransform()
    cols = img.RasterXSize
    rows = img.RasterYSize

    # NOTE: Transform from image space (col, row) to Geo space Xp,Yp
    # Xp = padfTransform[0] + col * padfTransform[1] + row * padfTransform[2];
    # Yp = padfTransform[3] + col * padfTransform[4] + row * padfTransform[5];
    footprint_ring = ogr.Geometry(ogr.wkbLinearRing)

    # Upper Left
    footprint_ring.AddPoint(
        transform[0],
        transform[3])
    # Lower Left
    footprint_ring.AddPoint(
        transform[0] + 0 * transform[1] + rows * transform[2],
        transform[3] + 0 * transform[4] + rows * transform[5])
    # Lower Right
    footprint_ring.AddPoint(
        transform[0] + cols * transform[1] + rows * transform[2],
        transform[3] + cols * transform[4] + rows * transform[5])
    # Upper Right
    footprint_ring.AddPoint(
        transform[0] + cols * transform[1] + 0 * transform[2],
        transform[3] + cols * transform[4] + 0 * transform[5])
    # Upper Left
    footprint_ring.AddPoint(
        transform[0],
        transform[3])

    footprint = ogr.Geometry(ogr.wkbPolygon)
    footprint.AddGeometry(footprint_ring)

    log.info("Image %s - Footprint: %s", granule, footprint.ExportToWkt())

    date = datetime.datetime.now()
    feature = ogr.Feature(lyr.GetLayerDefn())
    feature.SetField("location", granule)

#    feature.SetField("ingestion", date.year, date.month, date.day, date.hour, date.minute, date.second, 1)
    feature.SetGeometry(footprint)
    lyr.CreateFeature(feature)

    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load image mosaic granules directly into postgres")
    parser.add_argument('--host', help='Postgres Host')
    parser.add_argument('--port', default='5432', help='Postgres Port')
    parser.add_argument('--user', default='geoserver', help='Postgres User')
    parser.add_argument('--password', help='Postgres Password')
    parser.add_argument('--db', help='Postgres database')
    parser.add_argument('--layer', help='layer name (schema.table_name)')
    parser.add_argument('granules', metavar='granules', nargs='+', help='Path to granules (wildcards supported')

    args = parser.parse_args()

    driverName = "PostgreSQL"
    drv = ogr.GetDriverByName(driverName)
    if drv is None:
        log.critical("%s driver not available.\n" % driverName)
        exit(-1)

    connString = "PG: host=%s port=%s dbname=%s user=%s password=%s" % (
        args.host, args.port, args.db, args.user, args.password)

    log.debug("Connecting to PostgreSQL - host=%s port=%s dbname=%s user=%s password=******", args.host, args.port,
              args.db, args.user)
    conn = ogr.Open(connString, 1)
    if not conn:
        log.critical("Database connection failed")
        exit(-1)
    log.debug("Connected to PostgreSQL")

    lyr = conn.GetLayer(args.layer)

    for path in args.granules:
        log.debug("Processing %s", path)
        files = glob.iglob(path)
        for g in files:
            process_granule(lyr, g)

    conn = None
