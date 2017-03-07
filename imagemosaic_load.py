import argparse
import glob
import datetime
import logging
import os.path
from osgeo import gdal
from osgeo import ogr
from multiprocessing import Process, Pool, Manager, Queue

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(process)d] %(name)s:%(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S %Z')

log = logging.getLogger('imagemosaic_load')
log.addFilter(logging.Filter('imagemosaic_load'))


def file_lister(paths, q, count, finished):
    for path in paths:
        log.debug("Processing %s", path)
        files = glob.iglob(path)
        for g in files:
            count.value += 1
            q.put(g)
    finished.value = 1


def granule_processor(fq, gq):
    while True:
        granule = fq.get()
        log.debug("Processing granule %s", granule)
        if not os.path.isfile(granule):
            log.error("Unable to locate granule '%s'", granule)
            continue

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

        value = {'location': granule, 'footprint': footprint.ExportToWkt()}
        gq.put(value)

if __name__ == "__main__":
    driverName = "PostgreSQL"
    drv = ogr.GetDriverByName(driverName)
    if drv is None:
        log.critical("%s driver not available.\n" % driverName)
        exit(-1)

    parser = argparse.ArgumentParser(description="Load image mosaic granules directly into postgres")
    parser.add_argument('--host', help='Postgres Host')
    parser.add_argument('--port', default='5432', help='Postgres Port')
    parser.add_argument('--user', default='geoserver', help='Postgres User')
    parser.add_argument('--password', help='Postgres Password')
    parser.add_argument('--db', help='Postgres database')
    parser.add_argument('--layer', help='layer name (schema.table_name)')
    parser.add_argument('--threads', default='4', help='number of threads for parsing images')
    parser.add_argument('granules', metavar='granules', nargs='+', help='Path to granules (wildcards supported')

    args = parser.parse_args()
    manager = Manager()
    fileQueue = manager.Queue()
    granuleQueue = manager.Queue()
    granuleCount = manager.Value('i', 0)
    finished = manager.Value('i', 0)
    fileListProcess = Process(target=file_lister, args=(args.granules, fileQueue, granuleCount, finished, ))

    fileListProcess.start()

    granulePool = Pool(int(args.threads))
    granulePool.apply_async(granule_processor, args=(fileQueue, granuleQueue, ))

    connString = "PG: host=%s port=%s dbname=%s user=%s password=%s" % (
        args.host, args.port, args.db, args.user, args.password)

    log.debug("Connecting to PostgreSQL - host=%s port=%s dbname=%s user=%s password=******", args.host, args.port,
              args.db, args.user)
    conn = ogr.Open(connString, 1)
    if not conn:
        log.critical("Database connection failed")
        exit(-1)
    log.debug("Connected to PostgreSQL")

    log.info("GetLayer %s", args.layer)
    lyr = conn.GetLayer(args.layer)

    processed = 0
    while True:
        granule = granuleQueue.get()

        log.info("Image %s - Footprint: %s", granule['location'], granule['footprint'])

        feature = ogr.Feature(lyr.GetLayerDefn())

        feature.SetField("location", granule['location'])

        feature.SetGeometry(ogr.CreateGeometryFromWkt(granule['footprint']))
        lyr.CreateFeature(feature)

        processed += 1
        log.debug("processed %d, total %d, finished %d", processed, granuleCount.value, finished.value)
        if finished.value and granuleCount.value == processed:
            break


    fileListProcess.join()
    granulePool.close()

    conn = None
