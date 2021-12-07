"""
This script uploads the UAVSAR raster to the database after they have been
converted to geotifs.

Usage:
1. Download the data from the GDrive sent from HP.

2. Unzip into your Downloads.

3. Convert data to GeoTiffs, and reprojects it (use convert_uavsar.py)

4. Run this script.

Usage:
    # To run with all the scripts
    python run.py

    # To run individually
    python add_UAVSAR.py

"""
import sys
import glob
from os.path import abspath, expanduser, join

from snowexsql.batch import UploadUAVSARBatch


def main():

    region = 'all'

    if len(sys.argv) > 1:
        region = sys.argv[1]

    # Location of the downloaded data
    downloads = '../download/data/uavsar'

    # Sub folder name under the downloaded data that the tifs were saved to
    geotif_loc = 'geotiffs'

    data = {
        # Tile the data going in for faster retrieval
        'tiled': True,

        # Spatial Reference
        'epsg': 26912,

        # Metadata
        'surveyors': 'UAVSAR team, JPL',
        'instrument': 'UAVSAR, L-band InSAR',
        'site_name': 'Grand Mesa',
        'units': None,  # Add from the Annotation file
        'description': '',  # Added from the annotation file
        'doi': "https://asf.alaska.edu/doi/uavsar/#R0ARICRBAKYE"
    }

    # Expand the paths
    downloads = abspath(expanduser(downloads))
    geotif_loc = join(downloads, geotif_loc)

    # error counting
    errors_count = 0

    if region in ['all', 'grand_mesa']:
        print('Uploading Grand Mesa')
        ########################## Grand Mesa #####################################
        # Grab all the grand mesa annotation files in the original data folder
        ann_files = glob.glob(join(downloads, 'grmesa_*.ann'))

        # Instantiate the uploader
        rs = UploadUAVSARBatch(ann_files, geotiff_dir=geotif_loc, **data)

        # Submit to the db
        rs.push()

        # Keep track of number of errors for run.py
        errors_count += len(rs.errors)

        # Memory clean up
        del rs

    if region in ['all', 'lowman']:
        print('Uploading Lowman')
        ############################### Idaho - Lowman ####################################
        # Make adjustments to metadata for lowman files
        data['site_name'] = 'idaho'
        data['epsg'] = 29611

        # Grab all the lowman and reynolds annotation files
        ann_files = glob.glob(join(downloads, 'lowman_*.ann'))

        # Instantiate the uploader
        rs = UploadUAVSARBatch(ann_files, geotiff_dir=geotif_loc, **data)

        # Submit to the db
        rs.push()

        # Keep track of the number of errors
        errors_count += len(rs.errors)

        # Memory clean up
        del rs

    if region in ['all', 'reynolds']:
        print("Uploading Reynolds Creek")

        ############################## Idaho - Reynolds ####################################
        # Make adjustments to metadata for lowman files
        data['site_name'] = 'idaho'
        data['epsg'] = 29611

        # Grab all the lowman and reynolds annotation files
        ann_files = glob.glob(join(downloads, 'silver_*.ann'))

        # Instantiate the uploader
        rs = UploadUAVSARBatch(ann_files, geotiff_dir=geotif_loc, **data)

        # Submit to the db
        rs.push()

        # Keep track of the number of errors
        errors_count += len(rs.errors)

        # Return the error count so run.py can keep track
        return errors_count


if __name__ == '__main__':
    main()
