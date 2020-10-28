'''
Script is used to make a UAVSAR subset data in the original data format so
we can use it for testing
'''
from os.path import join, dirname, basename, abspath, expanduser, isdir
import os
from os import mkdir
import numpy as np
from snowxsql.metadata import read_InSar_annotation
from snowxsql.conversions import INSAR_to_rasterio
from snowxsql.projection import reproject_raster_by_epsg
from snowxsql.utilities import get_logger
import utm
import matplotlib.pyplot as plt
import rasterio
from rasterio.crs import CRS
from rasterio.plot import show
from rasterio.transform import Affine
import glob
from shutil import copyfile, rmtree
from snowxsql.utilities import find_kw_in_lines

log = get_logger('InSar Test Data')


def get_crop_indices(n, ratio):
    '''
    Given a number of columns or row, return the indices of the start and end
    of the value cropped on its middle value out to a certain percentage

    Args:
        n: Total number of columns or rows
        ratio: decimal of the data to cut around the half the count
    '''
    start = int(0.5 * n - 0.5 * ratio * 0.5 * n)
    end = int(0.5 * n + 0.5 * ratio * 0.5 * n)
    spread = end - start
    return start, end, spread

def get_uavsar_annotation(fkey, directory):
    '''
    '''
    # search local files for a matching file with .ann in its name
    ann_candidates = os.listdir(directory)
    fmatches = [f for f in ann_candidates if fkey in f and 'ann' in f]

    # If we find too many or not enough raise an exception and exit
    if len(fmatches) != 1:
        raise ValueError('Unable to find a corresponding description file to'
                         ' UAVsAR file {}'.format(f))

    # Form the descriptor file name based on the grid file name, should have .ann in it
    ann_file = join(directory, fmatches[0])
    desc = read_InSar_annotation(ann_file)

    return desc, ann_file

def get_grid_dims(desc):
    '''
    Returns the nrows, ncols, dlat, and dlon from the annotation file
    '''
    # Grab array size
    nrows = desc['ground range data latitude lines']['value']
    ncols = desc['ground range data longitude samples']['value']
    dlat = desc['ground range data latitude spacing']['value']
    dlon = desc['ground range data longitude spacing']['value']

    return nrows, ncols, dlat, dlon

def make_mods(desc, ratio):
    '''
    Make the modifcation Dictionary

    Args:
        desc: Dictionary reprenting the annotation file
        ratio: Fraction of the data to crop to in both directions on center

    Returns:
        mods: Dictionary of desc entries to modify
    '''
    mods = {}
    # Grab array size
    nrows, ncols, dlat, dlon = get_grid_dims(desc)

    # Attempt to crop on the center of the image
    start_row, end_row, new_nrows = get_crop_indices(nrows, ratio)
    start_col, end_col, new_ncols = get_crop_indices(ncols, ratio)

    log.info('After cropping images should be {} x {}'.format(new_nrows, new_ncols))

    # mods to write out later to the ann file
    mods['ground range data latitude lines'] = new_nrows
    mods['ground range data longitude samples'] = new_ncols

    mods['ground range data starting latitude'] = start_row * dlat + desc['ground range data starting latitude']['value']
    mods['ground range data starting longitude'] = start_col * dlon + desc['ground range data starting longitude']['value']

    return mods


def open_crop_grd_files(f, desc, ratio, out_file):
    '''
    Open the grd file, crop to a known dimension. Then save back to bytes
    in the test/data folder

    Args:
        f: Input file
        desc: Annotation Dictionary
        ratio: Fraction to subset the data on center
        out_file: Output location
    '''

    # Grab array size
    nrows, ncols, dlat, dlon = get_grid_dims(desc)

    # Attempt to crop on the center of the image
    start_row, end_row, new_nrows = get_crop_indices(nrows, ratio)
    start_col, end_col, new_ncols = get_crop_indices(ncols, ratio)

    # how to map the names
    data_map = {'int':'interferogram',
                'amp1':'amplitude of pass 1',
                'amp2':'amplitude of pass 2',
                'cor':'correlation'}

    # Grab the dataname
    d_key =  basename(f).split('.')[-2]
    dname = data_map[d_key]
    log.info('Processing {} file...'.format(dname))

    # Gte the number of bytes for the dataset
    if 'amplitude' in dname:
        temp_d = 'amplitude'
    else:
        temp_d = dname

    desc_name = '{} bytes per pixel'.format(temp_d)
    bytes = desc[desc_name]['value']

    # Read in the data as bytes
    log.info('Reading {} as bytes...'.format(basename(f)))

    with open(f,'rb') as fp:
        z_b = fp.read()
        fp.close()

    # Number of Bits of the file
    nbytes = len(z_b)

    # Number of Bytes of the incoming array that we expect
    expected_full_nbytes = nrows * ncols * bytes

    # Number of Bytes of the resulting bits after cropping
    expected_cropped_nbytes = new_nrows * new_ncols * bytes

    # MegaBytes version of each of nbits
    Mbytes = nbytes / 1e6
    expected_full_Mbytes = expected_full_nbytes / 1e6
    expected_cropped_Mbytes = expected_cropped_nbytes / 1e6

    log.info('File is {:0.2f} Mb.'.format(Mbytes))
    log.info('Cropping to {:0.2f}% of data'.format(100.0 * ratio**2))
    log.info('Based on the nrows and ncols, data post crop should be {:0.2f} Mb'.format(expected_cropped_Mbytes))

    # Retrieve the reported file size
    key = 'ground range {}'.format(dname)
    reported_size = int(desc[key]['comment'].split(' ')[-2].strip()) / 1e6

    if reported_size != Mbytes:
        log.warning('Filesize read in doesnt match the reported value in the'
                    ' annotation file.\nAnnotation File = {}'
                    '\nReported = {}Mb\nRead = {}Mb'
                    ''.format(ann_file, reported_size, Mbytes))

    # First check that the incoming data is what we expected
    if Mbytes != expected_full_Mbytes:
        log.warning('File size does not match the estimated size based on rows and cols'
                    'file ({:0.2f} Mb != {:0.2f} Mb)'.format(Mbytes, expected_full_Mbytes))

    # Convert to our desired data
    if dname == 'interferogram':
        dtype = np.dtype([('real', '<f4'), ('imaginary', '<f4')])
    else:
        dtype = np.dtype([('real', '<f{}'.format(bytes))])

    log.info("dtype is defined as {}.".format(dtype))

    # Convert array and reshape to a matrix for easier indexing
    arr = np.frombuffer(z_b, dtype=dtype).reshape(nrows, ncols)

    # Crop the data, calc/report stats on it
    sub_arr = arr[start_row:end_row, start_col:end_col]
    b = sub_arr.tobytes()

    # log.info('Number of values being written to file {}'.format(len(sub_arr.flatten())))
    for n in sub_arr.dtype.names:
        log.info('Stats for {} ({}) subsampled...'.format(dname, n))
        for stat in ['mean','min','max', 'std']:
            log.info('\t* {} = {}'.format(stat, getattr(sub_arr[n], stat)()))

    # Write out the binary data to the file for testing the software
    log.info('Writing output to {}'.format(out_file))
    with open(out_file, 'wb+') as fp:
        fp.write(b)
        fp.close()


def copy_and_mod_annotation(ann_file, out_f, mods):
    '''
    Copy the annotation file to ../../tests/data/uavsar.ann. Then modify the file
    to represent the new data thats been cropped

    Args:
        ann_file: Path to the original annotation file
        out_f: Path to save the modified file
        mods: Dictionary of modifications to apply to the annotation file
    '''

    log.info('Copying over the annotation file to {} with modifications...'.format(out_f))
    # Read the file in
    with open(ann_file, 'r') as fp:
        lines = fp.readlines()

        # Modify it
        for k,v in mods.items():
            i = find_kw_in_lines(k.title(), lines, addon_str='')

            if i != -1:
                # Found the option in the file, try to replace the value and keep the comment
                log.info('\tUpdating {} in annotation file...'.format(k))
                info = lines[i].split('=')
                name = info[0].strip()

                data = ''.join(info[1:])
                comment = ''
                spacing = ''

                if ';' in lines[i]:
                    content = data.split(';')
                    comment = '; ' + content[-1].strip()

                msg = '{:<63}= {:<23}{:<50}\n'.format(name, v, comment)
                lines[i] = msg

        fp.close()

    # Write out the new file with mods
    with open(out_f, 'w+') as fp:
        fp.write(''.join(lines))
    fp.close()


def main():

    # How much of the original data cropped to the middle, e.g. 20% in each direction from center
    ratio = 0.2

    # Output directory
    outdir = '../../tests/data'
    temp = './temp'

    outdir = abspath(outdir)
    temp = abspath(temp)

    # Pattern to look for
    directory= '~/Downloads/SnowEx2020_UAVSAR'
    pattern = 'grmesa_27416_20003-028_20005-007_0011d_s01_L090HH_01.*.grd'

    # Get the directory the file
    directory = abspath(expanduser(directory))
    files = glob.glob(join(directory, pattern))

    log.info('Found {} files that can be used for testing...'.format(len(files)))

    fkey = pattern.split('.')[0]

    # Get the ann file
    desc, ann_file = get_uavsar_annotation(fkey, directory)

    # Form the modifications to the ann file
    mods = make_mods(desc, ratio)

    # make our temporary folder
    if isdir(temp):
        rmtree(temp)

    mkdir(temp)

    log.info("")
    log.info("Cropping binary files...")
    for f in files:
        # Output file name, use the same extension
        ext = '.'.join(f.split('.')[-2:])
        grd_file = join(outdir, 'uavsar_latlon.' + ext)

        # Crop and save resulting binary file in tests/data
        open_crop_grd_files(f, desc, ratio, grd_file)

    # Modify the annotation file and write it out to the new location
    new_ann = join(outdir, 'uavsar_latlon.ann')
    copy_and_mod_annotation(ann_file, new_ann, mods)
    desc = read_InSar_annotation(new_ann)

    log.info("")
    log.info("Converting files to GeoTiffs...")
    for f in glob.glob(join(outdir,'uavsar_latlon*.grd')):
        # Convert the binary to tiff and save in our testing
        tif_file = f.replace('.grd','.tif')
        INSAR_to_rasterio(f, desc, tif_file)

    log.info("")
    log.info("Reprojecting files to GeoTiffs...")

    utm_dir = '../../tests/data/uavsar'
    utm_dir = abspath(expanduser(utm_dir))

    if isdir(utm_dir):
        rmtree(utm_dir)
    mkdir(utm_dir)

    for f in glob.glob(join(outdir,'uavsar_latlon*.tif')):
        # # Reproject the data

        utm_file = basename(f).replace('_latlon','_utm')
        utm_file = join(utm_dir, utm_file)
        reproject_raster_by_epsg(f, utm_file, 26912)

if __name__ == '__main__':
    main()
