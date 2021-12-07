"""
Originally written by HPM in matlab. Transcribed by Micah J. into python.
Script uses the urls, polarizations and file types to download uavsar data. It will not overwrite files
so if you want to re-download fresh manually remove the output_dir.

Warning: Canceling the script mid run will produce a file partially written. Rerunning the script will think the
file is already downloaded and skip it. You will have to remove that file if you want to re-download it.

usage:
    python3 download_uavsar.py
"""

import requests
import os
from os.path import join, isdir, isfile, basename
import progressbar


def stream_download(url, output_f):
    """

    Args:
        url: url to download
        output_f: path to save the data to

    """
    print(f"\n\nDownloading {url}\nSaving to {output_f}")
    r = requests.get(url, stream=True)

    if r.status_code == 200:
        bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength)
        with open(output_f, 'wb') as f:
            for i, chunk in enumerate(r):
                f.write(chunk)
                bar.update(i)
    else:
        print(f"HTTP CODE {r.status_code}. Skipping download!")


def download_uavsar(url, base_flight_name, output_dir='./data/uavsar', polarizations=['HH', 'HV', 'VH', 'VV'],
                    file_types=['amp1', 'amp2', 'cor', 'int']):
    """
    Downlaods uavsar files by constructing the file urls using a url, a base file name, a polarization and a file type

    Args:
        url: A url containing multiple uavsar flights
        base_flight_name: basename of the flight files
        output_dir: Location to save the data
        polarizations: Polarizations to download, e.g. HH
        file_types: uavsar file to download, e.g. amp1

    Returns:
        None

    Raises:
        ValueError: If the base flight name is missing the polarization HH
    """

    # Make the output dir if it doesn't exist
    if not isdir(output_dir):
        os.makedirs(output_dir)

    if "HH" not in base_flight_name:
        raise ValueError("The base_flight_name param must have a HH polarization in the name to know where to replace "
                         "the polarization that's being requested.")

    # For each polarization requested download the set of files requested
    for pol in polarizations:
        for ext in file_types:
            # Form the base of the file to download
            base = f"{base_flight_name.replace('HH', pol)}.{ext}.grd"
            remote = join(url, base)
            local = join(output_dir, base)

            if not isfile(local):
                stream_download(remote, local)
            else:
                print(f"{local} already exists, skipping download!")

        # Download the ann file always.
        remote = remote.replace(f"{ext}.grd", "ann")
        local = local.replace(f"{ext}.grd", "ann")

        if not isfile(local):
            stream_download(remote, local)
        else:
            print(f"{local} already exists, skipping download!")

def main():
    """
    Main function to download our dataset
    """

    # list of Pairs of urls and base flights to download
    downloads = [

        # Grand Mesa processed with 5m DTM
        ('http://downloaduav.jpl.nasa.gov/Release2u/grmesa_27416_20003-028_20005-007_0011d_s01_L090_03/',
         'grmesa_27416_20003-028_20005-007_0011d_s01_L090HH_03'),

        # Boise Basin, lowman_23205, 1 / 31 - 2 / 13
        ('http://downloaduav.jpl.nasa.gov/Release2v/lowman_23205_20002-007_20007-003_0013d_s01_L090_01/',
         'lowman_23205_20002-007_20007-003_0013d_s01_L090HH_01'),

        # Boise Basin, lowman_23205, 2 / 13 - 2 / 21
        ('http://uavsar.asfdaac.alaska.edu/UA_lowman_23205_20007-003_20011-003_0008d_s01_L090_01/',
         'lowman_23205_20007-003_20011-003_0008d_s01_L090HH_01'),

        # Boise Basin, lowman_23205, 2 / 21 - 3 / 11
        ('http://downloaduav.jpl.nasa.gov/Release2v/lowman_23205_20011-003_20016-004_0019d_s01_L090_01/',
         'lowman_23205_20011-003_20016-004_0019d_s01_L090HH_01'),

        # Reynolds Creek, silver_34715, 2 / 21 - 3 / 11
        ('http://downloaduav.jpl.nasa.gov/Release2x/silver_34715_20011-001_20016-002_0019d_s01_L090_01/', \
         'silver_34715_20011-001_20016-002_0019d_s01_L090HH_01'),
    ]


    for url, base_flight in downloads:

        # Use the defaults for the polarizations and files to download.
        download_uavsar(url, base_flight)

# Do not run the downloading process unless the file is called directly.
# This allows us to import the functions here in other scripts is need be.
if __name__ == '__main__':
    main()