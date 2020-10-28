from resample_smp import resample_batch
from os.path import abspath, join, expanduser

# Chosen files for testing. Semi random choice
filenames = ['S19M1013_5S21_20200201.CSV', 'S06M0874_2N12_20200131.CSV']

d = '~/Downloads/NSIDC-upload/level_1_data/csv'

filenames = [abspath(expanduser(join(d, f))) for f in filenames]

# Add the files to our data folder but do not delete it ever.
resample_batch(filenames, '../../tests/data', 1000, header_pos=6, clean_on_start=False)
