'''
Module for classes that upload single files to the database.
'''
from . data import *
from .string_management import *
from .interpretation import *
from .utilities import get_logger, assign_default_kwargs
from .projection import reproject_point_in_dict, add_geom
from .metadata import DataHeader
from .db import get_table_attributes
from . import units as master_units

import pandas as pd
import progressbar
from subprocess import check_output, STDOUT
from geoalchemy2.elements import RasterElement, WKTElement
from geoalchemy2.shape import from_shape
import utm
import os
from os.path import join, abspath, expanduser
import numpy as np
import time


class BaseUploader():
    '''
    Base class for uploading everything
    '''
    # Default kwargs
    defaults = {'epsg':26912,
                'debug':True}

    # Class pointing to the table for retrieving common attributes
    TableClass = None

    def __init__(self, filename,  **kwargs):
        # Logger
        self.log = get_logger(__name__)

        # Filename to upload
        self.filename = filename

        # Expected Attributes of the table class
        self.expected_attributes = [c for c in dir(self.TableClass) if c[0] != '_']

        # Metadata passed through from kwargs
        self.data = assign_default_kwargs(self, kwargs, self.defaults)

        # Read in the data and add metadata
        self.log.info('Reading in {} data from {}'.format(self.TableClass.__name__.replace('Data', ''), filename))

        self.df = self._read(filename)
        self.df = self.prepare(self.df)

        # Keep track of successul uploads and errors
        self.uploaded = 0
        self.errors = []

        # Add a progressbar if its long upload
        if len(self.df.index) > 1000:
            self.long_upload = True
            self.bar = progressbar.ProgressBar(max_value=len(self.df.index))
        else:
            self.long_upload = False

    def _read(self, filename):
        '''
        Abstract function that defines self.df for uploading
        '''
        pass

    def prepare(self, df):
        '''
        Adds in constant meta data to the dataframe before the upload loop
        '''
        # Add in constant metadata here
        return df

    def trim_columns(self, df):
        '''
        Drops all the columns that are not relevant to the db

        Args:
            df: Pandas dataframe
        Return:
            trimmed: pandas dataframe with only valid database columns
        '''
        trimmed = df.copy()

        # Drop all columns were not expecting to go into the
        drop_cols = [c for c in trimmed.columns if c not in self.expected_attributes]

        # Let the database manage any indices. so always drop it (Snow depths have an ID)
        if 'id' in trimmed.columns:
            drop_cols.append('id')

        trimmed = trimmed.drop(columns=drop_cols)

        # Add any special submission adjustments here
        return trimmed

    def build_data(self, data_name):
        '''
        Function called during the upload loop which assigns the main value to
        the data and drops all other columns that are invalid to the db
        '''

        trimmed = self.trim_columns(self.df)
        return trimmed

    def submit(self, session):
        '''
        Abstract function for submitting to the database
        '''
        # Construct a dataframe with all metadata
        for name in self.data_names:
            df = self.build_data(name)

            # Grab each row, convert it to dict and join it with site info
            for i,row in df.iterrows():
                data = row.to_dict()

                if self.debug:
                    self.add_one(session, data)
                else:
                    try:
                        self.add_one(session, data)

                    except Exception as e:
                        self.errors.append(e)
                        self.log.error((i, e))

        # Error reporting
        if len(self.errors) > 0:
            name = (self.TableClass.__name__.replace('Data','') + 's').lower()
            self.log.error('{} {} failed to upload.'.format(len(self.errors),
                                                            name))
            self.log.error('The following {} indicies failed with '
                           'their corresponding errors:'.format(name))

            for e in self.errors:
                self.log.error('\t{} - {}'.format(e[0], e[1]))


    def add_one(self, session, data):
        '''
        Uploads smallest unit of database entry.
        e.g.
            Points: Is a single value with metadata
            Layers: Is a single layer
            Raster: Is a single Raster tile

        Args:
            session: SQLAlchemy database session
            data: Dictionary of the data to submit to the db
        '''

        # Create db interaction, pass data as kwargs to class submit data
        d = self.TableClass(**data)

        session.add(d)
        session.commit()
        self.uploaded += 1

class BaseTextUploader(BaseUploader):
    '''
    Uploading anything that comes out of a text csv file
    This includes Point data and Layer Data
    '''

    def __init__(self, filename, **kwargs):

        # All text files have columns and maybe some header info
        self.hdr = DataHeader(filename, **kwargs)
        super().__init__(filename, **kwargs)

        # Transfer a couple attributes for brevity
        for att in ['data_names', 'multi_sample_profiles']:
            setattr(self, att, getattr(self.hdr, att))

    def _read(self, filename):
        '''
        Reads in data according to the header interpretation
        '''
        # header = 0 because docs say to if using skiprows and columns
        df = pd.read_csv(filename, header=0, skiprows= self.hdr.header_pos,
                                             names=self.hdr.columns,
                                             encoding='latin')

        # replace all nans or string nones with None (none type)
        df = df.apply(lambda x: parse_none(x))

        return df

class UploadProfileData(BaseTextUploader):
    '''
    Class for submitting a single profile. Since layers are uploaded layer by
    layer this allows for submitting them one file at a time.
    '''
    expected_attributes = [c for c in dir(LayerData) if c[0] != '_']

    def __init__(self, profile_filename, **kwargs):
        self.log = get_logger(__name__)

        self.filename = profile_filename

        # Read in the file header
        self.hdr = DataHeader(profile_filename, **kwargs)

        # Transfer a couple attributes for brevity
        for att in ['data_names', 'multi_sample_profiles']:
            setattr(self, att, getattr(self.hdr, att))

        # Read in data
        self.df = self._read(profile_filename)


    def _read(self, profile_filename):
        '''
        Read in a profile file. Managing the number of lines to skip and
        adjusting column names

        Args:
            profile_filename: Filename containing the a manually measured
                             profile
        Returns:
            df: pd.dataframe contain csv data with standardized column names
        '''
        # header=0 because docs say to if using skiprows and columns
        df = pd.read_csv(profile_filename, header=0,
                                           skiprows= self.hdr.header_pos,
                                           names=self.hdr.columns,
                                           encoding='latin')

        # If SMP profile convert depth to cm
        depth_fmt = 'snow_height'
        is_smp = False
        if 'force' in df.columns:
            df['depth'] = df['depth'].div(10)
            is_smp = True
            depth_fmt = 'surface_datum'

        # Standardize all depth data
        new_depth = standardize_depth(df['depth'],
                                      desired_format=depth_fmt,
                                      is_smp=is_smp)

        if 'bottom_depth' in df.columns:
            delta = df['depth'] - new_depth
            df['bottom_depth'] = df['bottom_depth'] - delta

        df['depth'] = new_depth

        delta = abs(df['depth'].max() - df['depth'].min())
        self.log.info('File contains {} profiles each with {} layers across '
                     '{:0.2f} cm'.format(len(self.hdr.data_names), len(df), delta))
        return df

    def check(self, site_info):
        '''
        Checks to be applied before submitting data
        Currently checks for:

        1. Header information integrity between site info and profile headers

        Args:
            site_info: Dictionary containing all site information

        Raises:
            ValueError: If any mismatches are found
        '''

        # Ensure information matches between site details and profile headers
        mismatch = self.hdr.check_integrity(site_info)

        if len(mismatch.keys()) > 0:
            self.log.error('Header Error with {}'.format(self.filename))
            for k,v in mismatch.items():
                self.log.error('\t{}: {}'.format(k, v))
                raise ValueError('Site Information Header and Profile Header '
                                 'do not agree!\n Key: {} does yields {} from '
                                 'here and {} from site info.'.format(k,
                                                             self.hdr.info[k],
                                                             site_info[k]))

    def build_data(self, data_name):
        '''
        Build out the original dataframe with the metdata to avoid doing it
        during the submission loop. Removes all other main profile columns and
        assigns data_name as the value column

        Args:
            data_name: Name of a the main profile

        Returns:
            df: Dataframe ready for submission
        '''

        df = self.df.copy()

        # Assign all meta data to every entry to the data frame
        for k, v in self.hdr.info.items():
            df[k] = v

        df['type'] = data_name

        # Get the average if its multisample profile
        if data_name in self.multi_sample_profiles:
            kw = '{}_sample'.format(data_name)
            sample_cols = [c for c in df.columns if kw in c]
            df['value'] = df[sample_cols].mean(axis=1).astype(str)

            # Replace the data_name sample columns with just sample
            for s in sample_cols:
                n = s.replace(kw, 'sample')
                df[n] = df[s].copy()

        # Individual
        else:
            df['value'] = df[data_name].astype(str)

        # Drop all columns were not expecting
        df = self.trim_columns(df)

        # Clean up comments a bit
        if 'comments' in df.columns:
            df['comments'] = df['comments'].apply(lambda x: x.strip(' ') if type(x) == str else x)

        return df

    def submit(self, session):
        '''
        Submit values to the db from dictionary. Manage how some profiles have
        multiple values and get submitted individual

        Args:
            session: SQLAlchemy session
        '''
        long_upload = False

        # Construct a dataframe with all metadata
        for pt in self.data_names:
            df = self.build_data(pt)


            # Grab each row, convert it to dict and join it with site info
            for i,row in df.iterrows():
                data = row.to_dict()

                d = LayerData(**data)
                session.add(d)
                session.commit()

                if long_upload:
                    bar.update(i)

        self.log.debug('Profile Submitted!\n')

class PointDataCSV(BaseTextUploader):
    '''
    Class for submitting whole csv files of point data
    '''

    # Remapping for special keywords for snowdepth measurements
    measurement_names = {'mp':'magnaprobe','m2':'mesa', 'pr':'pit ruler'}

    # Units to apply
    units = master_units

    # Assign the table class which is used for uploading
    TableClass = PointData

    # Class attributes to apply
    defaults = {'debug':True, 'utm_zone':12, 'epsg':26912}

    def prepare(self, df):
        '''
        Prepare the data for before the upload loop.
        This renames the instruments
        '''

        # Assign the measurement tool verbose name
        if 'instrument' in df.columns:
            self.log.info('Renaming instruments to more verbose names...')
            df['instrument'] = \
                df['instrument'].apply(lambda x: remap_data_names(x, self.measurement_names))

        # Add date and time keys
        self.log.info('Adding date and time to metadata...')
        df = df.apply(lambda data: add_date_time_keys(data, timezone=self.hdr.timezone), axis=1)

        # Add projection info
        self.log.info('Converting locations...')

        if 'utm_zone' not in df.columns:
            df['utm_zone'] = int(self.utm_zone)

        df = df.apply(lambda row: reproject_point_in_dict(row), axis=1)

        self.log.info('Adding geometry object to the metadata...')
        df['geom'] = df.apply(lambda row: add_geom(row, self.epsg), axis=1)

        return df

    def build_data(self, data_name):
        '''
        Pad the dataframe with metdata or make info more verbose
        '''

        # Assign our main value to the value column
        df = self.df.copy()
        df['value'] = self.df[data_name].copy()
        df['type'] = data_name

        # Add units
        if data_name in self.units.keys():
            df['units'] = self.units[data_name]

        # Drop all columns were not expecting
        df = self.trim_columns(df)

        return df


class StationDataCSV(PointDataCSV):
    '''
    Uploads a csv of Station data
    '''
    pass

class UploadRaster(BaseUploader):
    '''
    Class for uploading a single tifs to the database. Utilizes the raster2pgsql
    command and then parses it for delivery via python.
    '''

    defaults = {'epsg':26912,
                'tiled':False,
                'no_data': None,
                'debug':True}

    # Since Uploading a raster has no header, we had a data_name for uploading
    data_names = ['raster']

    # Assign the table class which is used for uploading
    TableClass = ImageData

    def _read(self, filename):
        '''
        Reads the raster using the raster2pgsql function and builds a dataframe
        to upload each piece
        '''
        df = pd.DataFrame(columns=['raster'])

        # This produces a PSQL command with auto tiling
        cmd = ['raster2pgsql','-s', str(self.epsg)]

        # Add tiling if requested
        if self.tiled == True:
            cmd.append('-t')
            cmd.append('500x500')

        # If nodata applied:
        if self.no_data != None:
            cmd.append('-N')
            cmd.append(str(self.no_data))

        cmd.append(filename)
        self.log.debug('Executing: {}'.format(' '.join(cmd)))
        s = check_output(cmd, stderr=STDOUT).decode('utf-8')

        # Split the SQL command at values (' which is the start of every one
        tiles = s.split("VALUES ('")[1:]
        tiles = [t.split("'::")[0] for t in tiles]

        if len(tiles) > 1:
            # -1 because the first element is not a
            self.log.info('Raster is split into {} tiles for uploading...'.format(len(tiles)))

        df['raster'] = tiles

        # Add in meta data
        for k,v in self.data.items():
            df[k] = v

        return df
