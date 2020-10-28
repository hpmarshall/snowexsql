'''
Module for header classes and metadata interpeters. This includes interpetting
data file headers or dedicated files to describing data.
'''

from .string_management import *
from .interpretation import *
from .data import SiteData
from .db import get_table_attributes
from .projection import reproject_point_in_dict, add_geom
from .utilities import get_logger, read_n_lines, assign_default_kwargs
from . import rename as master_rename
import utm
import pandas as pd
import pytz

from os.path import basename


def read_InSar_annotation(ann_file):
    '''
    .ann files describe the INSAR data. Use this function to read all that
    information in and return it as a dictionary

    Expected format:

    `DEM Original Pixel spacing (arcsec) = 1`

    Where this is interpretted as:
    `key (units) = [value]`

    Then stored in the dictionary as:

    `data[key] = {'value':value, 'units':units}`

    values that are found to be numeric and have a decimal are converted to a
    float otherwise numeric data is cast as integers. Everything else is left
    as strings.

    Args:
        ann_file: path to UAVsAR description file
    Returns:
        data: Dictionary containing a dictionary for each entry with keys
              for value, units and comments
    '''

    with open(ann_file) as fp:
        lines = fp.readlines()
        fp.close()

    data = {}

    # loop through the data and parse
    for line in lines:

        # Filter out all comments and remove any line returns
        info = line.strip().split(';')
        comment = info[-1].strip().lower()
        info = info[0]
        # ignore empty strings
        if info:
            d = info.split('=')
            name, value = d[0], d[1]


            # Clean up tabs, spaces and line returns
            key = name.split('(')[0].strip().lower()
            units = get_encapsulated(name, '()')
            if not units:
                units = None
            else:
                units = units[0]

            value = value.strip()

            ### Cast the values that can be to numbers ###
            if value.strip('-').replace('.','').isnumeric():
                if '.' in value:
                    value = float(value)
                else:
                    value = int(value)

            # Assign each entry as a dictionary with value and units
            data[key] = {'value': value, 'units': units, 'comment':comment}

    # Convert times to datetimes
    for pass_num in ['1','2']:
        for timing in ['start','stop']:
            key = '{} time of acquisition for pass {}'.format(timing, pass_num)
            dt = pd.to_datetime(data[key]['value'])
            data[key]['value'] = dt

    return data

class SMPMeasurementLog(object):
    '''
    Opens and processes the log that describes the SMP measurments. This file
    contains notes on all the measurements taken.

    This class build a dataframe from this file. It also reorganizes the
    file contents to be more standardized with our database.
    Some of this includes merging information in the comments.

    File should have the headers:
              Date,
              Pit ID
              SMP instrument #
              Fname sufix
              Orientation
              Snow depth
              Flag
              Observer
              Comments

    Attributes:
        observer_map: Dictionary mapping name initials to full verbose names
        orientation_map: Dictionary mapping the measurement locations relative
                         to the pit
        header: Dictionary containing other header information regarding the
                details of measurements
        df: Dataframe containing rows of details describing each measurement

    '''


    def __init__(self, filename):
        self.log = get_logger(__name__)

        self.header, self.df = self._read(filename)

        # Cardinal map to interpet the orientation
        self.cardinal_map = {'N':'North', 'NE':'Northeast', 'E':'East',
                             'SE':'Southeast', 'S':'South', 'SW':'Southwest',
                             'W':'West', 'NW':'Northwest', 'C':'Center'}

    def _read(self, filename):
        '''
        Read the CSV file thet contains SMP log inforamtion. Also reads in the
        header and creates a few attributes from that information:
            1. observer_map
            2. orientation_map
        '''
        self.log.info('Reading SMP file log header')

        header_pos = 9
        header = read_n_lines(filename, header_pos + 1)
        self.observer_map = self._build_observers(header)

        # parse/rename column names
        line = header[header_pos]
        str_cols = [standardize_key(col) for col in line.lower().split(',') if col.strip()]

        # Assume columns are populated left to right so if we have empty ones they are assumed at the end
        n_cols = len(str_cols)
        str_cols = remap_data_names(str_cols, DataHeader.rename)

        dtype = {k:str for k in str_cols}
        df = pd.read_csv(filename, header=header_pos, names=str_cols,
                                   usecols=range(n_cols), encoding='latin',
                                   parse_dates=[0], dtype=dtype)

        # Insure all values are 4 digits. Seems like some were not by accident
        df['fname_sufix'] = df['fname_sufix'].apply(lambda v: v.zfill(4))

        df = self.interpret_dataframe(df)

        return header, df

    def interpret_dataframe(self, df):
        '''
        Using various info collected from the dataframe header modify the data
        frame entries to be more verbose and standardize the database

        Args:
            df: pandas.Dataframe

        Returns:
            new_df: pandas.Dataframe with modifications
        '''
        # interpret sampling strategy
        # df = self.interpret_sample_strategy(df)

        # Apply observer map
        df = self.interpret_observers(df)

        # Apply orientation map

        # Pit ID is actually the Site ID here at least in comparison to the
        df['site_id'] = df['pit_id'].copy()

        return df


    def _build_observers(self, header):
        '''
        Interprets the header of the smp file log which has a map to the
        names of the oberservers names. This creates a dictionary mapping those
        string names
        '''
        # Map for observer names and their
        observer_map = {}

        for line in header:
            ll = line.lower()

            # Create a name map for the observers and there initials
            if 'observer' in ll:
                data = [d.strip() for d in line.split(':')[-1].split(',')]
                data = [d for d in data if d]

                for d in data:
                    info = [clean_str(s).strip(')') for s in d.split('(')]
                    name = info[0]
                    initials = info[1]
                    observer_map[initials] = name
                break

        return observer_map

    def interpret_observers(self, df):
        '''
        Rename all the observers with initials in the observer_map which is
        interpeted from the header

        Args:
            df: dataframe containing a column observer
        Return:
            new_df: df with the observers column replaced with more verbose
                    names
        '''
        new_df = df.copy()
        new_df['surveyors'] = \
                        new_df['surveyors'].apply(lambda x: self.observer_map[x])
        return new_df

    def interpret_sample_strategy(self, df):
        '''
        Look through all the measurements posted by site and attempt to
        determine the sample strategy

        Args:
            df: Dataframe containing all the data from the dataframe
        Returns:
            new_df: Same dataframe with a new column containing the sampling
                    strategy
        '''

        pits = pd.unique(df['pit_id'])

        for p in pits:
            ind = df['pit_id'] == p
            temp = df.loc[ind]
            orientations = pd.unique(temp['orientation'])

    def interpret_orientation(self, abbreviation, strategy='A'):
        '''
        Using the orietation information in the header of the SMP log file,
        this functions generates a map of the orientation abbreviations to a
        more verbose regarding the measurement orientation to the pit location

        The orientation is referencing a transect on centered on the pit and
        aligned with cardinal directions. Two strategies were implemented.

        Both strategies involved transect that move each direction 50m from the
        center of the pit and the E-W transect for both contain 10 measurements
        The two strategies vary in how many measurements were taken in the N-S
        transect.

        Strategy A: N-S transect has 6 measurments
        Strategy B: N-S transect has 10 measurments

        This function auto applies these strategies to generate a more verbose
        string to be used as a comment for the data entry

        Args:
            abbreviation: Abbreviated orientation
            strategy: Sampling strategy used for the N-S transect
        Returns:
            note: A more verbose string describing the orientation
        '''

        portion = {'p': 'pitwall measurement', 'p_top': 'top portion of pit',
                  'p_mid': 'mid portion', 'p_bot': 'bottom portion'}

        # Manage pit references:
        if 'pit' in abbreviation.lower():
            note = abbreviation
        else:
            letter = abbreviation[0]
            direction = cardinal_map[letter]

            if letter == 'C':
                note = ('Located at the center of the transects using sample '
                       'strategy {}'.format(strategy))

            else:
                position = int(abbreviation[1:])

                if strategy == 'A' and letter in ['N','S']:
                    dist_map = {3:10, 2:30, 1:50}

                elif strategy == 'B':
                    dist_map = {5:10, 4:20, 3:30, 2:40, 1:50}
                    dist = dist_map[position]

                else:
                    raise ValueError('Invalid strategy, please use A or B.'
                                     ' See docs for more info.')

                note = ('Located {}m {} of the center of the transect'
                       ''.format(direction, dist))

        return note

    def get_metadata(self, smp_file):
        '''
        Builds a dictionary of extra header information useful for SMP
        files which lack some info regarding submission to the db

        S06M0874_2N12_20200131.CSV, 0874 is the suffix

        '''
        s = basename(smp_file).split('.')[0].split('_')
        suffix = s[0].split('M')[-1]
        ind = self.df['fname_sufix'] == suffix
        meta = self.df.loc[ind]
        return meta.iloc[0].to_dict()

class DataHeader(object):
    '''
    Class for managing information stored in files headers about a snow pit
    site.

    Format of such file headers should be
    1. Each line of importance is preceded by a #
    2. Key values should be comma separated.

    e.g.
        `# PitID,COGM1C8_20200131`
        `# Date/Time,2020-01-31-15:10`

    If the file is not determined to be a site details file as indicated by the
    word site in the filename, then the all header lines except the last line
    is interpretted as header. In csv files the last line of the
    header should be the column header which is also interpretted and stored
    as a class attribute

    Attributes:
        info: Dictionary containing all header information, stripped of
              unnecesary chars, all lower case, and all spaces replaced with
              underscores
        columns: Column names of data stored in csv. None for site description
                 files which is basically all one header
        data_names: List of data names to be uploaded
        multi_sample_profiles: List containing profile types that
                              have multiple samples (e.g. density). This
                              triggers calculating the mean of the profiles
                              for the main value
        extra_header: Dictionary containing supplemental information to write
                      into the .info dictionary after its genrerated. Any
                      duplicate keys will be overwritten with this info.
    '''

    # Typical names we run into that need renaming
    rename = master_rename
    # rename = {'location':'site_name',
    #          'top': 'depth',
    #          'height':'depth',
    #          'bottom':'bottom_depth',
    #          'site': 'site_id',
    #          'pitid': 'pit_id',
    #          'slope':'slope_angle',
    #          'weather':'weather_description',
    #          'sky': 'sky_cover',
    #          'notes':'site_notes',
    #          'sample_top_height':'depth',
    #          'deq':'equivalent_diameter',
    #          'operator':'surveyors',
    #          'observer':'surveyors',
    #          'total_snow_depth':'total_depth',
    #          'smp_serial_number':'instrument',
    #          'lat':'latitude',
    #          'long':'longitude',
    #          'lon':'longitude',
    #          'twt':'two_way_travel',
    #          'measurement_tool':'instrument',
    #          'avgdensity':'density',
    #          'avg_density':'density',
    #          'dielectric_constant': 'permittivity',
    #          }

    # Known possible types anything not in here will throw an error
    available_data_names = ['density', 'permittivity','lwc_vol', 'temperature',
                     'force', 'reflectance','sample_signal',
                     'specific_surface_area', 'equivalent_diameter',
                     'grain_size', 'hand_hardness', 'grain_type',
                     'manual_wetness', 'two_way_travel', 'depth','swe',
                     # Met station names
                     'air_temperature', 'relative_humidity',
                     'mean_horizontal_wind_speed',
                     'unit_vector_mean_wind_direction', 'wind_direction',
                     'barometric_pressure', 'upward_looking_shortwave_radiation',
                     'downward_looking_shortwave_radiation',
                     'upward_looking_longwave_radiation',
                     'downward_looking_longwave_radiation',
                     'temperature',
                     'net_shortwave_radiation', 'net_longwave_radiation',
                     'albedo', 'total_net_radiation',
                     'upward_looking_longwave_radiation',
                     'downward_looking_longwave_radiation',
                     'infrared_surface_temperature', 'sensor_body_temperature',
                     'soil_temperature', 'air_snow_temperature',
                     'soil_moisture', 'soil_conductivity', 'soil_temperature',
                     'soil_conductivity', 'real_dielectric_permittivity',
                     'imaginary_dielectric_permittivity', 'mean_snow_depth',
                     'infrared_surface_temperature_nadir1',
                     'infrared_surface_temperature_nadir2',
                     'infrared_surface_temperature_30_degrees_north',
                     'infrared_surface_temperature_30_degrees_south',
                     'infrared_surface_temperature_s50cm_above_soil',
                     'infrared_surface_temperature_s40cm_above_soil',
                     'infrared_surface_temperature_s30cm_above_soil',
                     'infrared_surface_temperature_s20cm_above_soil',
                     'infrared_surface_temperature_s10cm_above_soil',
                     'infrared_surface_temperature_s5cm_above_soil',
                     'infrared_surface_temperature_s2cm_above_soil',
                     'infrared_surface_temperature_0cm_above_soil',
                     'infrared_surface_temperature_2cm_above_soil',
                     'infrared_surface_temperature_5cm_above_soil',
                     'infrared_surface_temperature_10cm_above_soil',
                     'infrared_surface_temperature_20cm_above_soil',
                     'infrared_surface_temperature_30cm_above_soil',
                     'infrared_surface_temperature_40cm_above_soil',
                     'infrared_surface_temperature_50cm_above_soil',
                     'infrared_surface_temperature_75cm_above_soil',
                     'infrared_surface_temperature_100cm_above_soil',
                     'infrared_surface_temperature_125cm_above_soil',
                     'infrared_surface_temperature_150cm_above_soil',
                     'infrared_surface_temperature_175cm_above_soil',
                     'infrared_surface_temperature_200cm_above_soil',
                     'infrared_surface_temperature_225cm_above_soil',
                     'infrared_surface_temperature_250cm_above_soil',
                     'infrared_surface_temperature_275cm_above_soil',
                     'infrared_surface_temperature_300cm_above_soil']
    # Defaults to keywords arguments
    defaults = {'timezone': 'MST',
                'epsg':26912,
                'header_sep': ',',
                'northern_hemisphere':True,
                'depth_is_metadata':True}


    def __init__(self, filename, **kwargs):
        '''
        Class for managing site details information

        Args:
            filename: File for a site details file containing
            header_sep: key value pairs in header information separtor (: , etc)
            northern_hemisphere: Bool describing if the pit location is in the
                                 northern_hemisphere for converting utms coords
            depth_is_metadata: Whether or not to include depth as a main
                              variable (useful for point data that contains
                              snow depth and other variables), profiles should
                              use depth as metadata
            kwargs: keyword values to pass to the database as metadata
        '''
        self.log = get_logger(__name__)

        self.extra_header = assign_default_kwargs(self, kwargs, self.defaults, leave=['epsg'])

        self.log.info('Interpretting metdaata in {}'.format(filename))

        # Site location files will have no data_name
        self.data_names = None

        # Does our profile type have multiple samples
        self.multi_sample_profiles = []

        # Read in the header into dictionary and list of columns names
        info, self.columns, self.header_pos = self._read(filename)

        # Interpret any data needing interpretation e.g. aspect
        self.info = self.interpret_data(info)

    def submit(self, session):
        '''
        Submit meta data to the database as site info, Do not use on profile
        headers. Only use on site_details files.

        Args:
            session: SQLAlchemy session object
        '''
        # only submit valid  keys to db
        kwargs = {}
        valid = get_table_attributes(SiteData)
        for k,v in self.info.items():
            if k in valid:
                 kwargs[k] = v

        kwargs = add_geom(kwargs, self.info['epsg'])
        d = SiteData(**kwargs)
        session.add(d)
        session.commit()

    def rename_sample_profiles(self, columns, data_names):
        '''
        Rename columns like density_a to density_sample_a
        '''
        result = []
        for c in columns:
            for data_name in data_names:
                    v = c
                    if data_name in c and c[-2]=='_':
                        v = c.replace(data_name, '{}_sample'.format(data_name))
                        result.append(v)

                    elif c not in result and c[-2] != '_':
                        result.append(c)
        return result

    def parse_column_names(self, lines):
        '''
        A flexible mnethod that attempts to find and standardize column names
        for csv data. Looks for a comma separated line with N entries == to the
        last line in the file. If an entry is found with more commas than the
        last line then we use that. This allows us to have data that doesn't
        have all the commas in the data (SSA typically missing the comma for
        veg unless it was notable)

        Assumptions:

        1. There is NOT greater than N commas in the header information prior to the column
        list

        2. The last line in file is of representative csv data

        3. The first column is numeric

        Args:
            lines: Complete list of strings from the file

        Returns:
            columns: list of column names
        '''

        # Minimum calumn size should match the last line of data (Assumption #2)
        n_columns = len(lines[-1].split(','))

        # Use these to monitor if a larger column count is found
        header_pos_options = [0, 0]
        header_lengths = [0, 0]

        for i,l in enumerate(lines):
            # Get rid of things in parenthese.
            clean_line = l.split(',')

            # column count
            n = len(clean_line)

            # Grab the columns header if we see one a little bigger
            if n >= n_columns:
                header_pos_options[0] = i
                header_lengths[0] = n

            # If we find a column count larger than the current replace it
            if header_lengths[0] > header_lengths[1]:
                header_lengths[1] = header_lengths[0]
                header_pos_options[1] = header_pos_options[0]

            # Break if we find number in the first position (Assumption #3)
            entry = clean_line[0].replace('-','').replace('.','')

            if entry.isnumeric():
                self.log.debug('Found end of header at line {}...'.format(i))
                header_pos_options[1] = i - 1
                break


        header_pos = header_pos_options[1]

        # Parse the columns header based on the size of the last line
        str_line = lines[header_pos]
        # Remove units
        for c in ['()','[]']:
            str_line = strip_encapsulated(str_line, c)

        raw_cols = str_line.strip('#').split(',')
        columns = [standardize_key(c) for c in raw_cols]

        # Rename any column names to more standard ones
        columns = remap_data_names(columns, self.rename)

        # Detmerine the profile type
        (self.data_names, self.multi_sample_profiles) = \
                                             self.determine_data_names(columns)

        self.data_names = remap_data_names(self.data_names, self.rename)

        if self.multi_sample_profiles:
            columns = self.rename_sample_profiles(columns, self.data_names)

        return columns, header_pos

    def determine_data_names(self, raw_columns):
        '''
        Determine the names of the data to be uploaded from the raw column
        header. Also determine if this is the type of profile file that will
        submit more than one main value (e.g. hand_hardness, grain size all in
        the same file)

        Args:
            raw_columns: list of raw text split on commas of the column names

        Returns:
            type: **data_names** - list of column names that will be uploaded
                   as a main value
                  **multi_sample_profiles** - boolean representing if we will
                    average the samples for a main value (e.g. density)
        '''
        # Names of columns we are going to submit as main values
        data_names = []
        multi_sample_profiles = []

        # String of the columns for counting
        str_cols =  ' '.join(raw_columns).replace(' ',"_").lower()

        for dname in self.available_data_names:

            kw_count = str_cols.count(dname)

            # if we have keyword match in our columns then add the type
            if kw_count > 0:
                data_names.append(dname)

                # Avoid triggering on depth and bottom depth in profiles
                if kw_count > 1 and dname != 'depth':
                    self.log.debug('{} is multisampled...'.format(dname))
                    multi_sample_profiles.append(dname)

        # If depth is metadata (e.g. profiles) then remove it as a main variable
        if 'depth' in data_names and self.depth_is_metadata:
            data_names.pop(data_names.index('depth'))

        if data_names:
            self.log.info('Names to be uploaded as main data are: {}'
                          ''.format(', '.join(data_names)))
        else:
            raise ValueError('Unable to determine data names from'
                            ' header/columns columns: {}'.format(", ".join(raw_columns)))

        if multi_sample_profiles:
            self.log.info('{} contains multiple samples for each '
                              'layer. The main value will be the average of '
                              'these samples.'.format(', '.join(multi_sample_profiles)))

        return data_names, multi_sample_profiles

    def _read(self, filename):
        '''
        Read in all site details file from the PITS folder under
        SnowEx2020_SQLdata If the filename has the word site in it then we
        read everything in the file. Otherwise we use this to read all the site
        data up to the header of the profile.

        E.g. Read all commented data until we see a column descriptor.

        Args:
            filename: Path to a csv containing # leading lines with site details

        Returns:
            tuple: **data** - Dictionary containing site details
                   **columns** - List of clean column names
                   **header_pos** - Index of the columns header for skiprows in
                                    read_csv
       '''

        with open(filename, encoding='latin') as fp:
            lines = fp.readlines()
            fp.close()

        # Site description files have no need for column lists
        if 'site' in filename.lower():
            self.log.info('Parsing site description header...')
            columns = None
            header_pos = None

            # Site location parses all of the file


        # Find the column names and where it is in the file
        else:
            columns, header_pos = self.parse_column_names(lines)
            self.log.debug('Column Data found to be {} columns based on Line '
                           '{}'.format(len(columns), header_pos))

            # Only parse what we know if the header
            lines = lines[0:header_pos]


        # Clean up the lines from line returns to grab header info
        lines = [l.strip() for l in lines]
        str_data = " ".join(lines).split('#')

        # Keep track of the number of lines with # in it for data opening
        self.length = len(str_data)

        # Key value pairs are separate by some separator provided.
        data = {}

        # Collect key value pairs from the information above the column header
        for l in str_data:
            d = l.split(self.header_sep)

            # Key is always the first entry in comma sep list
            k = standardize_key(d[0])

            # Avoid splitting on times
            if 'time' in k or 'date' in k:
                value = ':'.join(d[1:]).strip()
            else:
                value = ', '.join(d[1:])
                value = clean_str(value)

            # Assign non empty strings to dictionary
            if k and value:
                data[k] = value.strip(' ').replace('"','').replace('  ',' ')

        # If there is not header data then don't bother (useful for point data)
        if data:
            data = add_date_time_keys(data, timezone=self.timezone)

        # Rename the info dictionary keys to more standard ones
        data = remap_data_names(data, self.rename)
        self.log.debug('Discovered {} lines of valid header info.'
                       ''.format(len(data.keys())))

        return data, columns, header_pos

    def check_integrity(self, site_info):
        '''
        Compare the attritbute info to the site dictionary to insure integrity
        between datasets. Comparisons are only done as strings currently.

        In theory the site details header should contain identical info
        to the profile header, it should only have more info than the profile
        header.

        Args:
            site_info: Dictionary containing the site details file header

        Returns:
            mismatch: Dictionary with a message about how a piece of info is
                      mismatched

        '''
        mismatch = {}

        for k, v in self.info.items():
            if k not in site_info.keys():
                mismatch[k] = 'Key not found in site details'

            else:
                if v != site_info[k]:
                    mismatch[k] = 'Profile header != Site details header'

        return mismatch


    def interpret_data(self,  raw_info):
        '''
        Some data inside the headers is inconsistently noted. This function
        adjusts such data to the correct format.

        Adjustments include:

        A. Add in any extra info from the extra_header dictionary, defer to info
        provided by user

        B: Rename any keys that need to be renamed

        C. Aspect is recorded either cardinal directions or degrees from north,
        should be in degrees

        D. Cast UTM attributes to correct types. Convert UTM to lat long, store both


        Args:
            raw_info: Dictionary containing information to be parsed
        Returns:
            info: Dictionary of the raw_info containing interpetted info

        '''
        info = {}

        # A. Parse out any nans, nones or other not-data type entries
        for k,v in raw_info.items():
            info[k] = parse_none(raw_info[k])

        keys = info.keys()

        # Merge information, warn user about overwriting
        overwrite_keys = [k for k in keys if k in self.extra_header.keys()]

        if overwrite_keys:
            self.log.warning('Extra header information passed will overwrite '
                             'the following information found in the file '
                             'header:\n{}'.format(', '.join(overwrite_keys)))

        info.update(self.extra_header)

        # Convert slope and aspect to numbers
        info = manage_degrees(info)
        info = manage_aspect(info)

        # Convert lat/long to utm and vice versa if either exist
        info = reproject_point_in_dict(info, is_northern=self.northern_hemisphere)

        # Check for point data which will contain this in the data not the header
        if not is_point_data(self.columns):
            info = add_geom(info, self.epsg)

        # If columns or info does not have coordinates raise an error
        important = ['northing', 'latitude']

        cols_have_coords = []
        if self.columns != None:
            cols_have_coords = [c for c in self.columns if c in important]

        hdr_has_coords = [c for c in info if c in important]

        if not cols_have_coords and not hdr_has_coords:
            raise(ValueError('No geographic information was provided in the'
                            ' file header or via keyword arguments.'))
        return info
