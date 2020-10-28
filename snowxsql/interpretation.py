'''
Module for intentional interpretation of data/scenarios. These are often
decisions being made about situations that are perhaps not universal but useful
in the context of snowex data and creating the database.
'''

from . utilities import get_logger
import pandas as pd
import warnings
import datetime
import numpy as np
import pytz


def is_point_data(columns):
    '''
    Searches the csv column names to see if the data set is point data,
    which will have latitude or easting in the columns. If it is, return True

    Args:
        columns: List of dataframe columns
    Return:
        result: Boolean indicating if the data is point data
    '''

    result = False

    # Check for point data which will contain this in the data not the header
    if columns != None and ('latitude' in columns or 'easting' in columns):
        result = True

    return result

def manage_degrees(info):
    '''
    Manages and interprets string values relating to degrees. Removes
    degrees symbols and interprets key word flat for slope.

    Args:
        info: Dictionary containing potential degrees entries to be converted
              to numbers
    Returns:
        info: Modificed dictionary containing string numeric representations of keys
              aspect and slope_angle
    '''

    # Manage degrees symbols
    for k in ['aspect','slope_angle','air_temp']:
        if k in info.keys():
            v = info[k]
            if type(v) == str and v != None:
                # Remove any degrees symbols
                v = v.replace('\u00b0','')
                v = v.replace('Â','')

                # Sometimes a range is used for the slope. Always pick the larger value
                if '-' in v:
                    v = v.split('-')[-1]

                if v.lower() == 'flat':
                    v = '0'

                if v.isnumeric():
                    v = float(v)
                info[k] = v
    return info

def manage_aspect(info):
    '''
    Manages when aspect is recorded in cardinal directions and converts it to
    a degrees from North float.

    Args:
        info: Dictionary potentially containing key aspect. Converts cardinal
    Returns:
        info: Dictionary with any key named aspect converted to  a float of degrees from north
    '''

    log = get_logger(__name__)

    # Convert Cardinal dirs to degrees
    if 'aspect' in info.keys():
        aspect = info['aspect']
        if aspect != None and type(aspect) == str:
            # Check for number of numeric values.
            numeric = len([True for c in aspect if c.isnumeric()])

            if numeric != len(aspect) and aspect != None:
                log.warning('Aspect recorded for site {} is in cardinal '
                'directions, converting to degrees...'
                ''.format(info['site_id']))
                deg = convert_cardinal_to_degree(aspect)
                info['aspect'] = deg
    return info


def convert_cardinal_to_degree(cardinal):
    '''
    Converts cardinal directions to degrees. Also removes any / or - that
    might get used to say between two cardinal directions

    e.g. S/SW turns into SSW which is interpetted as halfway between those
    two directions allowing for 22.5 degree increments.

    Args:
        Cardinal: Letters representing cardinal direction

    Returns:
        degrees: Float representing cardinal direction in degrees from north
    '''

    dirs = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW']

    # Manage extra characters separating composite dirs, make it all upper case
    d = ''.join([c.upper() for c in cardinal if c not in '/-'])

    # Assume West, East, South, Or North
    if len(d) > 3:
        d = d[0]
        warnings.warn("Assuming {} is {}".format(cardinal, d))

    if d in dirs:
        i = dirs.index(d)
        degrees = i * (360. / len(dirs))
    else:
        print(repr(d))
        raise ValueError('Invalid cardinal direction {}!'.format(cardinal))

    return degrees


def add_date_time_keys(data, timezone='MST'):
    '''
    Convert string info from a date/time keys in a dictionary to date and time
    objects and assign it back to the dictionary as date and time

    Args:
        data: dictionary containing either the keys date/time or two keys date
              and time
        timezone: String representing Pytz valid timezone

    Returns:
        d: Python Datetime object
    '''
    keys = data.keys()
    d = None

    # Look for a single header entry for date and time.
    for k in keys:
        if 'date' in k and 'time' in k:
            str_date = str(data[k].replace('T','-')) + ' ' + timezone
            d = pd.to_datetime(str_date)
            del data[k]
            break

    # If we didn't find date/time combined.
    if d == None:
        smp_keys = ['date','time']
        gpr_keys = ['utcyear', 'utcdoy', 'utctod']
        met_keys = ['year', 'doy', 'hour','minute']

        # Handle SMP data dates and times
        if 'date' in keys and 'time' in keys:
            dstr = ' '.join([str(data['date']), str(data['time']), timezone])
            d = pd.to_datetime(dstr)

        # Handle gpr data dates
        elif len([True for k in gpr_keys if k in keys])==3:
            base = pd.to_datetime('{:d}-01-01 00:00:00 '.format(int(data['utcyear'])) + 'UTC')

            # Number of days since january 1
            d = int(data['utcdoy']) - 1

            # Zulu time (time without colons)
            time = str(data['utctod'])
            hr = int(time[0:2]) # hours
            mm = int(time[2:4]) # minutes
            ss = int(time[4:6]) # seconds
            ms = int(float('0.' + time.split('.')[-1]) * 1000) # milliseconds

            delta = datetime.timedelta(days=d, hours=hr, minutes=mm, seconds=ss,
                                                                    milliseconds=ms)
            d = base.astimezone(pytz.timezone(timezone)) + delta

            # Remove them
            for v in gpr_keys:
                del data[v]

        # Handle met data dates
        elif len([True for k in met_keys if k in keys])==4:
            base = pd.to_datetime('{:d}-01-01 00:00:00 '.format(int(data['year'])) + 'UTC')

            # Number of days since january 1
            d = int(data['doy']) - 1

            hr = data['hour']
            mm = data['minute']

            delta = datetime.timedelta(days=d, hours=hr, minutes=mm)
            d = base.astimezone(pytz.timezone(timezone)) + delta

            # Remove them
            for v in met_keys:
                del data[v]

        else:
            raise ValueError('Data is missing date/time info!\n{}'.format(data))

    data['date'] = d.date()
    data['time'] = d.timetz()

    return data

def standardize_depth(depths, desired_format='snow_height', is_smp=False):
    '''
    Data that is a function of depth comes in 2 formats. Sometimes 0 is
    the snow surface, sometimes 0 is the ground. This function standardizes it
    for each profile. desired_format can be:

        snow_height: Zero at the bottom of the data.
        surface_datum: Zero at the top of the data and uses negative depths
                       (easier for plotting)

    Args:
        depths: Pandas series of depths in either format
        desired_format: string indicating which format the data is in
        is_smp: Boolean indicating which data this is, if smp then the data is
                surface_datum but with positive depths
   Returns:
        new:
    '''
    log = get_logger(__name__)

    max_depth = depths.max()
    min_depth = depths.min()

    new = depths.copy()

    # How is the depth ordered
    max_depth_at_top = depths.iloc[0] > depths.iloc[-1]

    # Is the data in surface_datum already
    bottom_is_negative = depths.iloc[-1] < 0

    if desired_format == 'snow_height':

        if is_smp:
            log.info('Converting SMP depths to snow height format.')
            new = (depths - max_depth).abs()

        elif bottom_is_negative:
            log.info('Converting depths in surface datum to snow height format.')

            new = (depths + abs(min_depth))

    elif desired_format == 'surface_datum':
        if is_smp:
            log.info('Converting SMP depths to surface datum format.')
            new = depths.mul(-1)

        elif not bottom_is_negative:
            log.info('Converting depths in snow height to surface datum format.')
            new = depths - max_depth

    else:
        raise ValueError('{} is an invalid depth format! Options are: {}'
                         ''.format(', '.join(['snow_height','surface_datum'])))
    return new

def avg_from_multi_sample(layer, value_type):
    '''
    Our database entries sometimes have multiple values. We want to extract
    those, cast them, average them and return the the value to be used as the main
    value in the database

    e.g.
        layer = {density_a: 180, density_b: 200, density_c: nan}
        result = 190

    Args:
        layer: layer dictionary (a single entry from a vertical profile)
        value_type: string labeling type of data were looking for (density, dielectric constant..)

    Returns:
        result: Nan mean of the values found
    '''
    values =[]

    for k, v in layer.items():
        if value_type in k:
            # If the bool is not nan and is not empty
            if str(v).lower() !='nan' and bool(str(v).strip()):
                values.append(float(v))

    if values:
        result = np.mean(np.array(values))
    else:
        result = np.nan
    return result

def get_InSar_flight_comment(data_name, desc):
    '''
    Takes an annotation file dictionary and forms a string for the Insar
    file to add to the description of the raster entry

    If the data is an amplitude file then it only gets a comment like:

        overpass duration : Feb 1 2020 2pm MST - Feb 1 2020 3pm MST

    If the data is a derived product then we assign both flight durations:

            1st overpass duration: Feb 1 2020 2pm MST - Feb 1 2020 3pm MST
            2nd overpass duration: Feb 2 2020 2pm MST - Feb 2 2020 3pm MST

    Args:
        f: Filename used to parse the data type (int, cor, amp1, amp2)
        desc: descriptor dictionary formed from the annotation file

    Returns:
        comment: A comment for the database for the uavsar file uploaded
    '''

    tz = pytz.timezone('MST')
    blank = '{} time of acquisition for pass {}'

    # Assign the correct date to the amplitude flights which dont require both flights
    if 'amplitude' in data_name:
        pass_num = data_name.split(' ')[-1]
        passes = [pass_num]
        comment = 'Overpass Duration: {} {} - {} {} (MST)'

    # Build a comment for both flights
    else:
        # Start stop times
        passes = ['1', '2']
        comment = '1st Overpass Duration: {} {} - {} {} (MST), '
        comment += '2nd Overpass Duration {} {} - {} {} (MST)'

    # Format the comment strings given the overpasses,
    times = []
    for n in passes:
        for timing in ['start', 'stop']:
            key = blank.format(timing, n)
            dt = desc[key]['value'].astimezone(tz)
            times.append(dt.date())
            times.append(dt.time())

    return comment.format(*times)
