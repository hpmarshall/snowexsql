'''
Module for functions that interpret various strings encountered in files.
These functions either prep, strip, or interpret strings for headers or
the actual data to be uploaded.
'''

import warnings
import pandas as pd
import datetime
import numpy as np

def clean_str(messy):
    '''
    Removes unwanted character in a str that we encounter alot
    '''
    clean = messy

    # Strip of any chars that are beginning and end
    for ch in [' ', '\n']:
        clean = clean.strip(ch)

    # Remove colons but not when its between numbers (e.g time)
    if ':' in clean:
        work = clean.split(' ')
        result = []

        for w in work:
            s = w.replace(':', '')
            if s.isnumeric():
                result.append(w)

            else:
                result.append(s)

        clean = ' '.join(result)

    # Remove characters anywhere in string that is undesireable
    for ch in ['"',"'"]:
        clean = clean.replace(ch, '')

    clean = clean.strip(' ')
    return clean

def standardize_key(messy):
    '''
    Preps a key for use in dataframe columns or dictionary. Makes everything
    lowercase, removes units, replaces spaces with underscores.

    Args:
        messy: string to be cleaned
    Returns:
        clean: String minus all characters and patterns of no interest
    '''
    key = messy

    # Remove units
    for c in ['()','[]']:
        key = strip_encapsulated(key, c)

    key = clean_str(key)
    key = key.lower().replace(' ','_')
    key = key.lower().replace('-','_')

    return key


def remap_data_names(original, rename_map):
    '''
    Remaps keys in a dictionary according to the rename dictionary. Also can be
    used for lists where the entries in the list can be renamed

    Args:
        original: list/dictionary of names and values that may need remapping
        rename: Dictionary mapping names (keys) {old: new}

    Returns:
        new: List/dictionary containing the names remapped

    '''
    remap_keys = rename_map.keys()

    if type(original) == dict:
        new = {}

        for k, v in original.items():

            if k in remap_keys:
                print(k, v)
                new_k = rename_map[k]

            # handle multisample names that need changing (e.g. dielectric_constant_a)
            elif k[-2] == '_':
                kw = k[0:-2]
                if kw in remap_keys:
                    new_k = k.replace(kw, rename_map[kw])

            else:
                new_k = k

            new[new_k] = v

    elif type(original) == list:
        new = []

        for i, v in enumerate(original):

            if v in remap_keys:
                new.append(rename_map[v])

            elif v[-2] == '_' and v[0:-2] in remap_keys:
                new.append(v.replace(v[0:-2], rename_map[v[0:-2]]))
            else:
                new.append(v)
    else:
        new = original.lower()
        if new in remap_keys:
            new = rename_map[new]

    return new

def get_encapsulated(str_line, encapsulator):
    '''
    Returns items found in the encapsulator, useful for finding units

    Args:
        str_line: String that has encapusulated info we want removed
        encapsulator: string of characters encapusulating info to be removed
    Returns:
        result: list of strings found inside anything between encapsulators

    e.g.
        line = 'density (kg/m^3), temperature (C)'
        ['kg/m^3', 'C'] = get_encapsulated(line, '()')
    '''

    result = []

    if len(encapsulator) > 2:
        raise ValueError('encapsulator can only be 1 or 2 chars long!')

    elif len(encapsulator) == 2:
        lcap = encapsulator[0]
        rcap = encapsulator[1]

    else:
        lcap = rcap = encapsulator

    # Split on the lcap
    if lcap in str_line:
        for i, val in enumerate(str_line.split(lcap)):
            # The first one will always be before our encapsulated
            if i != 0:
                if lcap != rcap:
                    result.append(val[0:val.index(rcap)])
                else:
                    result.append(val)

    return result

def strip_encapsulated(str_line, encapsulator):
    '''
    Removes from a str anything thats encapusulated by characters and the
    encapsulating chars themselves

    Args:
        str_line: String that has encapusulated info we want removed
        encapsulator: string of characters encapsulating info to be removed
    Returns:
        final: String without anything between encapsulators
    '''
    final = str_line
    result = get_encapsulated(final, encapsulator)

    if len(encapsulator) == 2:
        lcap = encapsulator[0]
        rcap = encapsulator[1]

    else:
        lcap = rcap = encapsulator

    # Remove all the encapsulated words
    for v in result:
        final = final.replace(lcap + v + rcap,'')

    # Make sure we remove the last one
    return final

def parse_none(value):
    '''
    parses values looking for NANs, Nones, etc...

    Args:
        value: Value potentially containing a none or nan

    Returns:
        result: If string value is nan or none, then return None type otherwise
                return original value
    '''
    result = value
    dtype = type(value)

    # If its a nan or none or the string is empty
    if dtype == str:
        if value.lower() in ['nan', 'none'] or not value:
            result = None
    elif dtype == float:
        if np.isnan(value):
            result = None

    return result

def kw_in_here(kw, d, case_sensitive=True):
    '''
    Determines if the keyword is found in any of the entries in the List
    If any match is found returns true

    Can use a list or dictionary. If a dictionary is supplied the keys will be
    used

    e.g.

    dielectric_constant is found in [temperature, dielectric_constant_a]

    Args:
        kw: Keyword we're searching for
        d: List or dictionary with keys of strings
        case_sensitive: Boolean indicating whether it should be case sensitive
                        or not

    Returns:
        Bool: Indicating the keyword was found

    '''
    if isinstance(d, dict):
        d_keys = d.keys()
    else:
        d_keys = d

    if not case_sensitive:
        k = kw.lower()
        d_keys = [c.lower() for c in d_keys]

    else:
        k = kw

    truth = [True for c in d_keys if k in c]
    return len(truth) > 0
