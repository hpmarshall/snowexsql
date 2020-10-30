'''
Used once to parse the met names and create dictionaries of
units, renames, and instruments.
The copy those into the __init__.py to create usable data objects in the scripts.

'''
import pprint
pp = pprint.PrettyPrinter(indent=2)

with open('met_names.txt') as fp:
    lines = fp.readlines()
    fp.close()

with open('met_columns.txt') as fp:
    columns = fp.readlines()
    fp.close()

columns = [c.strip() for c in columns]


rename = {}
instruments = {}
units = {}

special_cols = [c for c in columns if 'IRtarget' in c or 'TCstringC' in c]

# '''IRtarget_nadir1_C_Avg	IRtarget_nadir2_C_Avg
# IRtarget_30degN_C_Avg	IRtarget_30degS_C_Avg
# TCstringC_s50cm_Avg	TCstringC_s40cm_Avg	TCstringC_s30cm_Avg
# TCstringC_s20cm_Avg	TCstringC_s10cm_Avg	TCstringC_s5cm_Avg
# TCstringC_s2cm_Avg	TCstringC_0cm_Avg	TCstringC_2cm_Avg
# TCstringC_5cm_Avg	TCstringC_10cm_Avg	TCstringC_20cm_Avg
# TCstringC_30cm_Avg	TCstringC_40cm_Avg	TCstringC_50cm_Avg
# TCstringC_75cm_Avg	TCstringC_100cm_Avg	TCstringC_125cm_Avg
# TCstringC_150cm_Avg TCstringC_175cm_Avg	TCstringC_200cm_Avg
# TCstringC_225cm_Avg	TCstringC_250cm_Avg	TCstringC_275cm_Avg
# TCstringC_300cm_Avg''')

special_cols = [c.strip().lower() for c in special_cols]

# special keyword
specials = ['IRtarget_XXX','IRsensor_XXX', 'TCstringC-XX', 'TCstringC-sXX']

# Build the maps
for i,line in enumerate(lines):
    data = line.split(',')
    name = data[2].lower().strip().replace(' ','_').replace('/','_')
    original_name = data[0]
    rename[original_name.lower()] = name.lower()
    instruments[name] = data[1].lower().strip()
    units[name] = data[3].lower().strip().replace('(','').replace(')','')

# Apply special interpretation for the pattern names
extras = {'30degN':'30_degrees_north', '30degS':'30_degrees_south'}

rename_keys = rename.keys()
print(rename_keys)

columns_mapping = {}
units_mapping = {}
instruments_mapping = {}
for c in columns:
    name = None

    info = c.split('_')
    key = info[0].lower()

    # Map it
    if key in rename_keys:
        name = rename[key]

    elif 'TCstringC_s' in c:
        name = rename['tcstringc-sxx']

    elif 'TCstringC' in c:
        name = rename['tcstringc-xx']

    elif 'IRtarget' in c:
        name = rename['irtarget_xxx']

    # Special conditions
    if name != None:
        add = ''
        #print(c)
        if 'TCstringC_s' in c or 'sm' == key or 'st' == key or 'TC' == c[0:2]:
            dist = info[1]
            add = '_{}_below_ground'.format(dist)

        elif 'ft' in c or 'cm' in c:
            dist = info[1]
            add = '_{}_above_ground'.format(dist)

        elif 'IR' in c:
            direction = info[1]
            if direction in extras.keys():
                add = extras[direction]
            else:
                add = direction
            add = '_{}'.format(add)

        instrument = instruments[name]
        unit = units[name]

        name += add
        columns_mapping[c.lower()] = name
        units_mapping[name] = unit
        instruments_mapping[name] = instrument

    # if 'ft'in c:
    #     c.split('_')[1]
    # if 'cm' in c:
    #     print(c)

print('\nRENAME DICTIONARY:')
pp.pprint(columns_mapping)

print('\nINSTRUMENTS DICTIONARY:')
pp.pprint(instruments_mapping)

print('\nUNITS DICTIONARY:')
pp.pprint(units_mapping)

print('Availble names')
for c in columns_mapping.values():
    print("'{}',".format(c))
