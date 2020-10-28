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

rename = {}
instruments = {}
units = {}
#
special_cols = (
'''IRtarget_nadir1_C_Avg	IRtarget_nadir2_C_Avg
IRtarget_30degN_C_Avg	IRtarget_30degS_C_Avg
TCstringC_s50cm_Avg	TCstringC_s40cm_Avg	TCstringC_s30cm_Avg
TCstringC_s20cm_Avg	TCstringC_s10cm_Avg	TCstringC_s5cm_Avg
TCstringC_s2cm_Avg	TCstringC_0cm_Avg	TCstringC_2cm_Avg
TCstringC_5cm_Avg	TCstringC_10cm_Avg	TCstringC_20cm_Avg
TCstringC_30cm_Avg	TCstringC_40cm_Avg	TCstringC_50cm_Avg
TCstringC_75cm_Avg	TCstringC_100cm_Avg	TCstringC_125cm_Avg
TCstringC_150cm_Avg TCstringC_175cm_Avg	TCstringC_200cm_Avg
TCstringC_225cm_Avg	TCstringC_250cm_Avg	TCstringC_275cm_Avg
TCstringC_300cm_Avg''')

special_cols = [c.strip() for c in special_cols.replace('\t', ' ').replace('\n', ' ').split(' ') if c.strip()]

# special keyword
specials = ['IRtarget_XXX','IRsensor_XXX', 'TCstringC-XX', 'TCstringC-sXX']

for i,line in enumerate(lines):
    data = line.split(',')

    name = data[2].lower().strip().replace(' ','_').replace('/','_')
    original_name = data[0]
    rename[original_name] = name
    instruments[name] = data[1].lower().strip()
    units[name] = data[3].lower().strip().replace('(','').replace(')','')

# Apply special interpretation for the pattern names
extras = {'30degN':'30_degrees_north', '30degS':'30_degrees_south'}
for c in special_cols:
    for s in specials:
        add = None
        if 'TCstringC-s' in c:
            dist = c.split('_')[1]
            add = '_{}_below_soil'.format(dist)

        elif 'TCstringC' in c:
            dist = c.split('_')[1]
            add = '_{}_above_soil'.format(dist)

        elif 'IRtarget' in c:
            location = c.split('_')[1]
            if location in extras.keys():
                location = extras[location]
            add = '_{}'.format(location)

        if add != None:
            orig_name = rename[s]
            name = orig_name + add
            rename[c] = name
            instruments[name] = instruments[orig_name]
            units[name] = units[orig_name]

            break

print('\nRENAME DICTIONARY:')
pp.pprint(rename)

print('\nINSTRUMENTS DICTIONARY:')
pp.pprint(instruments)

print('\nUNITS DICTIONARY:')
pp.pprint(units)

pp.pprint(rename.values())
