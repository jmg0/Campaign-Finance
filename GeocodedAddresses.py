import Geocoder
import Analysis
import pandas as panda
import numpy as np
import Hidden
import sqlite3
import re

def address_database_populate(connector, candidate_name):
    cursor = connector.cursor()
    fname = './Finance Data/' + candidate_name + '/' + candidate_name + '_raw_contribution_data.csv'
    dataframe = panda.read_csv(fname, dtype=Hidden.data_type)

    relation_name = candidate_name + '_Addresses'
    cursor.execute('''CREATE TABLE IF NOT EXISTS ''' + relation_name + '''
                    (Contributor_id INTEGER PRIMARY KEY, Name TEXT, Address TEXT, Latitude NUMERIC, Longitude NUMERIC, Geocoded INTEGER, UNIQUE(Name, Address) ON CONFLICT REPLACE)''')

    for index,row in dataframe.iterrows():
        name = row['contributor_name']
        address = str(row['contributor_street_1']) + ', ' + str(row['contributor_city']) + ', ' + str(row['contributor_state']) + ' ' + str(row['contributor_zip'])
        lat = 0
        lng = 0
        geocoded = 0
        cursor.execute('''INSERT OR IGNORE INTO ''' + relation_name + '''
                        (Name, Address, Latitude, Longitude, Geocoded) VALUES ( ?, ?, ?, ?, ? )''', (name, address, lat, lng, geocoded))
    cursor.close()
    return

def geocode_address_database(connector, candidate_name):
    cursor = connector.cursor()
    relation_name = candidate_name + '_Addresses'
    cursor.execute('SELECT max(Contributor_id) FROM ' + relation_name)
    max_id = cursor.fetchone()[0]
    for i in range(max_id):
        cursor.execute('SELECT Name,Address FROM ' + relation_name + ' WHERE Contributor_id=? AND Geocoded=0', (i,) )
        row = cursor.fetchone()
        if row is None:
            continue
        else:
            try:
                address = row[1]
                #coordinates = Geocoder.geocode_addresses_osm(address)
                #coordinates = Geocoder.geocode_addresses_locationIQ(address, key=Hidden.locationIQ_api_key)
                coordinates = Geocoder.geocode_addresses_mapbox(address, key=Hidden.mapbox_api_token_2)
            except:
                connector.commit()
                coordinates = [0, 0]
            lat = coordinates[0]
            lng = coordinates[1]
            cursor.execute('UPDATE ' + relation_name + ' SET Latitude=?, Longitude=?, Geocoded=1 WHERE Contributor_id=?', (lat, lng, i) )
        connector.commit()
    connector.commit()
    cursor.close()
    return

def fix_broken_addresses(connector, candidate_name):
    cursor = connector.cursor()
    relation_name = candidate_name + '_Addresses'
    compressed_relation_name = candidate_name + '_Contributions_compressed'
    ids_to_be_fixed = dict()
    ids_to_be_removed = list()
    cursor.execute('SELECT Contributor_id, Address, Latitude, Longitude FROM ' + relation_name + ' WHERE Latitude<25 OR Latitude>49 OR Longitude>-65')
    for row in cursor:
        contributor_id = row[0]
        address = row[1]
        lat = row[2]
        lng = row[3]
        if 'APO, AE' or 'APO, AA' or 'APO, AP'  or 'FPO, AP' or 'FPO, AE' or 'FPO, AS' or 'FPO, AA' or 'nan, nan, nan nan'.upper() in address.upper():
            ids_to_be_removed.append(contributor_id)
            continue
        if ' AK ' or ' HI ' or ' PR ' or ' GU ' or 'SAIPAN' or ' VI ' or ' ZZ ' in address.upper():
            if lat == 0 or lng == 0:
                pass # needs to be re-geocoded
            else:
                continue
        if 'PO BOX' in address.upper():
            address = re.findall('.*, (.*,.*)', address)
        if ' CT ' or ' MA ' or ' ME ' or ' NH ' or ' NJ ' or ' RI ' or ' VT ' in address.upper():
            zipcode = re.findall('.* ([0-9]*)', address)
            if zipcode is not None and len(zipcode[0]) == 4:
                new_zip = '0' + zipcode[0]
                address = address.replace(zipcode[0], new_zip)
        ids_to_be_fixed[contributor_id] = [address]
    for c_id in ids_to_be_removed:
        print(c_id)
        cursor.execute('DELETE FROM ' + compressed_relation_name + ' WHERE Contributor_id=?', (c_id, ))
    connector.commit()
    for c_id,address in ids_to_be_fixed.items():
        try:
            coordinates = Geocoder.geocode_addresses_google(address, Hidden.google_api_key)
        except:
            coordinates = [0,0]
        lat = coordinates[0]
        lng = coordinates[1]
        cursor.execute('UPDATE ' + relation_name + ' SET Latitude=?, Longitude=? WHERE Contributor_id=?', (lat, lng, c_id ) )
    connector.commit()
    cursor.close()
    return

def main():
    candidate_names = ['Trump', 'Sanders', 'Warren', 'Buttigieg', 'Biden', 'Klobuchar', 'Yang']
    database_name = 'raw_contribution_data.sqlite'
    connector = sqlite3.connect(database_name)
    # STEP 1
    #address_database_populate(connector, 'Yang')
    # STEP 2
    #geocode_address_database(connector, 'Yang')
    # STEP 3
    #fix_broken_addresses(connector, 'Trump')
    connector.commit()


if __name__ == '__main__':
    main()
