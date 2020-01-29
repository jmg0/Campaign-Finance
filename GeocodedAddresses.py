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
                coordinates = Geocoder.geocode_addresses_mapbox(address, key=Hidden.mapbox_api_token)
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
        address = str(row[1])
        lat = row[2]
        lng = row[3]
        if re.search('APO, AE', address) or re.search('APO, AA' , address) or re.search('APO, AP', address)  or re.search('FPO, AP', address) or re.search('FPO, AE', address) or re.search('FPO, AS', address) or re.search('FPO, AA', address) or re.search('DPO, AE', address) or re.search('nan, nan, nan nan', address) or re.search('INFORMATION REQUESTED', address):
            ids_to_be_removed.append(contributor_id)
            continue
        if re.search(' AK ', address) or re.search(' HI ', address) or re.search(' FL ', address) or re.search(' PR ', address) or re.search(' GU ', address) or re.search('SAIPAN', address) or re.search(' VI ', address) or re.search(' ZZ ', address) or re.search(' AS ', address):
            if lat == 0 or lng == 0:
                pass # needs to be re-geocoded
            else:
                continue
        if re.search('PO BOX', address):
            split_address = re.findall('.*, (.*,.*)', address)
            address = ''
            if split_address is not None:
                for piece in split_address:
                    address += piece + ' '
                address = address.rstrip()
        if re.search('nan, .*', address):
            split_address = re.findall('nan, (.*)', address)
            address = split_address[0]
        if re.search(' CT ', address) or re.search(' MA ', address) or re.search(' ME ', address) or re.search(' NH ', address) or re.search(' NJ ', address) or re.search(' RI ', address) or re.search(' VT ', address):
            zipcode = re.findall('.* ([0-9]*)', address)
            if zipcode is not None and len(zipcode[0]) == 4:
                new_zip = '0' + zipcode[0]
                address = address.replace(zipcode[0], new_zip)
        ids_to_be_fixed[contributor_id] = address
    print('TO BE REMOVED:', len(ids_to_be_removed))
    for c_id in ids_to_be_removed:
        print(c_id)
        cursor.execute('DELETE FROM ' + relation_name + ' WHERE Contributor_id=?', (c_id, ))
    connector.commit()
    print('TO BE FIXED:', len(ids_to_be_fixed))
    for c_id,address in ids_to_be_fixed.items():
        print(c_id, ' - ', address)
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
    for candidate in candidate_names:
        # STEP 1
        address_database_populate(connector, candidate)
        connector.commit()
        # STEP 2
        geocode_address_database(connector, candidate)
        connector.commit()
        # STEP 3
        fix_broken_addresses(connector, candidate)
        connector.commit()
    connector.commit()


if __name__ == '__main__':
    main()
