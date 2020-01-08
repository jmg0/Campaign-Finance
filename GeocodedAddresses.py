import Geocoder
import Analysis
import pandas as panda
import numpy as np
import Hidden
import sqlite3

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
                coordinates = Geocoder.geocode_addresses_mapbox(address, key=Hidden.mapbox_api_token_3)
            except:
                connector.commit()
                coordinates = [0, 0]
            lat = coordinates[0]
            lng = coordinates[1]
            cursor.execute('UPDATE ' + relation_name + ' SET Latitude=?, Longitude=?, Geocoded=1 WHERE Contributor_id=?', (lat, lng, i) )
        connector.commit()
    connector.commit()
    return

def main():
    candidate_names = ['Trump', 'Sanders', 'Warren', 'Buttigieg', 'Biden', 'Klobuchar', 'Yang']
    database_name = 'raw_contribution_data.sqlite'
    connector = sqlite3.connect(database_name)
    # STEP 1
    #address_database_populate(connector, 'Warren')
    # STEP 2
    geocode_address_database(connector, 'Warren')
    connector.commit()


if __name__ == '__main__':
    main()
