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
                        (Name, Address, Latitude, Longitude, Geocoded) VALUES ( ?, ?, ?, ? )''', (name, address, lat, lng, geocoded))
    cursor.close()
    return

def geocode_address_database(connector, candidate_name):
    cursor = connector.cursor()
    cursor2 = connector.cursor()
    relation_name = candidate_name + '_Addresses'
    cursor.execute('SELECT * FROM ' + relation_name + ' WHERE Geocoded=0')
    for row in cursor:
        cont_id = row[0]
        name = row[1]
        address = row[2]
        try:
            coordinates = Geocoder.geocode_addresses_osm(address)
        except:
            connector.commit()
            coordinates = [0, 0]
        geocoded = 1
        lat = coordinates[0]
        lng = coordinates[1]
        cursor2.execute(
            'INSERT OR IGNORE INTO ' + relation_name + '(Name, Address, Latitude, Longitude, Geocoded) VALUES ( ?, ?, ?, ?, ?, ? )',
            (cont_id, name, address, lat, lng, geocoded))
        if cont_id % 20 == 0:
            connector.commit()
    connector.commit()
    cursor2.close()
    cursor.close()
    return

def main():
    candidate_names = ['Trump', 'Sanders', 'Warren', 'Buttigieg', 'Biden', 'Klobuchar', 'Yang']
    database_name = 'raw_contribution_data.sqlite'
    connector = sqlite3.connect(database_name)
    # first attempt large batch of geocoding
    address_database_populate(connector, 'Sanders')
    connector.commit()


if __name__ == '__main__':
    main()
