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
                coordinates = Geocoder.geocode_addresses_osm(row[1])
            except:
                connector.commit()
                coordinates = [0, 0]
            lat = coordinates[0]
            lng = coordinates[1]
            cursor.execute('UPDATE ' + relation_name + ' SET Latitude=?, Longitude=?, Geocoded=1 WHERE Contributor_id=?', (lat, lng, i) )
        connector.commit()
    connector.commit()
    # return
    #
    #
    # geocoded_addresses = dict()
    # for row in cursor:
    #     cont_id = row[0]
    #     name = row[1]
    #     address = row[2]
    #     try:
    #         coordinates = Geocoder.geocode_addresses_osm(address)
    #     except:
    #         connector.commit()
    #         coordinates = [0, 0]
    #     lat = coordinates[0]
    #     lng = coordinates[1]
    #     geocoded_addresses[cont_id] = [name, address, lat, lng]
    #
    # for geo_id,geo_info in geocoded_addresses.items():
    #     cursor.execute('UPSERT INTO ' + relation_name + '(Contributor_id, Name, Address, Latitude, Longitude, Geocoded) VALUES ( ?, ?, ?, ?, ?, ? )',
    #                    (geo_id, geo_info[0], geo_info[1], geo_info[2], geo_info[3], 1))
    #     if geo_id % 20 == 0:
    #         connector.commit()
    # connector.commit()
    # cursor.close()
    # return

def main():
    candidate_names = ['Trump', 'Sanders', 'Warren', 'Buttigieg', 'Biden', 'Klobuchar', 'Yang']
    database_name = 'raw_contribution_data.sqlite'
    connector = sqlite3.connect(database_name)
    #print('step 1')
    #address_database_populate(connector, 'Sanders')
    #print('step 2')
    geocode_address_database(connector, 'Sanders')
    connector.commit()


if __name__ == '__main__':
    main()
