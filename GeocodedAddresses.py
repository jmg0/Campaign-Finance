import Geocoder
import Analysis
import pandas as panda
import Hidden
import sqlite3

def address_database_populate(connector, candidate_name):
    cursor = connector.cursor()
    fname = './Finance Data/' + candidate_name + '/' + candidate_name + '_raw_contribution_data.csv'
    dataframe = panda.read_csv(fname)
    contributor_info = Analysis.arrange_dataframe(dataframe)

    relation_name = candidate_name + '_Addresses'
    cursor.execute('''CREATE TABLE IF NOT EXISTS ''' + relation_name + '''
                    (Contributor_id INTEGER PRIMARY KEY, Name TEXT, Address TEXT, Latitude NUMERIC, Longitude NUMERIC, UNIQUE(Name, Address) ON CONFLICT IGNORE)''')

    for i in range(len(contributor_info[0])):
        name = contributor_info[0][i]
        address = contributor_info[1][i]
        cursor.execute('SELECT Contributor_id FROM ' + relation_name + ' WHERE Name=? AND Address=?', ( name, address ))
        try:
            row = cursor.fetchone()
            if row is not None:
                continue
        except:
            pass

        try:
            coordinates = Geocoder.geocode_addresses_osm(contributor_info[1][i])
        except:
            connector.commit()
            coordinates = ['not found', 'not found']
        lat = coordinates[0]
        lng = coordinates[1]
        cursor.execute('''INSERT OR IGNORE INTO ''' + relation_name + '''
                            (Name, Address, Latitude, Longitude) VALUES
                            ( ?, ?, ?, ? )''',
                       (name, address, lat, lng))
        if i % 10 == 0:
            connector.commit()
    return

def main():
    candidate_names = ['Trump', 'Sanders', 'Warren', 'Buttigieg', 'Biden', 'Klobuchar', 'Yang']
    database_name = 'raw_contribution_data.sqlite'
    connector = sqlite3.connect(database_name)
    address_database_populate(connector, 'Sanders')
    connector.commit()

if __name__ == '__main__':
    main()
