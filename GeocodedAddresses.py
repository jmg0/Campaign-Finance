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
                    (Contributor_id INTEGER, Name TEXT, Address TEXT, Latitude NUMERIC, Longitude NUMERIC)''')

    # get starting value
    cursor.execute('SELECT max(Contributor_id) FROM ' + relation_name)
    cont_id = None
    try:
        row = cursor.fetchone()
        if row is None:
            cont_id = 0
        else:
            cont_id = row[0]
    except:
        cont_id = 0
    if cont_id is None:
        cont_id = 0

    contributor_ids = list()
    for i in range(cont_id, len(contributor_info[0])):
        cursor.execute('SELECT * FROM ' + relation_name + ' WHERE Name=? AND Address=?', (contributor_info[0][i], contributor_info[1][i]))
        try:
            row = cursor.fetchone()
            if row is not None:
                cont_id = max(contributor_ids)
                continue
        except:
            pass

        cont_id += 1

        if cont_id not in contributor_ids:
            contributor_ids.append(cont_id)

        try:
            coordinates = Geocoder.geocode_addresses_osm(contributor_info[1][i])
        except:
            connector.commit()
            coordinates = ['not found', 'not found']
        lat = coordinates[0]
        lng = coordinates[1]
        cursor.execute('''INSERT OR IGNORE INTO ''' + relation_name + '''
                    (Contributor_id, Name, Address, Latitude, Longitude) VALUES
                    ( ?, ?, ?, ?, ? )''', (cont_id, contributor_info[0][i], contributor_info[1][i], lat, lng ) )
        if i % 25 == 0:
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