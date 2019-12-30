import sqlite3
import pandas as panda
import geocoder
import re


def arrange_dataframe(dataframe):
    contributor_names = dataframe['contributor_name']
    contributor_street_address = dataframe['contributor_street_1']
    contributor_city = dataframe['contributor_city']
    contributor_state = dataframe['contributor_state']
    contributor_zip = dataframe['contributor_zip']
    contributor_occupation = dataframe['contributor_occupation']
    contributor_date = dataframe['contribution_receipt_date']
    contributor_contribution = dataframe['contribution_receipt_amount']
    contributor_address = dict()
    for i in range(len(contributor_street_address)):
        contributor_address[i] = str(contributor_street_address[i]) + ', ' + str(contributor_city[i]) + ', ' + str(contributor_state[i]) + ' ' + str(contributor_zip[i])
    return [contributor_names, contributor_address, contributor_contribution, contributor_date, contributor_occupation]


def candidate_database_populate(connector, cursor, candidate_name):
    fname = './Finance Data/' + candidate_name + '/' + candidate_name + '_raw_contribution_data.csv'
    dataframe = panda.read_csv(fname)
    contributor_info = arrange_dataframe(dataframe)

    relation_name = candidate_name + '_Contributions'
    cursor.execute('''CREATE TABLE IF NOT EXISTS ''' + relation_name + '''
                    (Contributor_id INTEGER, id INTEGER UNIQUE,
                    Name TEXT, Address TEXT, Contribution NUMERIC, Date TEXT, Occupation TEXT)''')

    start = None
    cursor.execute('SELECT max(id) FROM ' + relation_name)
    try:
        row = cursor.fetchone()
        if row is None:
            start = 0
        else:
            start = row[0]
    except:
        start = 0

    if start is None:
        start = 0

    contributor_id = 0
    for i in range(start, len(contributor_info[0])):
        cursor.execute('SELECT * FROM ' + relation_name + ' WHERE Name=? AND Address=?', (contributor_info[0][i], contributor_info[1][i]))
        try:
            row = cursor.fetchone()
            if row is not None:
                contributor_id = row[0]
            else:
                contributor_id += 1
        except:
            contributor_id += 1

        cursor.execute('''INSERT OR IGNORE INTO ''' + relation_name + '''
                    (Contributor_id, id, Name, Address, Contribution, Date, Occupation) VALUES
                    ( ?, ?, ?, ?, ?, ?, ?)''', (contributor_id, start, contributor_info[0][i], contributor_info[1][i], float(contributor_info[2][i]), contributor_info[3][i], contributor_info[4][i]) )
        start += 1
        if i % 10 == 0:
            connector.commit()
    return

def candidate_database_compress(connector, cursor, candidate_name):
    contributor_map = dict()
    relation_name = candidate_name + '_Contributions'

    cursor.execute('SELECT max(Contributor_id) FROM ' + relation_name)
    try:
        row = cursor.fetchone()
        if row is None:
            max_contributor_id = 0
        else:
            max_contributor_id = row[0]
    except:
        max_contributor_id = 0

    for contributor_id in range(max_contributor_id):
        contributor_id += 1
        contribution_total = 0
        num_contributions = 0
        contributor_add = ''
        cursor.execute('SELECT Contribution, Address FROM ' + relation_name + ' WHERE Contributor_id=?', (contributor_id, ))
        for row in cursor:
            contribution_total += row[0]
            contributor_add = row[1]
            num_contributions += 1
        contributor_map[contributor_id] = [contribution_total, num_contributions, contributor_add]
    create_compressed_relation(connector, cursor, candidate_name, contributor_map)
    return


def create_compressed_relation(connector, cursor, candidate_name, contributor_map):
    compressed_relation_name = candidate_name + '_Contributions_compressed'
    cursor.execute('''CREATE TABLE IF NOT EXISTS ''' + compressed_relation_name + '''
                            (Contributor_id INTEGER, Contribution NUMERIC, Num_Contributions INTEGER, Lat NUMERIC, Lng NUMERIC)''')
    for contributor_id,contribution_info in contributor_map.items():
        cont_latlng = geocode_addresses(contribution_info[2])
        if cont_latlng is not None:
            cont_lat = cont_latlng[0]
            cont_lng = cont_latlng[1]
        else:
            cont_lat = 0
            cont_lng = 0
        cont_id = contributor_id
        total_conts = contribution_info[0]
        num_conts = contribution_info[1]
        cursor.execute('INSERT OR IGNORE INTO ' + compressed_relation_name +
                       ' (Contributor_id, Contribution, Num_Contributions, Lat, Lng) VALUES ( ?, ?, ?, ?, ? )',
                       ( cont_id, total_conts, num_conts, cont_lat, cont_lng ))
        if contributor_id % 10 == 0:
            connector.commit()
    return

def geocode_addresses(address):
    g = geocoder.osm(address)
    if g.latlng is None:
        address = re.findall('.*, (.*, .*)', address)[0]
        g = geocoder.osm(address)
    return g.latlng

def main():
    database_name = 'raw_contribution_data.sqlite'
    connector = sqlite3.connect(database_name)

    candidate_names = ['Trump', 'Sanders', 'Warren', 'Buttigieg', 'Biden', 'Klobuchar', 'Yang']

    for candidate in candidate_names:
        cursor = connector.cursor()
        candidate_database_populate(connector, cursor, candidate)
        connector.commit()
        cursor.close()

    for candidate in candidate_names:
        cursor = connector.cursor()
        candidate_database_compress(connector, cursor, candidate)
        connector.commit()
        cursor.close()


if __name__  == '__main__':
    main()
