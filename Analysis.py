import sqlite3
import pandas as panda

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


def candidate_database_populate(cursor, candidate_name):
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
    for i in range(len(contributor_info[0])):
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
    return

# def candidate_database_compress(cursor, candidate_name):
#     contributor_map = dict()
#     relation_name = candidate_name + '_Contributions'
#     compressed_relation_name = candidate_name + '_Contributions_compressed'
#
#     cursor.execute('SELECT * FROM ' + relation_name)
#     for row in cursor:





def main():
    database_name = 'raw_contribution_data.sqlite'
    connector = sqlite3.connect(database_name)


    candidate_names = ['Trump', 'Sanders', 'Warren', 'Buttigieg', 'Biden', 'Klobuchar', 'Yang']
    for candidate in candidate_names:
        cursor = connector.cursor()
        candidate_database_populate(cursor, candidate)
        connector.commit()
        cursor.close()


if __name__  == '__main__':
    main()
