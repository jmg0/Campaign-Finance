import sqlite3
import numpy as np
import Hidden
import pandas as panda

def candidate_database_populate(connector, cursor, candidate_name):
    fname = './Finance Data/' + candidate_name + '/' + candidate_name + '_raw_contribution_data.csv'
    dataframe = panda.read_csv(fname, dtype=Hidden.data_type)
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
    contributor_ids = list()

    cursor.execute('SELECT max(Contributor_id) FROM ' + relation_name)
    max_cont_id = 0
    try:
        row = cursor.fetchone()
        if row is None:
            max_cont_id = 0
        else:
            max_cont_id = row[0]
    except:
        max_cont_id = 0


    for index,row in dataframe.iterrows():
        name = str(row['contributor_name'])
        address = str(row['contributor_street_1']) + ', ' + str(row['contributor_city']) + ', ' + str(row['contributor_state']) + ' ' + str(row['contributor_zip'])
        contribution = row['contribution_receipt_amount']
        date = str(row['contribution_receipt_date'])
        occupation = str(row['contributor_occupation'])

        cursor.execute('SELECT * FROM ' + relation_name + ' WHERE Name=? AND Address=?', (name, address))
        try:
            row = cursor.fetchone()
            if row is not None:
                contributor_id = row[0]
            else:
                if max(contributor_ids) > max_cont_id:
                    max_cont_id = max(contributor_ids)
                contributor_id = max_cont_id + 1
        except:
            contributor_id += 1

        if contributor_id not in contributor_ids:
            contributor_ids.append(contributor_id)

        cursor.execute('''INSERT OR IGNORE INTO ''' + relation_name + '''
                            (Contributor_id, id, Name, Address, Contribution, Date, Occupation) VALUES
                            ( ?, ?, ?, ?, ?, ?, ?)''', (contributor_id, start, name, address, float(contribution), date, occupation) )
        start += 1
        if index % 10 == 0:
            connector.commit()
    connector.commit()
    return

def fix_cont_id(connector, cursor, candidate_name):
    relation_name = candidate_name + '_Contributions'
    contributor_id_map = dict()
    contributor_id = 1
    cursor.execute('SELECT * FROM ' + relation_name)
    for row in cursor:
        contributor_name = row[2]
        contributor_address = row[3]
        contributor = (contributor_name, contributor_address)
        if contributor_id_map.get(contributor) is None:
            contributor_id_map[contributor] = contributor_id
            contributor_id += 1

    for contributors,cont_ids in contributor_id_map.items():
        cursor.execute('UPDATE ' + relation_name + ' SET Contributor_id=? WHERE Name=? AND Address=?', (cont_ids, contributors[0], contributors[1]))
        if cont_ids % 10 == 0:
            connector.commit()
    connector.commit()
    return

def candidate_database_compress(connector, cursor, candidate_name):
    contributor_map = dict()
    relation_name = candidate_name + '_Contributions'

    compressed_relation_name = candidate_name + '_Contributions_compressed'
    cursor.execute('''CREATE TABLE IF NOT EXISTS ''' + compressed_relation_name + '''
                                (Contributor_id INTEGER, Contribution NUMERIC, Num_Contributions INTEGER)''')

    cursor.execute('SELECT max(Contributor_id) FROM ' + compressed_relation_name)
    try:
        row = cursor.fetchone()
        if row is None:
            contributor_id = 1
        else:
            contributor_id = row[0]+1
    except:
        contributor_id = 1

    for contributor_id in range(contributor_id):
        contributor_id += 1
        contribution_total = 0
        num_contributions = 0
        cursor.execute('SELECT Contribution FROM ' + relation_name + ' WHERE Contributor_id=?', (contributor_id, ))
        for row in cursor:
            contribution_total += row[0]
            num_contributions += 1
        contributor_map[contributor_id] = [contribution_total, num_contributions]
    create_compressed_relation(connector, cursor, candidate_name, contributor_map)
    return


def create_compressed_relation(connector, cursor, candidate_name, contributor_map):
    compressed_relation_name = candidate_name + '_Contributions_compressed'
    cursor.execute('''CREATE TABLE IF NOT EXISTS ''' + compressed_relation_name + '''
                            (Contributor_id INTEGER, Contribution NUMERIC, Num_Contributions INTEGER)''')
    for contributor_id,contribution_info in contributor_map.items():
        cont_id = contributor_id
        total_conts = contribution_info[0]
        num_conts = contribution_info[1]
        cursor.execute('INSERT OR IGNORE INTO ' + compressed_relation_name +
                       ' (Contributor_id, Contribution, Num_Contributions ) VALUES ( ?, ?, ? )',
                       ( cont_id, total_conts, num_conts ))
        if contributor_id % 10 == 0:
            connector.commit()
    return
