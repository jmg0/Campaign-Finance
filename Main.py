import sqlite3
import Analysis
import Geocoder

def main():
    database_name = 'raw_contribution_data.sqlite'
    connector = sqlite3.connect(database_name)

    candidate_names = ['Trump', 'Sanders', 'Warren', 'Buttigieg', 'Biden', 'Klobuchar', 'Yang']

    # STEP 1
    # transfer all contribution data into RDB
    # for candidate in candidate_names:
    #     cursor = connector.cursor()
    #     Analysis.candidate_database_populate(connector, cursor, candidate)
    #     connector.commit()
    #     cursor.close()

    # STEP 2
    # correct any broken contributor ids
    cursor = connector.cursor()
    Analysis.fix_cont_id(connector, cursor, 'Trump')
    connector.commit()
    cursor.close()


    # STEP 3
    # compress contribution data into single entry per person per address
    # for candidate in candidate_names:
    #     cursor = connector.cursor()
    #     Analysis.candidate_database_compress(connector, cursor, candidate)
    #     connector.commit()
    #     cursor.close()
    connector.close()

    #Geocoder.geocode_database(database_name, 'Sanders')

if __name__  == '__main__':
    main()
