import sqlite3
import Analysis
import Geocoder

def main():
    database_name = 'raw_contribution_data.sqlite'
    # connector = sqlite3.connect(database_name)
    #
    # candidate_names = ['Trump', 'Sanders', 'Warren', 'Buttigieg', 'Biden', 'Klobuchar', 'Yang']
    #
    # # transfer all contribution data into RDB
    # for candidate in candidate_names:
    #     cursor = connector.cursor()
    #     Analysis.candidate_database_populate(connector, cursor, candidate)
    #     connector.commit()
    #     cursor.close()
    #
    # # compress contribution data into single entry per person per address
    # for candidate in candidate_names:
    #     cursor = connector.cursor()
    #     Analysis.candidate_database_compress(connector, cursor, candidate)
    #     connector.commit()
    #     cursor.close()
    # connector.close()

    Geocoder.geocode_database(database_name, 'Sanders')



if __name__  == '__main__':
    main()
