import sqlite3

def candidate_database_analysis(cursor, candidate_name):
    fname = './Finance Data'
    relation_name = candidate_name + '_Contributions'
    cursor.execute('''CREATE TABLE IF NOT EXISTS ''' + relation_name + '''
                    (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, 
                    Name TEXT, Address TEXT, Contribution REAL, Date TEXT)''')




def main():
    database_name = 'raw_contribution_data.sqlite'
    connector = sqlite3.connect(database_name)
    cursor = connector.cursor()

    candidate_names = ['Trump', 'Sanders', 'Warren', 'Buttigieg', 'Biden', 'Klobuchar', 'Yang']
    for candidate in candidate_names:
        candidate_database_analysis(cursor, candidate)
        #print(candidate)

    connector.commit()
    cursor.close()

if __name__  == '__main__':
    main()
