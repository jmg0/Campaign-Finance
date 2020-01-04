import geocoder
import re
import sqlite3
import googlemaps
import Hidden
from arcgis.geocoding import geocode
import requests
from mapbox import Geocoder as gc

# unlimited if following OSM rules and not using too much
def geocode_addresses_osm(address):
    g = geocoder.osm(address)
    if g.latlng is None:
        address = re.findall('.*, (.*, .*)', address)[0]
        g = geocoder.osm(address)
        if g.latlng is None:
            address = re.findall('.*, (.*)', address)
            g = geocoder.osm(address)
    return g.latlng

# rate limit unchanging?
def geocode_addresses_mapbox(address, key):
    geocoder = gc(access_token=key)
    response = geocoder.forward(address, limit=1)
    print(response.headers['X-Rate-Limit-Limit'])
    coordinates = response.json()['features'][0]['geometry']['coordinates']
    lat = coordinates[1]
    lng = coordinates[0]
    return [lat, lng]

# ~60,000 per key
def geocode_addresses_google(address, key):
    gmaps = googlemaps.Client(key=key)
    g_json = gmaps.geocode(address)
    g = g_json[0]
    lat = g['geometry']['location']['lat']
    lng = g['geometry']['location']['lng']
    return [lat, lng]

# 10,000 per key per day
def geocode_addresses_locationIQ(address, key):
    url = "https://us1.locationiq.com/v1/search.php"
    data = {
        'key': key,
        'q': address,
        'format': 'json'
    }
    response = requests.get(url, params=data).json()
    lat = float(response[0]['lat'])
    lng = float(response[0]['lon'])
    return [lat, lng]

def geocode_addresses_arcgis(address):
    geocode(address)


def geocode_database(database_name, candidate_name):
    try:
        connector = sqlite3.connect(database_name)
    except:
        return
    cursor = connector.cursor()

    geocoded_contributors = dict()
    geocoded_RDB_name = candidate_name + '_Geocoded'
    candidate_RDB = candidate_name + '_Contributions'
    address_series = candidate_RDB + '.Address'
    cont_id_series = candidate_RDB + '.Contributor_id'
    compressed_RDB = candidate_RDB + '_compressed'
    comp_cont_id_series = compressed_RDB + '.Contributor_id'
    contribution_series = compressed_RDB + '.Contribution'

    create_table_query = 'CREATE TABLE IF NOT EXISTS ' + geocoded_RDB_name + ' (Contributor_id INTEGER, Contribution NUMERIC, Latitude NUMERIC, Longitude NUMERIC)'
    cursor.execute(create_table_query)

    # get starting value
    cursor.execute('SELECT max(Contributor_id) FROM ' + geocoded_RDB_name)
    start = None
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

    retrieve_data_query = 'SELECT DISTINCT ' + comp_cont_id_series + ', ' + contribution_series + ', ' + address_series + \
                          ' FROM ' + compressed_RDB + ' JOIN ' + candidate_RDB + ' ON ' + comp_cont_id_series + \
                          ' = ' + cont_id_series + ' AND ' + comp_cont_id_series + ' > ' + start
    cursor.execute(retrieve_data_query)
    for row in cursor:
        contributor_id = row[0]
        contribution = row[1]
        address = row[2]
        coordinates = list() # will = geocode_XX(address) method and return [lat, lng]
        lat = coordinates[0]
        lng = coordinates[1]
        insert_entry_query = 'INSERT OR IGNORE INTO ' + geocoded_RDB_name + \
                             ' (Contributor_id INTEGER, Contribution NUMERIC, Latitude NUMERIC, Longitude NUMERIC)' + \
                             ' VALUES ( ?, ?, ?, ? )'
        cursor.execute(insert_entry_query, (contributor_id, contribution, lat, lng) )
        if contributor_id % 25 == 0:
            connector.commit()

    connector.commit()
    cursor.close()
    connector.close()

    return

