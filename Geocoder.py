import geocoder
import re
import sqlite3
import googlemaps
import Hidden
from arcgis.geocoding import geocode
import requests

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
    geocoded_RBD_name = candidate_name + '_Geocoded'
    candidate_RDB = candidate_name + '_Contributions'
    address = candidate_RDB + '.Address'
    cont_id = candidate_RDB + '.Contributor_id'
    compressed_RDB = candidate_RDB + '_compressed'
    comp_cont_id = compressed_RDB + '.Contributor_id'
    contribution = compressed_RDB + '.Contribution'

    create_table_query = 'CREATE TABLE IF NOT EXISTS ' + geocoded_RBD_name + ' (Contributor_id INTEGER, Latitude NUMERIC, Longitude NUMERIC)'
    retrieve_data_query = 'SELECT DISTINCT ' + comp_cont_id + ', ' + contribution + ', ' + address + \
                          ' FROM ' + compressed_RDB + ' JOIN ' + candidate_RDB + ' ON ' + comp_cont_id + \
                          ' = ' + cont_id

    cursor.execute(retrieve_data_query)
    for row in cursor:
        contributor_id = row[0]
        contribution = row[1]
        address = row[2]

    cursor.close()
    connector.close()

    return

