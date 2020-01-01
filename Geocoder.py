import geocoder
import re
import sqlite3
import googlemaps
import Hidden

def geocode_addresses_osm(address):
    g = geocoder.osm(address)
    if g.latlng is None:
        address = re.findall('.*, (.*, .*)', address)[0]
        g = geocoder.osm(address)
        if g.latlng is None:
            address = re.findall('.*, (.*)', address)
            g = geocoder.osm(address)
    return g.latlng

def geocode_addresses_google(address):
    gmaps = googlemaps.Client(key=Hidden.google_api_key)
    g_json = gmaps.geocode(address)
    g = g_json[0]
    lat = g['geometry']['location']['lat']
    lng = g['geometry']['location']['lng']
    return [lat, lng]



def geocode_database(database_name, candidate_name):
    try:
        connector = sqlite3.connect(database_name)
    except:
        return

    cursor = connector.cursor()

    # join address and contribution id for
    #

