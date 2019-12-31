import geocoder
import re
import sqlite3

def geocode_addresses(address):
    g = geocoder.osm(address)
    if g.latlng is None:
        address = re.findall('.*, (.*, .*)', address)[0]
        g = geocoder.osm(address)
    return g.latlng

def geocode_database(database_name, candidate_name):
    try:
        connector = sqlite3.connect(database_name)
    except:
        return

    cursor = connector.cursor()

    # join address and contribution id for
    #

