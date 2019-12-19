import json
import googlemaps
from datetime import datetime

gmaps = googlemaps.Client(key='AIzaSyCjoOrgY8JDznjT9pVauI62XkVwVLL_nIQ')

geocode_result = gmaps.geocode('9630 1.5 Mile Road, East Leroy, MI 49051')

print(geocode_result)


# fname = './zipcodeGeoJSON/ri_rhode_island_zip_codes_geo.min.json'
# fhand = open(fname, 'r+')
#
# json1 = json.load(fhand)
# count = 0
# mapper = dict()
#
# for obj in json1['features']:
#     print(obj['properties']['ZCTA5CE10'])
#     mapper[obj['properties']['ZCTA5CE10']] = count % 10
#     count += 1
#     obj['properties']['funds'] = count % 10
#
# json.dump(json1, fhand)

#
# print(count)
# print(mapper)
#
# f2 = './zipcodeGeoJSON/RI_Weights.json'
# fhand2 = open(f2, 'w')
# obj1 = dict()
# obj1['type'] = 'weights'
# obj1['props'] = mapper
#
# json2 = json.dump(obj1, fhand2)
#
# fhand2.close()


