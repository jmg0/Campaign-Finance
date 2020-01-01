import json
import googlemaps
from datetime import datetime
import geocoder
import re
import pandas as pd
import Geocoder


addy = '7789 WILBURN RD, PANGBURN, AR 72121'
print(Geocoder.geocode_addresses_osm(addy))
print(Geocoder.geocode_addresses_google(addy))


# try:
#     df = pd.read_csv('mycsv.csv')
# except IOError:
#     df = None
#
# if df is None:
#     start = 0
# else:
#     start = len(df['Contribution Total'])
#
# print(start)






# start = 2
# x = [1, 2, 3, 4, 5]
# for i in range(start, len(x)):
#     print(x[i])

# g = geocoder.osm('SPOONER, WI 54801')
# print(g.latlng) # returns [lat, lng] [40.7127281, -74.0060152]
# g = geocoder.geonames('New York City')
# print(g.latlng)

# string = '5317 ASPEN DR, 5317 ASPEN DR, 5317 ASPEN DR, asdf, OKLAHOMA CITY, OK 73118'
# add1 = re.findall('.*, (.*, .*)', string)[0]
# print(add1)
# address = re.split(',', string)
# print((address[-2] + ',' + address[-1]).strip())




#
#
# geocode_result = gmaps.geocode('9630 1.5 Mile Road, East Leroy, MI 49051')
#
# print(geocode_result)




# QUOTA LIMIT OF 0 HAS BEEN SET FOR GOOGLE ACCOUNT MAKE SURE TO UNDO THAT AT
# https://console.cloud.google.com/apis/api/geocoding_backend/quotas?project=my-project-1576761556969&supportedpurview=project
# BEFORE RUNNING ANYTHING








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


