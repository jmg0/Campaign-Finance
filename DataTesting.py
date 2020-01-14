import json
import googlemaps
from datetime import datetime
import geocoder
import re
import pandas as pd
import Geocoder
import Hidden

ad1 = 'PO BOX 639, BELLEVUE, WA 980090639'
ad2 = '22 QUICKS ROAD WIMBLEDON, LONDON SW19 1EZ UNITED KINGDOM, ZZ 0'
ad3 = '2050 JAMIESON AVE, ALEXANDRIA, VA 223146827'
ad4 = '111 HARRIS RD, PRINCETON, NJ 8540'
address = '2506 AKEPA ST, PEARL CITY, HI 96782'
address = '61 CLINTON ST, MALDEN, MA 02148'
address = 'PO BOX 4233, COVINA, CA 917230633'

if re.search('PO BOX', address):
    na = ''
    address = re.findall('.*, (.*,.*)', address)
    for x in address:
        na += x + ' '
print(na.rstrip())

# if re.search(' CT ', address) or re.search(' MA ', address) or re.search(' ME ', address) or re.search(' NH ',address) or re.search(' NJ ', address) or re.search(' RI ', address) or re.search(' VT ', address):
#     zipcode = re.findall('.* ([0-9]*)', address)
#     if zipcode is not None and len(zipcode[0]) == 4:
#         new_zip = '0' + zipcode[0]
#         address = address.replace(zipcode[0], new_zip)
# print(address)

# if ' NJ ' in ad4.upper():
#     x = re.findall('.* ([0-9]*)', ad4)
#     if x is not None and len(x[0]) == 4:
#         y = '0' + x[0]
#         ad4 = ad4.replace(x[0], y)
# print(ad4)

# if ' wa ' or ' WA ' in ad1:
#     print('yes'.upper())




# ad_list = ['18 WILDWOOD CIR, FLETCHER, NC 28732', '19634 E 1080 RD, ELK CITY, OK 73644', '31221 VIA DEL VERDE, SAN JUAN CAPISTRANO, CA 92675', '40 N 14TH PL, FERNANDINA BEACH, FL 32034', '95 DILLINGHAM WAY, HANOVER, MA 2339']

# for ad in ad_list:
#     print(Geocoder.geocode_addresses_osm(ad))
#     print(Geocoder.geocode_addresses_mapbox(ad, Hidden.mapbox_api_token))

# addy = '7789 WILBURN RD, PANGBURN, AR 72121'
# addy = '4509 BOASTFIELD LN, OLNEY, MD 208322068'
# print('1', Geocoder.geocode_addresses_osm(addy))
# print('2', Geocoder.geocode_addresses_google(addy, key=Hidden.google_api_key))
# # print(Geocoder.geocode_addresses_arcgis(addy))
# print('3', Geocoder.geocode_addresses_locationIQ(addy, key=Hidden.locationIQ_api_key))
# print('4', Geocoder.geocode_addresses_mapbox(addy, Hidden.mapbox_api_token))

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


