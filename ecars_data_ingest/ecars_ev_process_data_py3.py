
import os
import bz2
import lxml
import pykml
import re
import copy
from collections import defaultdict
import pickle
from datetime import datetime
import time

import influxdb_client

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from bz2 import BZ2File as bzopen
from pykml import parser
import xml.etree.ElementTree as et
from html.parser import HTMLParser
from html.entities import name2codepoint

charger_data = {}

class charger_info():
     name = ''
     location = ''
     description = ''
     #charger_ports = {}

     def __init__(self,name_in,loc,desc):
         self.name=name_in
         self.location = loc
         self.description= desc
         self.charger_ports = defaultdict(list)

class charger_state():
     date_str = ''
     time_str = ''
     state = ''

     def __init__(self,date,time,state):
         self.date_str = date
         self.time_str= time 
         self.state = state
        

charger_types = ["CHAdeMO",
    "Combo DC",
    "Fast AC",
    "Type-2 AC Socket 3.7kW",
    "Type-2 AC Socket 22kW",
    "Type-2 AC Socket 11kW",
    "Type-2 AC Socket 7kW"]

charger_port_count = {
        "CHAdeMO" : 1,
        "Combo DC" : 1,
        "Fast AC" : 1,
        "Type-2 AC Socket 3.7" : 1,
        "Type-2 AC Socket 22kW" :2,
        "Type-2 AC Socket 11kW" :1,
        "Type-2 AC Socket 7kW" :1 }

class MyHTMLParser(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.search_str=[]
        self.string_finds={}

    def set_search_in_data(self,search_string):
        #print(search_string)
        self.search_str = copy.copy(search_string)
        #print(self.search_str)
        self.string_finds={}

    def get_matches_in_data(self):
        return self.string_finds

    def handle_starttag(self, tag, attrs):
        #print "Start tag:", tag
        #for attr in attrs:
        #    print "     attr:", attr
        pass

    def handle_endtag(self, tag):
        #print "End tag  :", tag
        pass

    def handle_data(self, data):
        #print "Data     :", data
        #perform body searches here/
        for x in self.search_str:
            find_it = data.find(x)
            if find_it != -1:
                self.string_finds[x] = data

        
    def handle_comment(self, data):
        #print "Comment  :", data
        pass

    def handle_entityref(self, name):
        c = chr(name2codepoint[name])
        #print "Named ent:", c
        pass

    def handle_charref(self, name):
        if name.startswith('x'):
            c = chr(int(name[1:], 16))
        else:
            c = chr(int(name))
        #print "Num ent  :", c
        pass

    def handle_decl(self, data):
        #print "Decl     :", data
        pass

parser = MyHTMLParser()

#Processing to do
# 1 - parce seaved data files and create pickle file
# 2 - load previous pickle file

processing_to_do = 1
update_influx_db=1
max_files = -1
files_processed=0

if update_influx_db:
    bucket = "esb-charger-bucket"
    client = InfluxDBClient(url="http://localhost:9999", token="a6ht_xX40q3dWXG61XWtV2beocFqKYDzqK8eJz596x6uCBSdL9ajze-SR6-cBhOG0kaIQq4kBdcq5_XThWX39Q==", org="Home")
    write_api = client.write_api(write_options=SYNCHRONOUS)
    query_api = client.query_api()


if processing_to_do == 1:
    data_directory = './saved_data'
    # Get .txt files
    for f_name in os.listdir(data_directory):
        if f_name.endswith('.bz2'):
            if update_influx_db==1:
                charger_data = {} #// reset after each file, as we uploaded to influx    
            f_name = data_directory + '/' + f_name
            #print('********************************')
            print(f_name)
            if max_files != -1:
                if files_processed > max_files:
                    pickle.dump( charger_data, open( "save_charger_data.p", "wb" ) )

                    print("exiting max files processed,saving picklefile")
                    exit()
            files_processed += 1


            end = f_name.find('.charging-locations.kml.bz2')
            time_str = f_name[end-8:end]
            date_str = f_name[end-19:end-9]
            date_str = date_str.replace('_','/')
            #print(date_str + ' ' + time_str)
            # reading a bz2 archive
            with bzopen(f_name, "r") as bzfin:
                kml_data = bzfin.read()
                try:
                    kml_root = pykml.parser.fromstring(kml_data)
                    place_list = kml_root.Document.Placemark
                except:
                    print(('ERROR:Catpured parce error for file' + f_name))
                    place_list = []

                for i in place_list:
                    name = str(i.name).encode('utf-8')

                    coord  =i.Point.coordinates
                    desc = str(i.description).encode('utf-8')



                    if name not in list(charger_data.keys()):
                        charger_data[name] = charger_info(name,coord,desc)
                    
                    parser.set_search_in_data(charger_types)
                    parser.feed(str(desc))
                    matchs = parser.get_matches_in_data()
                    #print('<<<<<Description Start')
                    #print(desc)
                    #print('<<<<Matches Found in description')
                    #print(matchs)
                    if len(matchs) == 0:
                        pass
                        #print('No Matches found')
                        #print(desc)
                        #print('End of matches')
                    #print('<<<<END of Matches Found in description')

                    for x in charger_types:
                        try:
                            all_count = matchs[x].count('ALL')
                            avail_count = matchs[x].count('Available')
                            oos_count = matchs[x].count('Out-of-Service')
                            occupied_count = matchs[x].count('Occupied')
                            ooc_count = matchs[x].count('Out-of-Contact')
                            #print('all:' + str(all_count) + ' avail:' + str(avail_count) + ' oos:' + str(oos_count) + ' occupied_count:' + str(occupied_count ) + ' ooc_count:' + str(ooc_count)  )
                            
                            port_count = charger_port_count[x]
                            port_override = matchs[x].count('(x2')
                            if port_override != 0:
                                port_count = 2
                            port_override = matchs[x].count('(x4')
                            if port_override != 0:
                                port_count = 4   

                            if port_count == 1:
                                #print('one port')
                                if avail_count >= 1:
                                    cs = charger_state(date_str,time_str,'Available')
                                elif occupied_count >= 1:
                                    cs = charger_state(date_str,time_str,'Occupied')
                                elif oos_count >= 1:
                                    cs = charger_state(date_str,time_str,'Out-of-Service')
                                elif  ooc_count >= 1:
                                    cs = charger_state(date_str,time_str,'Out-of-Contact')

                                if ( avail_count + occupied_count + oos_count +ooc_count) == 0:
                                    print('Error in counting')

                                charger = charger_data[name]
                                charger.charger_ports[x].append(cs)
                            elif port_count == 2:
                                #print('2 ports')
                                charger = charger_data[name]

                                if (all_count > 0):
                                    if avail_count >= 1:
                                        cs = charger_state(date_str,time_str,'Available')
                                    elif occupied_count >= 1:
                                        cs = charger_state(date_str,time_str,'Occupied')
                                    elif oos_count >= 1:
                                        cs = charger_state(date_str,time_str,'Out-of-Service')
                                    elif  ooc_count >= 1:
                                        cs = charger_state(date_str,time_str,'Out-of-Contact')

                                    k1 = x + ' Port 1'
                                    k2 = x + ' Port 2'
                                    charger.charger_ports[k1].append(cs)
                                    charger.charger_ports[k2].append(cs)
                                else:
                                    sum_val = avail_count  + (occupied_count << 1)+ (oos_count << 2)+ (ooc_count << 3)
                                    #print sum_val
                                    k1 = x + ' Port 1'
                                    k2 = x + ' Port 2'
                                    if sum_val == 1:
                                        charger.charger_ports[k1].append(charger_state(date_str,time_str,'Available'))
                                        charger.charger_ports[k2].append(charger_state(date_str,time_str,'Available'))
                                    elif sum_val == 2:
                                        charger.charger_ports[k1].append(charger_state(date_str,time_str,'Occupied'))
                                        charger.charger_ports[k2].append(charger_state(date_str,time_str,'Occupied'))
                                    elif sum_val == 3:
                                        charger.charger_ports[k1].append(charger_state(date_str,time_str,'Available'))
                                        charger.charger_ports[k2].append(charger_state(date_str,time_str,'Occupied'))
                                    elif sum_val == 4:
                                        charger.charger_ports[k1].append(charger_state(date_str,time_str,'Out-of-Service'))
                                        charger.charger_ports[k2].append(charger_state(date_str,time_str,'Out-of-Service'))
                                    elif sum_val == 5:
                                        charger.charger_ports[k1].append(charger_state(date_str,time_str,'Available'))
                                        charger.charger_ports[k2].append(charger_state(date_str,time_str,'Out-of-Service'))
                                    elif sum_val == 6:
                                        charger.charger_ports[k1].append(charger_state(date_str,time_str,'Occupied'))
                                        charger.charger_ports[k2].append(charger_state(date_str,time_str,'Out-of-Service'))
                                    elif sum_val == 7:
                                        # should not be found, but was in data 
                                        charger.charger_ports[k1].append(charger_state(date_str,time_str,'Available'))
                                        charger.charger_ports[k2].append(charger_state(date_str,time_str,'Out-of-Service'))
                                    elif sum_val == 8:
                                        charger.charger_ports[k1].append(charger_state(date_str,time_str,'Out-of-Contact'))
                                        charger.charger_ports[k2].append(charger_state(date_str,time_str,'Out-of-Contact'))
                                    elif sum_val == 9:
                                        charger.charger_ports[k1].append(charger_state(date_str,time_str,'Available'))
                                        charger.charger_ports[k2].append(charger_state(date_str,time_str,'Out-of-Contact'))
                                    elif sum_val == 10:
                                        charger.charger_ports[k1].append(charger_state(date_str,time_str,'Occupied'))
                                        charger.charger_ports[k2].append(charger_state(date_str,time_str,'Out-of-Contact'))
                                    elif sum_val == 11:
                                        # should not be found, but was in data 
                                        charger.charger_ports[k1].append(charger_state(date_str,time_str,'Available'))
                                        charger.charger_ports[k2].append(charger_state(date_str,time_str,'Out-of-Contact'))
                                    elif sum_val == 12:
                                        charger.charger_ports[k1].append(charger_state(date_str,time_str,'Out-of-Service'))
                                        charger.charger_ports[k2].append(charger_state(date_str,time_str,'Out-of-Contact'))
                                    elif sum_val == 13:
                                        pass
                                        #print('Error to modes  modes set' + str(sum_val) + desc )
                                    elif sum_val == 14:
                                        pass
                                        #print('Error to modes  modes set' + str(sum_val) + desc )
                                    elif sum_val == 15:
                                        pass
                                        #print('Error to modes  modes set' + str(sum_val) + desc )
                                    else:
                                        pass
                                        #print('Error to modes  modes set' + str(sum_val) + desc )
                            elif port_count == 4:
                                charger = charger_data[name]

                                if (all_count > 0):
                                    if avail_count >= 1:
                                        cs = charger_state(date_str,time_str,'Available')
                                    elif occupied_count >= 1:
                                        cs = charger_state(date_str,time_str,'Occupied')
                                    elif oos_count >= 1:
                                        cs = charger_state(date_str,time_str,'Out-of-Service')
                                    elif  ooc_count >= 1:
                                        cs = charger_state(date_str,time_str,'Out-of-Contact')

                                    k1 = x + ' Port 1'
                                    k2 = x + ' Port 2'
                                    k3 = x + ' Port 3'
                                    k4 = x + ' Port 4'
                                    charger.charger_ports[k1].append(cs)
                                    charger.charger_ports[k2].append(cs)
                                    charger.charger_ports[k3].append(cs)
                                    charger.charger_ports[k4].append(cs)
                                else:
                                    pass
                                    #print('Not supported 4 port config yet')


                        except KeyError:
                            pass
                    #print(desc)
        

        
        #if files_processed % 5000 == 0:
        #    pickle.dump( charger_data, open( "save_charger_data.p", "wb" ) )
        #    print('pickefile saved')

        if update_influx_db:
            i=0
            point_list=[]
            for charger_key in list(charger_data.keys()):
                #print(charger_key)A
                #print((charger_data[charger_key].name))
                #print((charger_data[charger_key].location))
                for port_info in list(charger_data[charger_key].charger_ports.keys()):
                    #print(port_info)
                    #print((charger_data[charger_key].charger_ports[port_info][0].state))
                    #print((charger_data[charger_key].charger_ports[port_info][0].time_str))
                    #print((charger_data[charger_key].charger_ports[port_info][0].date_str))

                    if (str(charger_data[charger_key].charger_ports[port_info][0].state) == "Available"):
                        avail_val=1
                    else:
                        avail_val=0

                    rfc_date_time_str = date_str.replace('/','-')+"T"+time_str+"Z"
                    date_time_obj = datetime.strptime(rfc_date_time_str,"%d-%m-%YT%H:%M:%SZ")
                    #unix_time=time.mktime(date_time_obj.timetuple())
                    address = str(charger_data[charger_key].name)
                    address = address.replace("\"","'")
                    address = address[2:-1]
                    #print(address)
                    p = Point("ChargerState") \
                        .tag("location", charger_data[charger_key].location) \
                        .tag("address",address) \
                        .tag("Port Type",port_info) \
                        .tag("StateTag",str(charger_data[charger_key].charger_ports[port_info][0].state)) \
                        .field("state_int",avail_val) \
                        .time(time=date_time_obj)

                    
                    i = i+1
                    point_list.append(p)
                    #write_api.write(bucket=bucket, org="Home", record=p)   
            if ( len(point_list)) > 0:
                write_api.write(bucket=bucket, org="Home", record=point_list)   
        else:
            print('saving last picklefile')
            pickle.dump( charger_data, open( "save_charger_data.p", "wb" ) )
            print('done')

elif processing_to_do == 2:
    pickle_in = open("save_charger_data.p","rb")
    charger_data_new = pickle.load(pickle_in)
    print('Loaded pickle file')

