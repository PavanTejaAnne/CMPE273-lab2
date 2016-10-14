import json
import urllib
import heapq
import logging
logging.basicConfig(level=logging.DEBUG)


from datetime import datetime as datime, time as t
from spyne import Application, Iterable, srpc, ServiceBase, UnsignedInteger,String
from spyne.protocol.http import HttpRpc
from spyne.protocol.json import JsonDocument
from spyne.server.wsgi import WsgiApplication


class CmpeService(ServiceBase):
    @srpc(String, String,String, _returns=Iterable(String))
    def checkcrime(lat, lon,radius):
        url='https://api.spotcrime.com/crimes.json?lat='+lat+'&lon='+lon+'&radius='+radius+'&key=.'
        print url
        response = urllib.urlopen(url)
        data = json.loads(response.read())
        
        sets = {
                "total_crime":0,
                "the_most_dangerous_streets":[],
                "crime_type_count":{
                    "Assault":0,
                    "Arrest":0,
                    "Burglary":0,
                    "Robbery":0,
                    "Theft":0,
                    "Other":0
                    },
                "event_time_count":{
                    "12:01am-3am": 0,
                    "3:01am-6am" : 0,
                    "6:01am-9am" : 0,
                    "9:01am-12noon" : 0,
                    "12:01pm-3pm" : 0,
                    "3:01pm-6pm" : 0,
                    "6:01pm-9pm" : 0,
                    "9:01pm-12midnight" : 0
                    }
            }

        sets['total_crime'] = len(data['crimes'])
        street_sets = {}

        for entries in data['crimes']:

            if entries.get('type')=='Assault':
                sets['crime_type_count']['Assault'] = sets['crime_type_count'].get('Assault')+1
            elif entries.get('type')=='Arrest':
                sets['crime_type_count']['Arrest'] = sets['crime_type_count'].get('Arrest')+1
            elif entries.get('type')=='Burglary':
                sets['crime_type_count']['Burglary'] = sets['crime_type_count'].get('Burglary')+1
            elif entries.get('type')=='Robbery':
                sets['crime_type_count']['Robbery'] = sets['crime_type_count'].get('Robbery')+1
            elif entries.get('type')=='Theft':
                sets['crime_type_count']['Theft'] = sets['crime_type_count'].get('Theft')+1
            elif entries.get('type')=='Other':
                sets['crime_type_count']['Other'] = sets['crime_type_count'].get('Other')+1


            crimet = entries.get('date')[9:]

            blok = datime.strptime(crimet,'%I:%M %p')
            crimetime = blok.time()
       
        #Printing crime time 
          
            if crimetime >= t(00,01,00) and crimetime <= t(03,00,00):
                sets['event_time_count']['12:01am-3am'] = sets['event_time_count'].get('12:01am-3am')+1
            elif crimetime >= t(03,01,00) and crimetime <= t(06,00,00):
                sets['event_time_count']['3:01am-6am'] = sets['event_time_count'].get('3:01am-6am')+1
            elif crimetime >= t(06,01,00) and crimetime <= t(9,00,00):
                sets['event_time_count']['6:01am-9am'] = sets['event_time_count'].get('6:01am-9am')+1
            elif crimetime >= t(9,01,00) and crimetime <= t(12,00,00):
                sets['event_time_count']['9:01am-12noon'] = sets['event_time_count'].get('9:01am-12noon')+1
            elif crimetime >= t(12,01,00) and crimetime <= t(15,00,00):
                sets['event_time_count']['12:01pm-3pm'] = sets['event_time_count'].get('12:01pm-3pm')+1
            elif crimetime >= t(15,01,00) and crimetime <= t(18,00,00):
                sets['event_time_count']['3:01pm-6pm'] = sets['event_time_count'].get('3:01pm-6pm')+1
            elif crimetime >= t(18,01,00) and crimetime <= t(21,00,00):
                sets['event_time_count']['6:01pm-9pm'] = sets['event_time_count'].get('6:01pm-9pm')+1
            elif crimetime >= t(21,01,00) and crimetime < t(00,00,00) or crimetime == t(00,00,00):
                sets['event_time_count']['9:01pm-12midnight'] = sets['event_time_count'].get('9:01pm-12midnight')+1
           

            add = entries.get('address')
            addon=''
            if 'OF ' in add:
                addon = add.split('OF ')
                for var in addon:
                    var=var.strip()
                    if 'ST' in var:
                        if street_sets.has_key(var):
                            street_sets[var] = street_sets.get(var)+1
                        else:
                            street_sets.update({var:1})
            elif '& ' in add:
                addon =add.split('& ')
                for var in addon:
                    var=var.strip()
                    if 'ST' in var:
                        if street_sets.has_key(var):
                            street_sets[var] = street_sets.get(var)+1
                        else:
                            street_sets.update({var:1})
            elif 'BLOCK ' in add:
                addon = add.split('BLOCK ')
                for var in addon:
                    var=var.strip()
                    if 'ST' in var:
                        if street_sets.has_key(var):
                            street_sets[var] = street_sets.get(var)+1
                        else:
                            street_sets.update({var:1})

        sets['the_most_dangerous_streets'] = heapq.nlargest(3,street_sets, key=street_sets.get) 
        yield sets
 
    application = Application([CmpeService], 
        tns='checkcrime.',
        in_protocol=HttpRpc(validator='soft'),
        out_protocol=JsonDocument(ignore_wrappers=True),
    )


if __name__ == '__main__':
    from wsgiref.simple_server import make_server
    
    wsgi_application = WsgiApplication(application)

    server = make_server('127.0.0.1', 8000, wsgi_application)

    logging.info("listening to http://127.0.0.1:8000")
    logging.info("wsdl is at: http://localhost:8000/?wsdl")

    server.serve_forever()