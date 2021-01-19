# chebi_wsdl2es.py

import pandas, json, zeep, Elasticsearch
wsdl = 'https://www.ebi.ac.uk/webservices/chebi/2.0/webservice?wsdl'

infile = os.path.join(data_folder,filename)
dat = pandas.read_csv(infile,sep="\t")

for id in dat["ID"] :
        try:
            data = client.service.getCompleteEntity(id)
           
            dataStr = str(data) 
            dataStr = dataStr.replace('False','false')
            dataStr = dataStr.replace('True','true')
  
        except:
           print("ERROR WSDL ", datSize, ctr)

        try:
            res = es.index(index=indexname, id=id, body=dataJson)
            print(res['result'])
        except:
            print("ERROR ES", id)
        
