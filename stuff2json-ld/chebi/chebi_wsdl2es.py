# chebi_wsdl2es.py

# https://www.ebi.ac.uk/QuickGO/api/index.html#/gene_ontology
# http://geneontology.org/
# http://amigo.geneontology.org/amigo/landing
# ftp://ftp.ebi.ac.uk/pub/databases/chebi/Flat_file_tab_delimited/compounds.tsv

import pandas
import json
import zeep

wsdl = 'https://www.ebi.ac.uk/webservices/chebi/2.0/webservice?wsdl'
client = zeep.Client(wsdl=wsdl)


from elasticsearch import Elasticsearch
es = Elasticsearch("http://192.168.3.46:9209")
es = Elasticsearch("http://***@206.12.89.13:9200")
print(es)

data_folder = "/root/chebi/"

def chebi2es(filename, indexname, skip):
    infile = os.path.join(data_folder,filename)
    print(infile)
    assert os.path.exists(infile)
    dat = pandas.read_csv(infile,sep="\t")

    print(dat)
    datSize = dat.shape[0]
    print(datSize)

    #exit()

    ctr = 0 
    for id in dat["ID"] :
        ctr = ctr + 1
        print(datSize, ctr, id)
        
        if ctr < skip :
            continue

        #continue

        try:
            data = client.service.getCompleteEntity(id)
            #print(data)
            dataStr = str(data) 
            #print(dataStr)
            dataStr = dataStr.replace('None',"'None'")
            dataStr = dataStr.replace('False','false')
            dataStr = dataStr.replace('True','true')
            dataStr = dataStr.replace("'",'"')
            #print(dataStr)

            dataJson = json.loads(dataStr) 
            #print(dataJson)
            #del dict["requiredInputComponent"]
        except:
           print("ERROR WSDL ", datSize, ctr)
           dataJson = {}
        #contents = contents.replace("\\","_").replace('"input"','"input2"').replace('"output"','"output2"')
        #print(dict)
        
        try:
            res = es.index(index=indexname, id=id, body=dataJson)
            print(res['result'])
        except:
            print("ERROR ES", id)
        

skip = int(sys.argv[1])

chebi2es("compounds.tsv", "chebi-wsdl", skip) 
