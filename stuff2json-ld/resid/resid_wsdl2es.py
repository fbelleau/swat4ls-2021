# resid database
# https://proteininformationresource.org/resid/
# https://registry.identifiers.org/registry/resid
# ftp://ftp.pir.georgetown.edu/pir_databases/other_databases/resid/RESIDUES.XML

from xml.dom import minidom
import xmltodict
import json

from elasticsearch import Elasticsearch
es = Elasticsearch("http://192.168.3.46:9209")
es = Elasticsearch("http://***@206.12.89.13:9200")
print(es)

xmlDoc = minidom.parse('RESIDUES.XML')

print(xmlDoc)

itemlist = xmlDoc.getElementsByTagName('Entry')
print(len(itemlist))
print(itemlist[0].attributes['id'].value)

ctr = 0

for xml in itemlist:
    ctr = ctr + 1
    id = xml.attributes['id'].value
    print(ctr, id)
    xmlStr = xml.toprettyxml()
    xmlDict = xmltodict.parse(xmlStr)
    xmlDict = xmlDict["Entry"]
    #print(xmlDict)

    xmlDict["@context"] = "http://schema.org/"
    xmlDict["@id"] = id
    xmlDict["@type"] = "reside:Entry"
    xmlDict["dcterms:idenfifier"] = "resid:"+id
    xmlDict["bm:namespace"] = "reside"
    xmlDict["bm:idenfifier"] = id
    xmlDict["bm:uri"] = "identifiers:resid/"+id
    xmlDict["rdfs:label"] = xmlDict["Names"]["Name"]

    if "Comment" in xmlDict.keys():
        xmlDict["rdfs:comment"] = xmlDict["Comment"]

    # problem with Citation : object and string
    if "GeneratingEnzyme" in xmlDict.keys():
       enzymeNames = xmlDict["GeneratingEnzyme"]["EnzymeName"]
       #print("1", type(enzymeNames), enzymeNames)
       listNames = []
       if isinstance(enzymeNames, list):
           #print("2")
           for en in enzymeNames:
              d1 = {"@link":"ASN","#text":en}
              #print(type(d1), d1)
              listNames.append(d1)
              #print(listNames)
       else:
           listNames.append(enzymeNames)
       
       xmlDict["GeneratingEnzyme"]["EnzymeName"] = listNames
       enzymeNames = xmlDict["GeneratingEnzyme"]["EnzymeName"]
       #print("3", type(enzymeNames), enzymeNames)
       #bug
       del xmlDict["GeneratingEnzyme"]
 
    if "ReferenceBlock" in xmlDict.keys():
       del xmlDict["ReferenceBlock"]

    if "ReferenceBlock" in xmlDict.keys():
       del xmlDict["ReferenceBlock"]

    jsonStr = json.dumps(xmlDict)
    #print(jsonStr)
    #res = es.index(index="reside-xml", id=id, body=xmlDict)
    try:
        res = es.index(index="resid-xml", id=id, body=xmlDict)
        print(res)
    except:
        print("ES ERROR", id)
        print(jsonStr)
