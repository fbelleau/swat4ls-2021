# intact_txt2es.py
# by Francois Belleau
# created during swat4ls 2020 hackathon

# python3 intact_txt2es.py xaa 1000 0

# https://www.ebi.ac.uk/intact/downloads
# ftp://ftp.ebi.ac.uk/pub/databases/intact/current/psimitab/intact.txt
# http://www.psidev.info/mif
# http://www.ebi.ac.uk/Tools/webservices/psicquic/registry/registry?action=STATUS


import os, pandas, csv, re, sys
import json

from elasticsearch import Elasticsearch, helpers


data_folder = "/mnt/scratch/intact/"

fields = ['ID_interactor_A','ID_interactor_B','Alt_ID_interactor_A',
'Alt_ID_interactor_B','Alias_interactor_A',
'Alias_interactor_B','Interaction_detection_method',
'Publication_1st_author','Publication_Identifier',
'Taxid_interactor_A','Taxid_interactor_B','Interaction_type',
'Source_database','Interaction_identifier',
'Confidence_value','Expansion_method',
'Biological_role_interactor_A','Biological_role_interactor_B',
'Experimental_role_interactor_A',
'Experimental_role_interactor_B','Type_interactor_A',
'Type_interactor_B','Xref_interactor_A','Xref_interactor_B',
'Interaction_Xref','Annotation_interactor_A',
'Annotation_interactor_B','Interaction_annotation',
'Host_organism','Interaction_parameter','Creation_date',
'Update_date','Checksum_interactor_A','Checksum_interactor_B',
'Interaction_Checksum','Negative','Feature_interactor_A',
'Feature_interactor_B','Stoichiometry_interactor_A',
'Stoichiometry_interactor_B','Identification_method_participant_A',
'Identification_method_participant_B']


def splitList(str1):
     str1 = str1.replace("uniprotkb:","uniprot:")
     str1 = str1.replace('\"',"")
     str1 = str1.replace('go:GO:',"GO:")
     str1 = str1.replace('rcsb pdb:',"pdb:")
     str1 = str(str1).split('|')
     return str1


def intact2es(filename, indexname, size, skip):
    infile = os.path.join(data_folder,filename)
    print(infile)
    assert os.path.exists(infile)
    dat = pandas.read_csv(infile,sep="\t",names=fields,skiprows=1,nrows=size)

    print(dat)
    print(type(dat))

    print(dat.shape)
    print(dat.columns)

    datSize = dat.shape[0]

    ctr = 0 
    for index, row in dat.iterrows():
        ctr = ctr + 1
        
        result = row.to_json()
        jsonDict = json.loads(result)
        
        jsonDict["ID_interactor_A"] = jsonDict["ID_interactor_A"].replace("uniprotkb:","uniprot:")
        jsonDict["ID_interactor_B"] = jsonDict["ID_interactor_B"].replace("uniprotkb:","uniprot:")

        jsonDict["Alt_ID_interactor_A"] = splitList(jsonDict["Alt_ID_interactor_A"])
        jsonDict["Alt_ID_interactor_B"] = splitList(jsonDict["Alt_ID_interactor_B"])
        jsonDict["Xref_interactor_A"] = splitList(jsonDict["Xref_interactor_A"])
        jsonDict["Xref_interactor_B"] = splitList(jsonDict["Xref_interactor_B"])
        jsonDict["Publication_Identifier"] = splitList(jsonDict["Publication_Identifier"])
        jsonDict["Annotation_interactor_A"] = splitList(jsonDict["Annotation_interactor_A"])
        jsonDict["Annotation_interactor_B"] = splitList(jsonDict["Annotation_interactor_B"])
        jsonDict["Taxid_interactor_A"] = splitList(jsonDict["Taxid_interactor_A"])
        jsonDict["Taxid_interactor_B"] = splitList(jsonDict["Taxid_interactor_B"])
        jsonDict["Alias_interactor_A"] = splitList(jsonDict["Alias_interactor_A"])
        jsonDict["Alias_interactor_B"] = splitList(jsonDict["Alias_interactor_B"])
        jsonDict["Interaction_identifier"] = splitList(jsonDict["Interaction_identifier"])

        jsonDict["Creation_date"] = jsonDict["Creation_date"].replace("/","-")
        jsonDict["Update_date"] = jsonDict["Update_date"].replace("/","-")

        ia = jsonDict["ID_interactor_A"]
        ib = jsonDict["ID_interactor_B"]
        
        id = ia + "-" + ib + "-" + str(ctr)
        #print(id)
        
        #dict = json.dumps(jsonDict, indent=4) 
        dict = json.dumps(jsonDict) 
        #print(dict)

        print("intact", datSize, ctr, id)
        #continue

        if ctr < skip :
            continue

        #res = es.index(index=indexname, id=id, body=dict)
        #print(res['result'])

        yield {
               "_index": indexname,
               "_id": id,
               "_source": dict
        }  


filename = sys.argv[1]
size = int(sys.argv[2])
skip = int(sys.argv[3])

#intact2es("intact.txt", "intact-txt", size, skip) 


elastic = Elasticsearch("http://192.168.3.46:9209")
print(elastic)

try:
    response = helpers.bulk(elastic, intact2es(filename, "intact-txt", size, skip))
    print ("\nRESPONSE:", response)
except Exception as e:
    print("\nERROR:", e)
