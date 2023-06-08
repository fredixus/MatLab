import requests
import json
import time

"""loop = [
'Assurance',
'Events',
'AuditActions',
'Organisation1',
'Organisation2',
'Organisation3',
'Organisation4',
'Organisation5',
'Organisation6',
'Organisation7',
'Supplier',
'Customer',
'Basiccauses',
'Rootcauses',
'Standards',
'Standardtypes',
'Disciplines',
'Functions',
'Clauses',
'EventsHistory',
'AuditActionsHistory',
'AssuranceHistory',
'ASPNETUsers',
'Events_Supplier_ForField_SupplierLookup_Junction']"""


loop = [
    #'Organisation1',
    'AuditActions'
]

url = "http://localhost:7071/api/Collection"
function_key = "ZpxuUF5AsKCi3Vee07AFupNv1cek_XA1vNoiPvTNJ8DKAzFuPBqu5A=="
headers = {"Content-Type": "application/json" ,"x-functions-key":function_key}

for el in loop:
    time.sleep(2)
    data = {
        "collectionName":[el],
        "year":["1991"],
        "month":["06"],
        "day":["29"]
    }

    response = requests.post(url, data=json.dumps(data), headers=headers)
    print(response.status_code, response.text)

    response.close
