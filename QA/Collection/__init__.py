from calendar import month
import logging, json, os, sys, re
import requests, traceback
import azure.functions as func

from . import blob
import pandas as pd

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    #Get Collection name
    reg = req.get_body()
    req = json.loads(reg)
    collectionName = req['collectionName'][0]
    year = req['year'][0]
    month = req['month'][0]
    day = req['day'][0]

    path = f'{os.environ["BaseDirStorage"]}{year}/{month}/{day}/'
    logging.info(f"Request Collection Name: {collectionName}")
    logging.info(f"Will be saved: {path}")

    #Get Secrets
    CLIENT_ID = os.environ["CLIENT_ID"]
    CLIENT_SECRET = os.environ["CLIENT_SECRET"]
    ACCESS_TOKEN_URL = os.environ["ACCESS_TOKEN_URL"]

    #Get Token
    wood_token = json.loads(requests.post(
        ACCESS_TOKEN_URL,
        data={
            'grant_type': 'client_credentials',
            'scope': 'api1',
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET
        },
        headers={
            'Content-Type':'application/x-www-form-urlencoded'
        }).text)["access_token"]

    #compose url
    url = os.environ["API_URL"] + collectionName

    #compose token
    token = f'Bearer {wood_token}'

    #Get data
    JSON_FILE = requests.get(
            url,
            headers={'Authorization': token}
    )

    
    if (JSON_FILE.status_code == 200):
        try:
            #Open
            df_nested_list = pd.json_normalize(json.loads(JSON_FILE.text), record_path =['dataRows'])

            #Flatten
            df = pd.DataFrame(df_nested_list['row'].apply(lambda row: row[0]).to_list())

            #Clear memory
            del df_nested_list

            #Save as csv
            try:
                
                #blob.createBlobFromDataFrame("{}{}.csv".format(path, collectionName), os.environ["Storage"], df)
                blob.append("{}{}.csv".format(path, collectionName), os.environ["Storage"], df) 
            except Exception as e:
                logging.info("Append CSV")
                blob.append("{}{}.csv".format(path, collectionName), os.environ["Storage"], df)   

                logging.error(f'Azure Blob Error: {collectionName} '+ str(e))

                return func.HttpResponse(
                        f"Azure Blob Error. {collectionName} \n" + str(e),
                        status_code=500
                )

            return func.HttpResponse(
                    f"Sucess file saved {path}{collectionName}.csv",
                    status_code=200
            )
        except Exception as e:
            logging.error(f'AF internal error: {collectionName} '+ str(e))

            # Get current system exception
            ex_type, ex_value, ex_traceback = sys.exc_info()

            # Extract unformatter stack traces as tuples
            trace_back = traceback.extract_tb(ex_traceback)

            # Format stacktrace
            stack_trace = list()

            for trace in trace_back:
                stack_trace.append("File : %s , Line : %d, Func.Name : %s, Message : %s \n" % (trace[0], trace[1], trace[2], trace[3]))

            return func.HttpResponse(
                        f"AF internal error. {collectionName} {str(stack_trace)} \n" + str(e),
                        status_code=500
                )
    else:
        return func.HttpResponse(
                    JSON_FILE.text,
                    status_code=JSON_FILE.status_code
            )
