import os, logging
from socket import timeout
import pandas as pd
#from azure.storage.blob import BlockBlobService
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
#, AppendBlobService
import base64

def _createBlockBlobService() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(
        os.environ["AZURE_STORAGE_CONNECTION_STRING"], 
        max_block_size = 128 * 1024, 
        timeout = 1800, 
        max_single_put_size = 128 * 1024 * 1024
    )

def append (blob_file_name, conteiner, df) -> None :
    blob_service = _createBlockBlobService()

    #Create Append Blob
    blob = blob_service.get_blob_client(
            conteiner,
            blob_file_name
    )

    blob.create_append_blob()

    #Split for small chunks
    length = len(df)
    chunk = int(length / (length / 10))
    chunk_start = range(0, length, chunk)
    chunk_start = [i for i in chunk_start]
    chunk_end = chunk_start[1:]
    chunk_end.append(length)

    #Append by chunk
    for i in zip(chunk_start, chunk_end):
        if (i[0] == 0):
            blob.append_block(df.iloc[i[0]:i[1]].to_csv(index=False, sep=';', encoding="utf-8"))
        else:
            blob.append_block(df.iloc[i[0]:i[1]].to_csv(index=False, sep=';', encoding="utf-8", header=False))

    logging.info("Append {} {}/{}".format(str(df.shape), conteiner, blob_file_name))

def createBlobFromDataFrame(blob_file_name, conteiner, df) -> None:
    #Create Blob object representation.
    blob_service = _createBlockBlobService()

    #Create Blob Client
    blob_client = blob_service.get_blob_client(container=conteiner, blob=blob_file_name) 

    #Parse to CSV
    output = df.to_csv(index=False, sep=';', encoding="utf-8")

    #Create file csv from text and save in Blob Storage
    blob_client.upload_blob(output, overwrite=True, blob_type="BlockBlob")
    
    logging.info("Saved {} {}/{}".format(str(df.shape), conteiner, blob_file_name))
