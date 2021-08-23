import io
import json
import logging
import oci
from kafka import KafkaProducer

from fdk import response

signer = oci.auth.signers.get_resource_principals_signer()
client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
namespace = client.get_namespace().data

def list_objects(bucket):
    
    list_objects_response = client.list_objects(
        namespace,
        bucket,
        fields ="name")
    
    return(list_objects_response.data)

def move_object(source_bucket, destination_bucket, object_name):

    objstore_composite_ops = oci.object_storage.ObjectStorageClientCompositeOperations(client)
    resp = objstore_composite_ops.copy_object_and_wait_for_state(
        namespace, 
        source_bucket, 
        oci.object_storage.models.CopyObjectDetails(
            destination_bucket=destination_bucket, 
            destination_namespace=namespace,
            destination_object_name=object_name,
            destination_region=signer.region,
            source_object_name=object_name
            ),
        wait_for_states=[
            oci.object_storage.models.WorkRequest.STATUS_COMPLETED,
            oci.object_storage.models.WorkRequest.STATUS_FAILED])
    if resp.data.status != "COMPLETED":
        raise Exception("cannot copy object {0} to bucket {1}".format(object_name,destination_bucket))
    else:
        resp = client.delete_object(namespace, source_bucket, object_name)
        logging.getLogger().info("INFO - Object {0} moved to Bucket {1}".format(object_name,destination_bucket), flush=True)

def push_kafka(stagebucket, destinationbucket, server, username, password):

    producer = KafkaProducer(bootstrap_servers = server,
                         security_protocol = "SASL_SSL", sasl_mechanism = "PLAIN",
                         sasl_plain_username = username, 
                         sasl_plain_password = password)

    key = 'File'.encode('utf-8')
    file_list = list_objects(stagebucket)

    for x in file_list.objects:

        value = json.dumps(x.name).encode('UTF-8')
        try:
            producer.send('File', key=key, value=value)
            producer.flush()
            move_object(stagebucket, destinationbucket, x.name)
        except (Exception, ValueError) as ex:
            logging.getLogger().error('Erro na insersao do registo: ' + x + ' ' + str(ex))
    
def handler(ctx, data: io.BytesIO = None):

    try:
        cfg = ctx.Config()
        server = cfg["server"]
        username = cfg["username"]
        password = cfg["password"]
        stagebucket = cfg["stagebucket"]
        destinationbucket = cfg["destinationbucket"]
    except Exception as e:
        print('Missing function parameters', flush=True)
        raise

    try:
        push_kafka(stagebucket, destinationbucket, server, username, password)
        logging.getLogger().info("Executado com sucesso")
        return response.Response(
        ctx, response_data=json.dumps(
            {"message": "Dados inseridos com sucesso"}),
        headers={"Content-Type": "application/json"}
    )
    except (Exception, ValueError) as ex:
        logging.getLogger().info('Erro ao executar a operação: ' + str(ex))
