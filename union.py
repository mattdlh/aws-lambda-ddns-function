import json
import boto3
import re
import uuid
import time
import random
from datetime import datetime

# DyanmoDB table name
dynamodb_table = 'DDNS'
# Delay in seconds before starting to poll the instance for 'Name' tag if not found on launch
instance_rdy_delay = 210

print('Loading function ' + datetime.now().time().isoformat())
route53 = boto3.client('route53')
ec2 = boto3.resource('ec2')
compute = boto3.client('ec2')
dynamodb_client = boto3.client('dynamodb')
dynamodb_resource = boto3.resource('dynamodb')

def lambda_handler(event, context):

    """ Check to see whether a DynamoDB table already exists.  If not, create it.  This table is used to keep a record of
    instances that have been created along with their attributes.  This is necessary because when you terminate an instance
    its attributes are no longer available, so they have to be fetched from the table."""
    tables = dynamodb_client.list_tables()
    if dynamodb_table in tables['TableNames']:
        pass
    else:
        print 'DynamoDB table does not exist - creating'
        create_table(dynamodb_table)
    table = dynamodb_resource.Table(dynamodb_table)

    # Set variables
    # Get the state from the Event stream
    state = event['detail']['state']
    # Get the instance id, region, and tag collection
    instance_id = event['detail']['instance-id']
    region = event['region']

    if state == 'running':
        ## Get instance info from machine and insert into DynamoDB
        # Gather instance Name and zone from tags if/when they are available
        instance_name = gather_tag(instance_id, 'Name')
        private_zone_name = gather_tag(instance_id, 'zone')
        if (private_zone_name!=False) & (instance_name!=False):
            pass
        else:
            # Name tag does not exist. Wait for instance to boot and run scipts then poll for Name tag
            print 'Required tags not detected on launch. Waiting ',instance_rdy_delay,' seconds for instance to be ready...'
            time.sleep(instance_rdy_delay)
            # Start polling for 'Name' tag, continue looping until found
            print 'Done waiting. Polling for required tags'
            # Abort after 5 mins
            abort_after = 5 * 60
            start = time.time()
            while True:
                delta = time.time() - start
                instance_name = gather_tag(instance_id, 'Name')
                private_zone_name = gather_tag(instance_id, 'zone')
                if (private_zone_name!=False) & (instance_name!=False):
                    break
                if delta >= abort_after:
                    print 'Poll timed out. Quitting'
                    quit()
                time.sleep(1)
        ## Gather remaining instance data
        instance_data = compute.describe_instances(InstanceIds=[instance_id])
        instance_private_ip = instance_data['Reservations'][0]['Instances'][0]['PrivateIpAddress']
        # Save instance info to DynamoDB
        print 'Writing record for ',instance_id,' to DynamoDB'
        table.put_item(
            Item={
                'InstanceId': instance_id,
                'InstanceName': instance_name,
                'InstanceZone': private_zone_name,
                'InstancePrivateIP': instance_private_ip,
            }
        )
    # If machine is not running, fetch instance info from DynamoDB
    else:
        instance = table.get_item(
        Key={
            'InstanceId': instance_id
        },
        AttributesToGet=[
            'InstanceName',
            'InstanceZone',
            'InstancePrivateIP'
            ]
        )
        try:
            instance['Item']
        except:
            print 'No record for instance ',instance_id,' found in DynamoDB table. Exiting'
            quit()
        instance_name = instance['Item']['InstanceName']
        private_zone_name = instance['Item']['InstanceZone']
        instance_private_ip = instance['Item']['InstancePrivateIP']

    # Get hosted zone info
    hosted_zones = route53.list_hosted_zones()
    private_zone_id = get_zone_id(private_zone_name)

    # Wait a random amount of time.  This is a poor-mans back-off if a lot of instances are launched all at once.
    time.sleep(random.random())

    # Perform actions on instance depending on state
    if state == 'running':
        pass
    else:
        try:
            # Delete record from Route53
            delete_resource_record(private_zone_id, instance_name, private_zone_name, 'A', instance_private_ip)
            # Delete record from DynamoDB
            print 'Deleting record for ',instance_id,' from DynamoDB'
            table.delete_item(Key={'InstanceId': instance_id})
        except BaseException as e:
            print e

def create_table(table_name):
    dynamodb_client.create_table(
            TableName=table_name,
            AttributeDefinitions=[
                {
                    'AttributeName': 'InstanceId',
                    'AttributeType': 'S'
                },
            ],
            KeySchema=[
                {
                    'AttributeName': 'InstanceId',
                    'KeyType': 'HASH'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 4,
                'WriteCapacityUnits': 4
            }
        )
    table = dynamodb_resource.Table(table_name)
    table.wait_until_exists()

def create_resource_record(zone_id, host_name, hosted_zone_name, type, value):
    """This function creates resource records in the hosted zone passed by the calling function."""
    print 'Updating %s record %s in zone %s ' % (type, host_name, hosted_zone_name)
    if host_name[-1] != '.':
        host_name = host_name + '.'
    route53.change_resource_record_sets(
                HostedZoneId=zone_id,
                ChangeBatch={
                    "Comment": "Updated by Lambda DDNS",
                    "Changes": [
                        {
                            "Action": "UPSERT",
                            "ResourceRecordSet": {
                                "Name": host_name + hosted_zone_name,
                                "Type": type,
                                "TTL": 60,
                                "ResourceRecords": [
                                    {
                                        "Value": value
                                    },
                                ]
                            }
                        },
                    ]
                }
            )

def delete_resource_record(zone_id, host_name, hosted_zone_name, type, value):
    """This function deletes resource records from the hosted zone passed by the calling function."""
    print 'Deleting %s record %s in zone %s' % (type, host_name, hosted_zone_name)
    if host_name[-1] != '.':
        host_name = host_name + '.'
    route53.change_resource_record_sets(
                HostedZoneId=zone_id,
                ChangeBatch={
                    "Comment": "Updated by Lambda DDNS",
                    "Changes": [
                        {
                            "Action": "DELETE",
                            "ResourceRecordSet": {
                                "Name": host_name + hosted_zone_name,
                                "Type": type,
                                "TTL": 60,
                                "ResourceRecords": [
                                    {
                                        "Value": value
                                    },
                                ]
                            }
                        },
                    ]
                }
            )

def gather_tag(instance_id, tag_key):
    instance_name_tag_info = compute.describe_tags(
        Filters=[
            {'Name': 'resource-type','Values': ['instance']},
            {'Name': 'resource-id','Values': [instance_id]}
        ]
    )
    instance_name_tag_info.pop('ResponseMetadata')
    instance_tags = instance_name_tag_info['Tags']
    if instance_tags == []:
        return False
    else:
        for tag in instance_tags:
            key = tag['Key']
            if key==tag_key:
                value = tag['Value']
                if value != '':
                    return value
                else:
                    return False
        return False

def get_zone_id(zone_name):
    """This function returns the zone id for the zone name that's passed into the function."""
    if zone_name[-1] != '.':
        zone_name = zone_name + '.'
    hosted_zones = route53.list_hosted_zones()
    x = filter(lambda record: record['Name'] == zone_name, hosted_zones['HostedZones'])
    try:
        zone_id_long = x[0]['Id']
        zone_id = str.split(str(zone_id_long),'/')[2]
        return zone_id
    except:
        return None

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial
    raise TypeError ("Type not serializable")

def remove_empty_from_dict(d):
    """Removes empty keys from dictionary"""
    if type(d) is dict:
        return dict((k, remove_empty_from_dict(v)) for k, v in d.iteritems() if v and remove_empty_from_dict(v))
    elif type(d) is list:
        return [remove_empty_from_dict(v) for v in d if v and remove_empty_from_dict(v)]
    else:
        return d
