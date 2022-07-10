#!/usr/bin/env python
# -*- coding: utf-8 -*-
# APIs used
# create_db_cluster_snapshot
# delete_db_cluster_snapshot
# describe_db_clusters
# describe_db_cluster_snapshots
# create_db_instance

from __future__ import print_function
from ast import arg
from distutils.command.config import config
from hashlib import new
from http import client
from operator import mul
import profile
from traceback import print_tb

import boto3
import botocore
import click
import datetime
import time
import sys
import argparse
import os
import warnings
import pytz
import prettytable
# from datetime import datetime
from datetime import datetime as dt

utc = pytz.UTC
warnings.simplefilter("ignore")

# with open('config.json', 'r') as f:
#     __data = json.load(f)

__version__ = '0.0.1'
code_start_time = time.time()
pretty = prettytable.PrettyTable()
pretty.field_names = ['                                       Current Operation                                    ','   Status  ','        Completed at       ','Elapsed Time']
pretty.hrules = 1
pretty.align['                                       Current Operation                                    ']='l'
dev_profile = 'rds-dev-ops'
prod_profile = 'rds-prod-ops'
region = 'us-west-2'
mandatory_tags=[
    {
        'Key': 'Category',
        'Value': 'Emea360'
    }
]
dev_cluster_name = 'dev9'

def pretty_print(row_val, new_row=None):
    # if len(pretty) > 1:
    # if pretty.__getattr__('rowcount') > 1:
    #     pretty.del_row(1)
    # if new_row is not None:
    #     print(pretty)
    # table_rows = lambda t: len(pretty.get_string().split('\n'))-4
    if pretty.__getattr__('rowcount') > 1:
        # print("Row value {}".format(pretty.__getattr__('rowcount')))
        del_row = pretty.__getattr__('rowcount') - 1
        # print(del_row)
        pretty.del_row(del_row)
    #     pretty.del_row(2)
    pretty.add_row(row_val)
    pretty_new = pretty.get_string(start=pretty.__getattr__('rowcount')-1)
    # row_txt = '\n'.join(table_txt.split('\n')[-2:])
    # print( "\n".join(pretty.get_string().splitlines()[-2:]) )
    print( "\n".join(pretty_new.split('\n')[-2:]), end='\r' )

# get the connection
def _get_con(con_name):
    """
    Get the jdoc connection string to establish the connections
    :param con_name: connection name
    :return: connection string
    """
    global con_list, gbl_job_id
    print(gbl_job_id, str(datetime.datetime.now()), 'get_con', con_name)
    try:
        if con_name in con_list.keys():
            return con_list[con_name]

        jdoc = __data[con_name]
        aws_access_key_id, aws_secret_access_key, aws_session_token = generate_temp_credentials()
        c_details = _get_secret(jdoc, aws_access_key_id, aws_secret_access_key, aws_session_token)
        if jdoc['conection_string'] is None:
            con_list[con_name] = c_details
            return c_details
        else:
            con_str = jdoc['conection_string']
            for k in c_details:
                con_str = con_str.replace(k, str(c_details[k]))
            con_list[con_name] = con_str
            return con_str
    except Exception as e:
        print("error connection {}".format(e))
        raise

def time_since(started, current=None):
    if current is not None:
        elapsed = current - started
    else:
        elapsed = time.time() - started
    m = int(elapsed // 60)
    s = int(elapsed % 60)
    if m >= 60:
        h = int(m // 60)
        m = m % 60
        return f'{h}h {m}m {s}s'
    else:
        return f'{m}m {s}s'

def description():
    return """This is a automation for configuring Auto kitchen setup."""

def profile_switch(profile):
    session = boto3.session.Session(profile_name=profile)
    client = session.client('rds', region_name=region)
    return client

def create(dbcon, db_instance, args):
    """Creates a new DB snapshot"""
    now = dt.now()
    time_folder = now.strftime("%Y-%m-%d-%H-%M-%S")
    snapshot = "{0}-{1}-{2}".format("emea-cust-360", args.dev_instance_name,time_folder)
    # current_status = validate_snapshot(profile_switch(prod_profile),snapshot,args)
    # resp = dbcon.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=snapshot.lower())
    # print(resp)
    # current_status = dbcon.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=snapshot.lower())['DBClusterSnapshots'][0]['Status']
    # if current_status == 'available' or current_status == 'failed':
    #     pretty_print(["The snapshot {} is {}".format(snapshot,current_status), current_status,str(datetime.datetime.now()),time_since(time.time())])
    # else:
    resp = dbcon.create_db_cluster_snapshot(DBClusterSnapshotIdentifier=snapshot, DBClusterIdentifier=db_instance, Tags=mandatory_tags)
    pretty_print(["The snapshot {} is started for cluster - {}".format(snapshot,db_instance), "Started",str(datetime.datetime.now()),time_since(time.time())])
    time.sleep(10)  # wait 20 seconds before status request
    current = time.time()
    current_status = "available"
    # wait_cluster_state(current, dbcon, db_instance,current_status)
    current_status = None
    dbcon = profile_switch(dev_profile)
    while True:
        # current_status = __status(dbcon,snapshot=snapshot)
        print(time_since(current, time.time()), end='\r')
        current_status = dbcon.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=snapshot.lower())['DBClusterSnapshots'][0]['Status']
        # print("Current status of snapshot {} is {}. sleeping 5 seconds".format(snapshot, current_status))
        time.sleep(5)
        if current_status == 'available' or current_status == 'failed':
            pretty_print(["The snapshot {} is {}".format(snapshot,current_status), current_status,str(datetime.datetime.now()),time_since(time.time())])
            break
    return snapshot, current_status

def cluster___status(current, dbcon, db_instance, status):
    current_status = dbcon.describe_db_clusters(DBClusterIdentifier=db_instance)['DBClusters'][0]['Status']
    """Returns the current status of the DB snapshot"""
    while True:
        try:
            current_status = dbcon.describe_db_clusters(DBClusterIdentifier=db_instance)['DBClusters'][0]['Status']
        except botocore.exceptions.ClientError as error:
            current_status = ''
            pass
        print(time_since(current, time.time()), end='\r')
        # print("Current status of cluster {} is {}. sleeping 5 seconds - elapsed - {}".format(db_instance, current_status, time_since(current, time.time())), end='\r')
        time.sleep(5)
        if current_status == status:
            break
    return status

def describe(dbcon, db_instance):
    """Describe cluster instance"""
    # while True:
    cluster_info = dbcon.describe_db_clusters(DBClusterIdentifier=db_instance)['DBClusters']
    status = cluster_info[0]['Status']
        # if status == 'available':
        #     break
        # else:
        #     time.sleep(5)
    return status, cluster_info
    
def latest_snapshot(dbcon, db_instance):
    """Get the latest snapshot for the cluster"""
    cluster_info = dbcon.describe_db_cluster_snapshots(DBClusterIdentifier=db_instance)['DBClusterSnapshots']
    starttime=datetime.datetime(1,1,1,tzinfo=utc)
    if len(cluster_info) > 0:
        for snap in cluster_info:
            if snap['SnapshotCreateTime'] > starttime:
                snap_id = snap['DBClusterSnapshotIdentifier']
                starttime= snap['SnapshotCreateTime']
                # print("Found this snapshot {} at {}".format(snap_id, starttime))
    else:
        snap_id = None
        print("No snapshots found for this cluster {}".format(db_instance))
        os._exit(1)
    return snap_id

def validate_snapshot(dbcon, db_snapshot, args):
    """Validate a user-specified DB snapshot"""
    try:
        current_status = __status(profile_switch(dev_profile),snapshot=db_snapshot)
        if current_status == 'available':
            pretty_print(["The snapshot {} is available".format(db_snapshot), "Completed",str(datetime.datetime.now()),time_since(time.time())])
    except:
        current_status = 'does not exist'
    return current_status

def delete(dbcon, snapshot):
    """Deletes a user-specified DB snapshot"""
    try:
        current_status = __status(dbcon,snapshot=snapshot)
        if current_status == 'available':
            dbcon.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snapshot)
            current_status = __status(dbcon, snapshot=snapshot)
    except:
        current_status = 'does not exist'
    return current_status

def __status(dbcon, snapshot):
    """Returns the current status of the DB snapshot"""
    return dbcon.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=snapshot)['DBClusterSnapshots'][0]['Status']

def cluster__status(dbcon, db_instance):
    """Returns the current status of the DB snapshot"""
    try:
        dbcon.describe_db_clusters(DBClusterIdentifier=db_instance)
        if len(dbcon.describe_db_clusters(DBClusterIdentifier=db_instance)) > 1:
            return dbcon.describe_db_clusters(DBClusterIdentifier=db_instance)['DBClusters'][0]['Status']
        else:
            return "No DB cluster found in this name {}".format(db_instance)
    except botocore.exceptions.ClientError as error:
        pretty_print(["Error while describing cluster - {}".format(db_instance), "Errorbelow",str(datetime.datetime.now()),time_since(time.time())])
        print("Error here:\n{}".format(error))
        os._exit(1)

def instances(dbcon, args):
    """Returns the available RDS instances"""
    db_instances = dbcon.describe_db_clusters()['DBClusters']
    click.echo("Database Instances:")
    for instance in db_instances:
        print("\t- {0}".format(instance['DBClusterIdentifier']))


def create_snapshot(dbcon, args):
    """Creates a new DB snapshot"""
    if not args.db_instance:
        click.echo("Please specify a database using --db-instance option", err=True)
        return sys.exit(1)
    current = time.time()
    pretty_print(["Creating a new snapshot from {0} instance...".format(args.db_instance),"Running",str(datetime.datetime.now()),time_since(current, time.time())])
    snapshot_name , response = create(dbcon, db_instance=args.db_instance, args=args)
    pretty_print(["Snapshot created {} and its {}".format(snapshot_name,response),"Completed",str(datetime.datetime.now()),time_since(current, time.time())])
    if response == 'available':
        create_instance(profile_switch(prod_profile), snapshot_name, args=args)

def wait_cluster_state(current, dbcon, db_instance, status):
    current_status = dbcon.describe_db_clusters(DBClusterIdentifier=db_instance)['DBClusters'][0]['Status']
    while True:
        print(time_since(current, time.time()), end='\r')
        current_status = cluster__status(dbcon, db_instance=db_instance)
        # print("Current status of cluster {} is {}. sleeping 5 seconds".format(db_instance, current_status))
        time.sleep(5)
        if current_status == status:
            break
        
def wait_instance_state(current, dbcon, db_instance, status):
    current_status = dbcon.describe_db_instances(DBInstanceIdentifier=db_instance)['DBInstances'][0]['DBInstanceStatus']
    while True:
        print(time_since(current, time.time()), end='\r')
        current_status = dbcon.describe_db_instances(DBInstanceIdentifier=db_instance)['DBInstances'][0]['DBInstanceStatus']
        # print("Current status of cluster {} is {}. sleeping 5 seconds".format(db_instance, current_status))
        time.sleep(5)
        if current_status == status:
            break

def create_instance(dbcon, snapshot_name, args):
    """Creates a new DB instance with snapshot"""
    current = time.time()
    if not args.db_instance:
        click.echo("Please specify a database using --db-instance option", err=True)
        return sys.exit(1)
    status, info = describe(profile_switch(prod_profile), db_instance=args.db_instance)
    # pretty_print(["Get the latest snapshot","Running",str(datetime.datetime.now()),time_since(current, time.time())])
    latest_snapshot_id = snapshot_name
    # time.sleep(1)
    # pretty_print(["Get the latest snapshot","Completed",str(datetime.datetime.now()),time_since(current, time.time())])
    pretty_print(["The cluster {} is in {} status".format(args.db_instance,status),"Completed",str(datetime.datetime.now()),time_since(current, time.time())])
    current = time.time()
    pretty_print(["Latest snapshot id {} for this cluster {}".format(latest_snapshot_id,args.db_instance),"Completed",str(datetime.datetime.now()),time_since(current, time.time())])
    temp_cluster_name = args.dev_instance_name + "-cluster"
    # new_cluster_instance_name = args.db_instance.replace("v1",temp_cluster_name)
    new_cluster_instance_name = "emea-cust-360-" + temp_cluster_name
    dbcon = profile_switch(prod_profile)
    # print("""aws rds restore-db-cluster-from-snapshot --db-cluster-identifier {},
    #                                         --availability-zones {},
    #                                         --snapshot-identifier {},
    #                                         --engine {}
    #                                         --engine-version {}
    #                                         --port {}
    #                                         --db-subnet-group-name {}
    #                                         --database-name {}
    #                                         --profile {}""".format(new_cluster_instance_name,
    #                                         info[0]['AvailabilityZones'],
    #                                         latest_snapshot_id,
    #                                         info[0]['Engine'],
    #                                         info[0]['EngineVersion'],
    #                                         info[0]['Port'],
    #                                         info[0]['DBSubnetGroup'],
    #                                         info[0]['DatabaseName'],
    #                                         dbcon))
    try:
        dbcon.restore_db_cluster_from_snapshot(DBClusterIdentifier=new_cluster_instance_name,
                                            AvailabilityZones=['us-west-2a'],
                                            DBClusterParameterGroupName='emea-cust-360-postgresql13-dev-test',
                                            SnapshotIdentifier=latest_snapshot_id,
                                            DBClusterInstanceClass='db.r5.large',
                                            EnableCloudwatchLogsExports=['postgresql'],
                                            Engine=info[0]['Engine'],
                                            VpcSecurityGroupIds=['sg-052340ada588a7c8a','sg-09ef8251b03674467'],
                                            # MultiAZ=info[0]['MultiAZ'],
                                            EngineVersion=info[0]['EngineVersion'],
                                            Port=info[0]['Port'],
                                            DBSubnetGroupName=info[0]['DBSubnetGroup'],
                                            DatabaseName=info[0]['DatabaseName'],
                                            Tags=mandatory_tags)
    except botocore.exceptions.ClientError as error:
        pretty_print(["Error while creating cluster from snapshot","Errorbelow",str(datetime.datetime.now()),time_since(current, time.time())])
        print("Error here:\n{}".format(error))
        os._exit(1)
    time.sleep(10)
    status, info = describe(profile_switch(dev_profile), db_instance=new_cluster_instance_name)
    current_status = None
    current = time.time()
    dbcon = profile_switch(dev_profile)
    pretty_print(["Cluster creation from {} as {} is started".format(latest_snapshot_id,new_cluster_instance_name),"Running",str(datetime.datetime.now()),time_since(current, time.time())])
    cluster___status(current, dbcon,new_cluster_instance_name,"available")
    wait_cluster_state(current, dbcon,new_cluster_instance_name,"available")
    pretty_print(["Cluster creation from {} as {} is completed".format(latest_snapshot_id,new_cluster_instance_name),"Completed",str(datetime.datetime.now()),time_since(current, time.time())])
    dbcon = profile_switch(prod_profile)
    status = dbcon.describe_db_instances(DBInstanceIdentifier='emea-cust-360-b')
    # for i in range(0,len(status['DBInstances'])-1):
    i=0
    instances = status['DBInstances']
    db_name = 'emea-cust-360-' + args.dev_instance_name
    try:
        dbcon = profile_switch(dev_profile)
        dbcon.create_db_instance(DBInstanceIdentifier=db_name,
                                            DBInstanceClass='db.r5.large',
                                            Engine=instances[i]['Engine'],
                                            DBSecurityGroups=instances[i]['DBSecurityGroups'],
                                            AvailabilityZone=instances[i]['AvailabilityZone'],
                                            DBSubnetGroupName=instances[i]['DBSubnetGroup']['DBSubnetGroupName'],
                                            PreferredMaintenanceWindow=instances[i]['PreferredMaintenanceWindow'],
                                            DBParameterGroupName='emea-cust-360-postgresql13-dev-test-param',
                                            MultiAZ=instances[i]['MultiAZ'],
                                            EngineVersion=instances[i]['EngineVersion'],
                                            AutoMinorVersionUpgrade=False,
                                            LicenseModel=instances[i]['LicenseModel'],
                                            OptionGroupName=instances[i]['OptionGroupMemberships'][0]['OptionGroupName'],
                                            PubliclyAccessible=instances[i]['PubliclyAccessible'],
                                            DBClusterIdentifier=new_cluster_instance_name,
                                            StorageType=instances[i]['StorageType'],
                                            StorageEncrypted=instances[i]['StorageEncrypted'],
                                            CopyTagsToSnapshot=instances[i]['CopyTagsToSnapshot'],
                                            MonitoringInterval=instances[i]['MonitoringInterval'],
                                            PromotionTier=instances[i]['PromotionTier'],
                                            Tags=mandatory_tags)
    except botocore.exceptions.ClientError as error:
        pretty_print(["Error while creating instance from snapshot","Errorbelow",str(datetime.datetime.now()),time_since(current, time.time())])
        print("Error here:\n{}".format(error))
        os._exit(1) 
    current = time.time()
    pretty_print(["Instance creation in {} as {} is started".format(db_name,new_cluster_instance_name),"Running",str(datetime.datetime.now()),time_since(current, time.time())])
    time.sleep(5)
    wait_instance_state(current, profile_switch(dev_profile),db_name,"available")
    pretty_print(["Instance creation in {} as {} is completed".format(db_name,new_cluster_instance_name),"Completed",str(datetime.datetime.now()),time_since(current, time.time())])


def delete_snapshot(dbcon, db_snapshot):
    """Deletes a user-specified DB snapshot"""
    if not db_snapshot:
        click.echo("Please specify a snapshot using --db-snapshot option", err=True)
        return sys.exit(1)
    # dbcon = DBSnapshot()
    current = time.time()
    response = delete(dbcon, snapshot=db_snapshot)
    if response == 'does not exist':
        pretty_print(["Snapshot: {0} has been deleted".format(db_snapshot),"Completed",str(datetime.datetime.now()),time_since(current, time.time())])
    else:
        pretty_print(["Snapshot: {0} deletion failed".format(db_snapshot),"Failed",str(datetime.datetime.now()),time_since(current, time.time())])
    

def main():
    # aws_region='us-east-1'
    parser = argparse.ArgumentParser(description=description(),
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-s", "--sql-path", default=os.getcwd(), help="sql directory path")
    parser.add_argument("-r", "--remove-snapshot", action="store_true", help="snapshot to remove from the option -t / --db-snapshot")
    parser.add_argument("-d", '--db-instance', help='Database instance')
    parser.add_argument("-n", '--dev-instance-name', default='dev9',help='Dev Database instance and cluster name')
    parser.add_argument("-a", '--db-snapshot-name', default='dev-instance-name',help='Name of the snapshot to create - Default will be dev-instance-name')
    parser.add_argument("-t", '--db-snapshot', help='DB cluster snapshot identifier to use to restore')
    # parser.add_argument("-a", '--aws-region', default='us-east-1', help='AWS region to run this automation')
    # parser.add_argument("-p", '--aws-profile-name', default='rds-ops', help='AWS cli profile name to connect')
    parser.add_argument("-v", '--version', action="store_true", help='version')
    parser.add_argument("-f", '--force', action="store_false", help='force to take snapshot now')
    args = parser.parse_args()
    config = parser.parse_args()
    # config = vars(args)
    print(config)
    #Region and job-type deciding parameters
    # region = args.aws_region
    # schema_data_in = __data['database_schemas'][region]['data_in']
    # schema_data_out = __data['database_schemas'][region]['data_out']
    # schema_crm = __data['database_schemas'][region]['crm']

    print("Running the automation against this cluster - ",args.db_instance)
    print(pretty)
    pretty_print(["Auto kitchen setup started"," Started ",str(datetime.datetime.now()),time_since(code_start_time)])

    # print(client)
    if args.version:
        # print("Auto kitchen setup - version : ",__version__)
        pretty_print(["Fetching Code version : Verion : {}".format(__version__),"Completed",str(datetime.datetime.now()),time_since(code_start_time)])
    else:
        if args.db_snapshot is not None:
            pretty_print(["Validating the snapshot - {}".format(args.db_snapshot),"Checking",str(datetime.datetime.now()),time_since(code_start_time)])
            snapshot_status = validate_snapshot(profile_switch(dev_profile),args.db_snapshot,config)
            if snapshot_status == 'available':
                create_instance(profile_switch(dev_profile),args.db_snapshot, config)
            else:
                print("Can't proceed for because the snapshot is not available - {} current status - {}".format(args.db_snapshot,snapshot_status))
                os._exit(1)
            # delete_snapshot(profile_switch(prod_profile),db_snapshot=args.db_snapshot)
        else:            
            status=cluster__status(profile_switch(prod_profile), db_instance=args.db_instance)
            if status != 'available':
                print("Can't proceed for taking snapshot as the cluster status is not available. current status - ", status)
                os._exit(1)
            else:
                if args.force:
                    pretty_print(["Creating snapshot","Running",str(datetime.datetime.now()),time_since(code_start_time)])
                    create_snapshot(profile_switch(prod_profile),config)
                # else:
                #     pretty_print(["Creating instance from latest snapshot","Running",str(datetime.datetime.now()),time_since(code_start_time)])
                #     create_instance(profile_switch(dev_profile),config)
    get_overall_execution_time = time_since(code_start_time)
    pretty_print(["Auto kitchen setup completed","Completed",str(datetime.datetime.now()),time_since(code_start_time)])
    print("Code overall execution time : {}".format(get_overall_execution_time))

if __name__ == '__main__':
    main()