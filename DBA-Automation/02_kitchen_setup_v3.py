#!/usr/bin/env python
# -*- coding: utf-8 -*-
# This code is for running the kitchen setup separately
# run with -s <full-path-sql-dir>
# command - python kitchen_setup.py -s kitchen_sqls_test -o emea-cust-360-dev1dev-cluster.cluster-cct3lkcjopzt.us-west-2.rds.amazonaws.com -n emea-cust-360-dev2dev-cluster.cluster-cct3lkcjopzt.us-west-2.rds.amazonaws.com -m emea-cust-360-dev1pd-cluster.cluster-cct3lkcjopzt.us-west-2.rds.amazonaws.com
# emoji used in this code
# 🚧 ---> inprogress
# ⏳ ---> started
# ✅ ---> completed
# ❌ ---> error


from __future__ import print_function
from ast import arg
from asyncio.log import logger
from distutils.command.config import config
from hashlib import new
from http import client
import io
from operator import contains, mul
import profile
import random
import shutil
from traceback import print_tb
from re import search
import traceback
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
import subprocess
import threading
from datetime import datetime as dt
from sh import tail
from pgsanity import sqlprep
from pgsanity import ecpg
import math
import re
import sqlparse
from git.repo.base import Repo

utc = pytz.UTC
warnings.simplefilter("ignore")

# with open('config.json', 'r') as f:
#     __data = json.load(f)
header_format="""
Sample:\n\n/*            !!! Connect to EMEA_CUST_360 DB as EMEA_SU in NEW DEV !!!                       */ 
EMEA_CUST_360   ===> DB name to connect
EMEA_SU         ===> User name to connect
NEW DEV         ===> Host name to connect\n"""
__version__ = '0.0.2'
validate_headers = ['connect','to','db','as','in','on']
pretty = prettytable.PrettyTable()
pretty.field_names = ['                                       Current Operation                                    ','   Status  ','         Timestamp        ',' Status code ', 'Elapsed Time']
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
run_check = False
emea_cust_360_admin={'user': 'emea_cust_360_admin', 'pass': 'ljc#/v7)A%gSH&/b'}
emea_su={'user': 'emea_su', 'pass': 'Ma8sdu0Mma8sd!'}

authentications={
    'emea_cust_360_admin' : [
        {
            'user': 'emea_cust_360_admin',
            'pass': 'ljc#/v7)A%gSH&/b'
        }
    ],
    'emea_su' : [
        {
            'user': 'emea_su',
            'pass': 'Ma8sdu0Mma8sd!'
        }
    ],
    'admin' : [
        {
            'user': 'emea_cust_360_admin',
            'pass': 'ljc#/v7)A%gSH&/b'
        }
    ]
}

def overall_time():
    get_overall_execution_time = time_since(code_start_time)
    print("Code overall execution time : {}".format(get_overall_execution_time))

def pretty_print(row_val, new_row=None):
    if pretty.__getattr__('rowcount') > 1:
        del_row = pretty.__getattr__('rowcount') - 1
        # print(del_row)
        pretty.del_row(del_row)
    #     pretty.del_row(2)
    pretty.add_row(row_val)
    pretty_new = pretty.get_string(start=pretty.__getattr__('rowcount')-1)
    print("\n".join(pretty_new.split('\n')[-2:]))


# def time_since(started, current=None):
def time_since(*arg):
    if len(arg) != 0: 
        elapsedTime = time.time() - arg[0];
        #print(elapsedTime);
        hours = math.floor(elapsedTime / (60*60))
        elapsedTime = elapsedTime - hours * (60*60);
        minutes = math.floor(elapsedTime / 60)
        elapsedTime = elapsedTime - minutes * (60);
        seconds = math.floor(elapsedTime);
        elapsedTime = elapsedTime - seconds;
        ms = elapsedTime * 1000;
        if(hours != 0):
            return ("%dh %dm %ds" % (hours, minutes, seconds)) 
        elif(minutes != 0):
            return ("%dm %ds" % (minutes, seconds))
        elif(seconds != 0):
            return ("%ds" % (seconds))
        else :
            return ("%dms" % (ms))
    else:
        #print ('does not exist. here you go.');
        return time.time()

code_start_time = time_since();

def validate_header(args, sql_path, file):
    current = time.time()
    pretty_print(["Validating headers : {}".format(file),"InProgress",str(datetime.datetime.now()),"🚧",time_since(current, time.time())])
    file_name = os.getcwd() + "/" + sql_path + "/" + file
    try:
        first_check = "!!!"
        second_line = open(file_name).readlines()[1]
        if re.findall(first_check,second_line):
            second = second_line.split("!!!")[1].split(" ")
            second_check_list = list(filter(None, second))
            # first check !!! in the header
            # second check is to count the value of db, user, and host it should match 3 count
            first_check = "!!!"
            second_check = 5
            # print(second_check_list)
            # print(validate_headers)
            sc = 1
            a = [x.lower() for x in validate_headers]
            for m in second_check_list:  # search against bigger list  
                if m.lower() in a:
                    sc += 1
                    # print(m)
            # print(sc)
            # print(search(second_check_list, validate_headers))
            if re.findall(first_check,second_line):
                if sc > second_check :
                    pretty_print(["Validating headers : {} completed".format(file),"Completed",str(datetime.datetime.now()),"✅",time_since(current, time.time())])
                else:
                    pretty_print(["Validating headers : {}".format(file),"HeaderErr",str(datetime.datetime.now()),"❌",time_since(current, time.time())])
                    print("Error: \n")
                    print("\nSome of the connection string not there in {} at line 2.\n{}".format(file,header_format))
                    print("\nError here:\n{}".format(second_line))
                    overall_time()
                    sys.exit(1)    
            else:
                pretty_print(["Validating headers : {}".format(file),"HeaderErr",str(datetime.datetime.now()),"❌",time_since(current, time.time())])
                print("Error: \n")
                print("Missing header in {}".format(file))
                print("\nHeaders are the mandatory comments that needs to be there in {} at line 2.\n{}".format(file,header_format))
                overall_time()
                sys.exit(1)
        else:
            pretty_print(["Validating headers : {}".format(file),"NotFound",str(datetime.datetime.now()),"❌",time_since(current, time.time())])
            print("Error: \n")
            print("Missing header in {}".format(file))
            print("\nHeaders are the mandatory comments that needs to be there in {} at line 2.\n{}".format(file,header_format))
            overall_time()
            sys.exit(1)
        result = None
    except Exception as e:
        pretty_print(["Validating headers : {} ".format(file),"ErrorBelow",str(datetime.datetime.now()),"❌",time_since(current, time.time())])
        # print(e)
        raise
        sys.exit(1)

    return result

def follow(thefile):
    '''generator function that yields new lines in a file
    '''
    # seek the end of the file
    # thefile.seek(0, os.SEEK_END)
    thefile.seek(0, 2)
    
    # start infinite loop
    while True:
        # read last line of file
        line = thefile.readline()
        ignore_list = ['-------','row)',]
        actual_line = "\n".join([x.strip() for x in line.splitlines() if x not in ignore_list])
        # sleep if file hasn't been updated
        log_file_name = line.replace("log","sql")
        if "{} is DONE".format(log_file_name) not in line:
            if not actual_line:
                time.sleep(0.1)
                continue

        yield actual_line

def description():
    return """This is a automation for configuring Auto kitchen setup."""

def check_file(sql_path, filename=None, show_filename=False, add_semicolon=False):
    """
    Check whether an input file is valid PostgreSQL. If no filename is
    pasted, STDIN is checked.

    Returns a status code: 0 if the input is valid, 1 if invalid.
    """
    # either work with sys.stdin or open the file
    current = time.time()
    file = os.getcwd() + "/" + sql_path + "/" + filename
    pretty_print(["Validating syntax : {}".format(filename),"InProgress",str(datetime.datetime.now()),"🚧",time_since(current, time.time())])
    if file is not None:
        with open(file, "r") as filelike:
            sql_string = filelike.read()
    else:
        with sys.stdin as filelike:
            sql_string = sys.stdin.read()

    success, msg = check_string(sql_string, add_semicolon=add_semicolon)

    # report results
    result = 0
    if not success:
        # possibly show the filename with the error message
        prefix = ""
        if show_filename and filename is not None:
            prefix = sql_path + "/" + filename + ": "
        pretty_print(["Validating syntax : {}".format(filename),"SyntaxErr",str(datetime.datetime.now()),"❌",time_since(current, time.time())])
        print("Error:\n")
        print(prefix + msg)
        result = 1
        print("\nPlease fix the above syntax error before executing it again")
        sys.exit(1)
    else:
        pretty_print(["Validating syntax : {} completed".format(filename),"Completed",str(datetime.datetime.now()),"✅",time_since(current, time.time())])

    return result

def check_string(sql_string, add_semicolon=False):
    """
    Check whether a string is valid PostgreSQL. Returns a boolean
    indicating validity and a message from ecpg, which will be an
    empty string if the input was valid, or a description of the
    problem otherwise.
    """
    prepped_sql = sqlprep.prepare_sql(sql_string, add_semicolon=add_semicolon)
    success, msg = ecpg.check_syntax(prepped_sql)
    return success, msg

def check_files(files, sql_path, add_semicolon=False):
    if files is None or len(files) == 0:
        return check_file(add_semicolon=add_semicolon)
    else:
        # show filenames if > 1 file was pasted as a parameter
        show_filenames = (len(files) > 1)
        accameulator = 0
        for filename in files:
            file_name_copy = os.getcwd() + "/" + sql_path + "/." + filename
            if os.path.exists(file_name_copy):
                os.remove(file_name_copy)
            if filename.endswith('sql') and not filename.startswith("99_KITCHEN_CLEANUP") and not filename.endswith("HELPFULL_QUERIES.sql"):
                accameulator |= check_file(sql_path, filename, show_filenames, add_semicolon=add_semicolon)
        return accameulator

class Tee(object):
    def __init__(self, *files):
        self.files = files
    def write(self, obj):
        for f in self.files:
            f.write(obj)
    def flush(self):
        pass

def main():
    # aws_region='us-east-1'
    parser = argparse.ArgumentParser(description=description(),
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-s", "--sql-path", default=None, help="sql directory path")
    parser.add_argument("-o", "--old-dev", help="old-dev instance cluster name")
    parser.add_argument("-n", "--new-dev", help="new-dev instance cluster name")
    parser.add_argument("-m", "--master", help="master instance cluster name")
    parser.add_argument("-r", "--replication", action="store_true", help="for setup replication")
    parser.add_argument("-k", "--kitchen-setup", action="store_true", help="for setup kitchen")
    parser.add_argument("-d", "--use-dummy", action="store_true", help="use dummy sqls from git not from local")
    parser.add_argument("-a", "--validate-sqls", action="store_true", help="use this option to validate syntax and headers. This won't execute the actual code.")
    parser.add_argument("-v", '--version', action="store_true", help='version')
    args = parser.parse_args()
    config = parser.parse_args()
    # config = vars(args)
    print(config)

    if args.replication:
        setup_type = "Replication"
    elif args.kitchen_setup:
        setup_type = "Kitchen"
    ### git download commands
    # rm -rf CMDB_DBA
    # git clone git@github.azc.ext.hp.com:emea-c360-org/CMDB_DBA.git
    # git clone git@github.com:Dhanasekar93/checks.git
    # rm -rf /home/ec2-user/automation/dummy_kitchen
    # cp -r CMDB_DBA/kitchen/dummy_kitchen /home/ec2-user/automation/dummy_kitchen

    if args.sql_path is None:
        # git_url = "git@github.com:Dhanasekar93/checks.git"
        git_url = "git@github.azc.ext.hp.com:emea-c360-org/CMDB_DBA.git"

        temp_dir = "/tmp/git-pull-" + str(random.randint(0,1000))
        os.makedirs(temp_dir,exist_ok=True)

        if args.replication:
            if args.use_dummy:
                sql_path = "dummy_replication"
                git_path = temp_dir + "/replication/dummy_replication"
            else:
                sql_path = "replication"
                git_path = temp_dir + "/replication"
        if args.kitchen_setup:
            if args.use_dummy:
                sql_path = "dummy_kitchen"
                git_path = temp_dir + "/kitchens/dummy_kitchen"
            else:
                sql_path = "kitchen"
                git_path = temp_dir + "/kitchens"

        if os.path.exists(sql_path) and os.path.isdir(sql_path):
            shutil.rmtree(sql_path)

        os.makedirs(sql_path,exist_ok=True)

        Repo.clone_from(git_url, temp_dir)

        print("Downloading the repo to {} folder and moved to this {}".format(temp_dir,os.path.join(os.getcwd(),sql_path)))
        # fetch all files
        # for path in git_path):
        for file_name in os.listdir(git_path):
            # construct full file path
            source = git_path + "/" + file_name
            destination = os.getcwd() + "/" +sql_path + "/" + file_name
            # move only files
            if os.path.isfile(source):
                shutil.move(source, destination)
        shutil.rmtree(temp_dir,ignore_errors = True)
    else:
        sql_path = args.sql_path

    now = dt.now()
    time_folder = now.strftime("%Y-%m-%d-%H-%M-%S")
    global log_path
    log_path = os.getcwd() + "/" + "kitchen_logs/" + time_folder
    logging_file = log_path + '/execution.logs'
    global log_to_file
    os.makedirs(log_path,exist_ok=True)
    log_to_file = open(logging_file,"w")
    f = open(logging_file, 'w')
    backup = sys.stdout
    sys.stdout = Tee(sys.stdout, f)
    print(pretty)
    pretty_print(["Auto {} setup".format(setup_type)," Started ",str(datetime.datetime.now()),"⏳",time_since(code_start_time)])

    # print(client)
    if args.version:
        # print("Auto kitchen setup - version : ",__version__)
        pretty_print(["Fetching Code version : Verion : {}".format(__version__),"Completed",str(datetime.datetime.now()),"✅",time_since(code_start_time)])
    # raw_psql_command = """psql -p5432 -U username_ database_name_ -f query_file_name_ > log_file_name_ 2> /tmp/error &"""
    elif args.validate_sqls:
        can_break = False
        # files = os.listdir(sql_path)
        files = sorted(os.listdir(sql_path))
        check_files(files, sql_path, add_semicolon=True)

        for file in files:
            if file.endswith('sql') and not file.startswith("99_KITCHEN_CLEANUP") and not file.endswith("HELPFULL_QUERIES.sql"):
                file_with_path = sql_path + "/" + file
                file_name_copy = os.getcwd() + "/" + sql_path + "/." + file
                if os.path.exists(file_name_copy):
                    os.remove(file_name_copy)
                if int(os.path.getsize(file_with_path) > 0):
                    validate_header(args, sql_path, file)
    else:
        files = sorted(os.listdir(sql_path))

        check_files(files, sql_path, add_semicolon=True)

        for file in files:
            if file.endswith('sql') and not file.startswith("99_KITCHEN_CLEANUP") and not file.endswith("HELPFULL_QUERIES.sql"):
                # os.remove(file_name_copy)
                file_name_copy = os.getcwd() + "/" + sql_path + "/." + file
                if os.path.exists(file_name_copy):
                    os.remove(file_name_copy)
                file_with_path = sql_path + "/" + file
                if int(os.path.getsize(file_with_path) > 0):
                    validate_header(args, sql_path, file)

        for file in files:
            if file.endswith('sql') and not file.startswith("99_KITCHEN_CLEANUP") and not file.endswith("HELPFULL_QUERIES.sql"):
                file_with_path = sql_path + "/" + file
                if int(os.path.getsize(file_with_path) > 0):
                    current = time.time()
                    # raw_psql_command = "psql -p5432 -U username_ database_name_ -f query_file_name_ > log_file_name_ 2> /tmp/error &"
                    file_name = os.getcwd() + "/" + sql_path + "/" + file
                    file_name_copy = os.getcwd() + "/" + sql_path + "/." + file
                    pretty_print(["Fetching connection details from {}".format(file),"Started",str(datetime.datetime.now()),"⏳",time_since(current, time.time())])
                    second_line = open(file_name).readlines()[1]
                    log_file_name = log_path + "/" + file.replace("sql","log")
                    log_file_name_print = log_file_name.replace(log_path,'"$(pwd)"/kitchen_logs/{}'.format(time_folder))
                    try:
                        db_name = second_line.split("!!!")[1].partition("to ")[2].split(" ")[0]
                        user_name = second_line.split("!!!")[1].partition("as ")[2].split(" ")[0]
                        if "in" in second_line.split("!!!")[1]:
                            host = second_line.split("!!!")[1].partition("in ")[2]
                        elif "on" in second_line.split("!!!")[1]:
                            host = second_line.split("!!!")[1].partition("on ")[2]
                        # print(host)
                        if host.rstrip().upper() == "OLD DEV":
                            host_name = args.old_dev
                        elif host.rstrip().upper() == "NEW DEV":
                            host_name = args.new_dev
                        elif host.rstrip().upper() == "MASTER":
                            host_name = args.master
                        elif host.rstrip().upper() == "OLD SLAVE":
                            host_name = args.old_dev
                        elif host.rstrip().upper() == "NEW SLAVE":
                            host_name = args.new_dev
                        user = authentications[user_name.lower()][0]['user']
                        password = authentications[user_name.lower()][0]['pass']
                        pretty_print(["Fetching connection details from : {}".format(file),"Completed",str(datetime.datetime.now()),"✅",time_since(current, time.time())])
                    except Exception as e:
                        pretty_print(["Fetching connection details from : {} ".format(file),"ErrorBelow",str(datetime.datetime.now()),"❌",time_since(current, time.time())])
                        # print(e)
                        raise
                        sys.exit(1)
                    if os.path.exists(file_name_copy):
                        os.remove(file_name_copy)
                    with open(file_name_copy, "w") as f:
                        sql_content = open(file_name, "r")
                        content = sqlparse.format(sql_content, strip_comments=True).strip()
                        f.write(content)
                    # psql_command = """export PGPASSWORD='{}'; echo "Executing {} in host - {}\n\n" > "{}"; psql -p5432 -U {} --host {} {} -c '\\timing' -f "{}" -c "SELECT '{} is DONE';" --echo-all >> "{}" 2> /tmp/error & """.format(password,file_name,host_name,log_file_name,user,host_name,db_name.lower(),file_name,file,log_file_name)
                    # psql_command = """export PGPASSWORD='{}'; echo "Executing {} in host - {}\n\n" > "{}"; export PGOPTIONS='-c client_min_messages=DEBUG5' ; psql -p5432 -U {} --host {} {} -c '\\timing' -f "{}" -c "SELECT '{} is DONE';" --echo-all >> "{}" 2> /tmp/error & """.format(password,file_name,host_name,log_file_name,user,host_name,db_name.lower(),file_name_copy,file,log_file_name)
                    psql_command = """export PGPASSWORD='{}'; echo "Executing {} in host - {}\n\n" > "{}"; psql -p5432 -U {} --host {} {} -c '\\timing' -f "{}" -c "SELECT '{} is DONE';" --echo-queries -v ON_ERROR_STOP=1 >> "{}" 2>&1 & """.format(password,file_name,host_name,log_file_name,user,host_name,db_name.lower(),file_name_copy,file,log_file_name,log_file_name)
                    # print(file_name,log_file_name,db_name,user_name)
                    pretty_print(["Executing : {} in {}".format(file,host),"InProgress",str(datetime.datetime.now()),"🚧",time_since(current, time.time())])
                    current = time.time()

                    try:
                        subprocess.check_output(psql_command,shell=True,stderr=subprocess.STDOUT)
                        while not os.path.exists(log_file_name):
                            time.sleep(0.5)
                        logfile = open(log_file_name,"r")
                        loglines = follow(logfile)
                        # loglines = tail(logfile,5)
                        # while not run_check:
                        print("Logs:")
                        for line in loglines:
                            if re.search("ERROR",line):
                                pretty_print(["Executing : {} in {}".format(file,host),"ErrorBelow",str(datetime.datetime.now()),"❌",time_since(current, time.time())])
                                error = ""
                                from_str = "psql:{}".format(file_name_copy)
                                to_str = "psql:{}".format(file)
                                line = re.sub(from_str,to_str,line)
                                # for line in logfile:
                                error += line
                                print("Error here:\n{}".format(error))
                                overall_time()
                                # line = line.replace('\r', '')
                                print("Full logs is in {}".format(log_file_name_print))
                                os.remove(file_name_copy)
                                sys.exit(1)
                            elif "{} is DONE".format(file) not in line:
                                # print(line.replace('\n',' '), end='\r')
                                done = len(line)
                                from_str = "psql:{}".format(file_name_copy)
                                to_str = "psql:{}".format(file)
                                if re.search(from_str, line):
                                    line = re.sub(from_str,to_str,line)
                                togo = 200-done
                                togo_str = ''*int(togo)
                                if done < 200:
                                    # sys.stdout = backup
                                    print(f'{line}{togo_str}', end='\r')    
                                    # text = line.ljust(togo)
                                    # # text = line
                                    # print(text, end='\r')   
                                # else:
                                #     text = line[0:200]
                                #     print(text, end='\r') 
                            else:
                                print("\nFull logs is in {}".format(log_file_name_print))
                                break
                        pretty_print(["Executing : {} in {}".format(file,host),"Completed",str(datetime.datetime.now()),"✅",time_since(current, time.time())])
                        os.remove(file_name_copy)
                            # continue
                        # while not can_break:
                        #     print(subprocess.STDOUT.readline())
                    except subprocess.CalledProcessError as e:
                        pretty_print(["Executing : {} in {}".format(file,host),"ErrorBelow",str(datetime.datetime.now()),"❌",time_since(current, time.time())])
                        print(e)
                        overall_time()
                        sys.exit(1)

    pretty_print(["Auto {} setup".format(setup_type),"Completed",str(datetime.datetime.now()),"✅",time_since(code_start_time)])
    print("""All the logs are stored under this "{}" folder""".format(log_path))
    overall_time()

if __name__ == '__main__':
    main()
