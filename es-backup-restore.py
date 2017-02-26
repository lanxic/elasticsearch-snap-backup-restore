#!/usr/bin/python
import os
import sys, socket, io, json, getopt
import requests, logging, boto3
from datetime import datetime
from elasticsearch import Elasticsearch
from subprocess import *

GTM_APPS_ES_VERSION = '1.0'
GTM_APPS_ES_HOST = ''
GTM_APPS_ES_PORT = ''
GTM_APPS_ES_CONF = ''

SG_DO_ES_CONF = False
SG_DO_ES_HOST = False
SG_DO_ES_PORT = False

def info_main():
   print "Usage: %s [OPTIONS]" % os.path.basename(sys.argv[0])
   print "example: %s -c es-config.json -a localhost -p 9200 -b backup|restore" % os.path.basename(sys.argv[0])
   print ""
   print('Where OPTIONS:')
   print('-a HOST       Specify URL address of the Elasticsearch server')
   print('-p PORT       Specify Port of the Elasticsearch server')
   print('-c FILE       Specify path of config file')
   print('-h            Printing the help')
   print('-b            To do job ex:backup or restore')
   print('-v            Print the version')
   print ""

def es_write_log(loglevel,logmsg):
    # logging.basicConfig(filename='Elasticsearch-backup-restore.log',level=logging.DEBUG)
    # logging.debug(arg)
    FORMAT = '%(asctime)s - %(name)s - [%(process)d] - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT)
    logger = logging.getLogger(socket.gethostname())
    logger.setLevel(logging.DEBUG)
    if loglevel == "debug":
        logger.debug(logmsg)
    elif loglevel == "info":
        logger.info(logmsg)
    elif loglevel == "warn":
        logger.warn(logmsg, exc_info=True)
    elif loglevel == "error":
        logger.error(logmsg,exc_info=True)
    elif loglevel == "critical":
        logger.critical(logmsg, exc_info=True)

def do_backup(host, port, fileConfig):
    try:
        es_write_log("info", "Service backup now running ...")
        # read json file
        configMain = open(fileConfig)
        configRepo = open("config/create_repo.json")
        configSnapIndecs = open("config/create_snap_indices.json")

        rdMain = json.loads(configMain.read())
        rdRepo = json.loads(configRepo.read())
        rdSnapIndecs = json.loads(configSnapIndecs.read())
        # print rdMain['datadir']

        s3 = boto3.client('s3', aws_access_key_id=rdMain['GTM_APPS_ES_S3_ACCESS_KEY'],aws_secret_access_key=rdMain['GTM_APPS_ES_S3_SECRET_KEY'])

        es = Elasticsearch([{'host': host, 'port': port, 'timeout': 500}])
        # print(es.info())
        regRepo = es.snapshot.create_repository(repository=rdMain['GTM_APPS_ES_REPONAME'], body=json.dumps(rdRepo))
        # checkRegRepo = regRepo['acknowledged']
        try:
            es_write_log("info", "Checking snapshots already exists or not ...")
            statusRegSnapRepo = es.snapshot.status(repository=rdMain['GTM_APPS_ES_REPONAME'], snapshot=rdMain['GTM_APPS_ES_SNAPNAME'],ignore=[400, 404])
            checkRegSnapRepo = json.dumps(statusRegSnapRepo['snapshots'][0]['snapshot'])
            es_write_log("warn", "Snapshot for " + checkRegSnapRepo + " already exists")
            es_write_log("info", "Service backup Stopped ...")

        except Exception as e:
            try:
                getFileBackup = rdMain['GTM_APPS_ES_REPONAME']+"_"+datetime.now().strftime("%Y%m%d")+".tar.gz"
                es_write_log("info", "Create snapshots ...")
                es.snapshot.create(repository=rdMain['GTM_APPS_ES_REPONAME'], snapshot=rdMain['GTM_APPS_ES_SNAPNAME'], body=json.dumps(rdSnapIndecs), wait_for_completion=True)
                es_write_log("info", "Trying to compress snapshots ...")
                check_call(["tar", "-czf",rdMain['GTM_APPS_ES_BACKUPDIR']+"/"+rdMain['GTM_APPS_ES_REPONAME']+"_"+datetime.now().strftime("%Y%m%d")+".tar.gz", "/var/elasticsearch/snapshot", "-P"])
                es_write_log("info", "Compress snapshots done ... file: "+rdMain['GTM_APPS_ES_BACKUPDIR']+"/"+getFileBackup)
                es_write_log("info", "Upload file backup to aws S3 ...")
                s3.upload_file(rdMain['GTM_APPS_ES_BACKUPDIR']+"/"+getFileBackup, rdMain['GTM_APPS_ES_S3_BUCKET'], getFileBackup)
                es_write_log("info", "Uploading S3 done ...")
                es_write_log("info", "Trying to retention file compress snapshots ...")
                check_call(["find", rdMain['GTM_APPS_ES_BACKUPDIR']+"/", "-name", "*.tar.gz", "-type", "f" ,"-mtime", "+3", "-delete"])
                es.snapshot.delete(repository=rdMain['GTM_APPS_ES_REPONAME'], snapshot=rdMain['GTM_APPS_ES_SNAPNAME'])
                es_write_log("info", "Service backup Stopped ...")

            except Exception as e:
                es_write_log("error",e)
                sys.exit(1)

    except Exception as e:
        es_write_log("error",e)
        sys.exit(e)

def do_restore(host, port, fileConfig):
    try:
        es_write_log("info", "Service restore now running ...")

        configMain = open(fileConfig)
        configRepo = open("config/create_repo.json")
        configSnapIndecs = open("config/create_snap_indices.json")

        rdMain = json.loads(configMain.read())
        rdRepo = json.loads(configRepo.read())
        rdSnapIndecs = json.loads(configSnapIndecs.read())

        GTM_APPS_ES_FILE_RESTORE = rdMain['GTM_APPS_ES_REPONAME']+"_"+datetime.now().strftime("%Y%m%d")+".tar.gz"

        s3 = boto3.client('s3', aws_access_key_id=rdMain['GTM_APPS_ES_S3_ACCESS_KEY'],aws_secret_access_key=rdMain['GTM_APPS_ES_S3_SECRET_KEY'])

        es_write_log("info", "Download file backup to aws S3 ...")
        s3.download_file(rdMain['GTM_APPS_ES_S3_BUCKET'], GTM_APPS_ES_FILE_RESTORE , rdMain['GTM_APPS_ES_BACKUPDIR']+"/"+GTM_APPS_ES_FILE_RESTORE)
        es_write_log("info", "Downloading S3 done ...")

        es_write_log("info", "Trying to extract snapshots ...")
        check_call(["sudo", "tar", "-xf",rdMain['GTM_APPS_ES_BACKUPDIR']+"/"+rdMain['GTM_APPS_ES_REPONAME']+"_"+datetime.now().strftime("%Y%m%d")+".tar.gz", "-P"])

        es = Elasticsearch([{'host': host, 'port': port, 'timeout': 500}])

        es_write_log("info","Trying to clean index ...")
        cleanAll = es.indices.delete(index='_all')
        regRepo = es.snapshot.create_repository(repository=rdMain['GTM_APPS_ES_REPONAME'], body=json.dumps(rdRepo))
        try:
            es_write_log("info","Trying to restore snapshots ...")
            es.snapshot.restore(repository=rdMain['GTM_APPS_ES_REPONAME'], snapshot=rdMain['GTM_APPS_ES_SNAPNAME'], body=json.dumps(rdSnapIndecs), wait_for_completion=True)
            es_write_log("info", "Restore snapshots done")
            es_write_log("info", "Remove local download file")
            check_call(["sudo", "rm", "-f",rdMain['GTM_APPS_ES_BACKUPDIR']+"/"+GTM_APPS_ES_FILE_RESTORE])
            es_write_log("info", "Service restore Stopped ...")

        except Exception as e:
            es_write_log("error",e)
            sys.exit(1)

    except Exception as e:
        es_write_log("error",e)

def main(argv):
   if len(sys.argv) == 1:
       info_main()
   try:
      opts, args = getopt.getopt(argv,"c:a:p:b:hv")
   except getopt.GetoptError as err:
       es_write_log("error",err)
       info_main()
       sys.exit(2)
   for opt, arg in opts:
        if opt == "-v":
            print 'Version:', GTM_APPS_ES_VERSION
        elif opt == "-c":
            GTM_APPS_ES_CONF = arg
            SG_DO_ES_CONF = True
        elif opt == "-a":
            GTM_APPS_ES_HOST = arg
            SG_DO_ES_HOST = True
        elif opt == "-p":
            GTM_APPS_ES_PORT = arg
            SG_DO_ES_PORT = True
        elif opt == "-b":
            GTM_APPS_ES_TODO = arg
            try:
                if SG_DO_ES_CONF|SG_DO_ES_HOST|SG_DO_ES_PORT == True:
                    if GTM_APPS_ES_TODO == "backup":
                        do_backup(GTM_APPS_ES_HOST,GTM_APPS_ES_PORT,GTM_APPS_ES_CONF)
                    elif GTM_APPS_ES_TODO == "restore":
                        do_restore(GTM_APPS_ES_HOST,GTM_APPS_ES_PORT,GTM_APPS_ES_CONF)

            except Exception as e:
                es_write_log("error",e)
                sys.exit(e)
        elif opt in ("-h"):
            info_main()
            sys.exit()

if __name__ == "__main__":
   main(sys.argv[1:])
