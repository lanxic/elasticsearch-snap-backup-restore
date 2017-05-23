#!/usr/bin/python
import os
import sys, socket, io, json, getopt, time
import requests, logging, boto3
from datetime import datetime
from elasticsearch import Elasticsearch
from subprocess import *


GTM_APPS_ES_VERSION = '1.1'
GTM_APPS_ES_HOST = ''
GTM_APPS_ES_PORT = ''
GTM_CONFIG_INI = ''

SG_DO_ES_CONF = False
SG_DO_ES_HOST = False
SG_DO_ES_PORT = False

fb_File = None

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

def check_file(aws_access_key_id,aws_secret_access_key,bucket):
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
    listBucket = s3.list_objects(Bucket=bucket)
    files3Bucket = {}
    for object in listBucket["Contents"]:
        files3Bucket.update(object)
    fb_File = files3Bucket['Key']
    return fb_File

def es_write_log(loglevel,logmsg):
    logging.basicConfig(filename='Elasticsearch-backup-restore.log',level=logging.DEBUG)
    # logging.debug(arg)
    FORMAT = '%(asctime)s - %(name)s - [%(process)d] - %(levelname)s - %(message)s'
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

        getFileBackup = rdMain['GTM_APPS_ES_REPONAME']+"_"+datetime.now().strftime("%Y%m%d")+".tar.gz"

        s3 = boto3.client('s3', aws_access_key_id=rdMain['GTM_APPS_ES_S3_ACCESS_KEY'],aws_secret_access_key=rdMain['GTM_APPS_ES_S3_SECRET_KEY'])

        es_write_log("info", "remove old file backup to aws S3 ..."+getFileBackup)
        s3.delete_object(Bucket=rdMain['GTM_APPS_ES_S3_BUCKET'],Key=getFileBackup)

        es = Elasticsearch([{'host': host, 'port': port, 'timeout': 1000}])
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
                es_write_log("info", "Create snapshots ...")
                es.snapshot.create(repository=rdMain['GTM_APPS_ES_REPONAME'], snapshot=rdMain['GTM_APPS_ES_SNAPNAME'], body=json.dumps(rdSnapIndecs), wait_for_completion=True)
                es_write_log("info", "Trying to compress snapshots ...")
                check_call(["tar", "-czf",rdMain['GTM_APPS_ES_BACKUPDIR']+"/"+rdMain['GTM_APPS_ES_REPONAME']+"_"+datetime.now().strftime("%Y%m%d")+".tar.gz", "/var/elasticsearch/snapshot", "-P"])
                es_write_log("info", "Compress snapshots done ... file: "+rdMain['GTM_APPS_ES_BACKUPDIR']+"/"+getFileBackup)
                es_write_log("info", "remove old file backup to aws S3 ...")
                s3.delete_object(Bucket=rdMain['GTM_APPS_ES_S3_BUCKET'],Key=rdMain['GTM_APPS_ES_REPONAME']+"_"+datetime.now().strftime("%Y%m%d")+".tar.gz")
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

        """Checks if the files are ready.
        For a file to be ready it must exist and can be opened in append
        mode.
        """
        es_write_log("info", "Checking file backup to aws S3 ...")

        filesTarget = rdMain['GTM_APPS_ES_REPONAME']+"_"+datetime.now().strftime("%Y%m%d")+".tar.gz"
        wait_time = 5
        x = False

        # # If the file doesn't exist, wait wait_time seconds and try again
        # # until it's found.
        while not x:
            # print "%s hasn't arrived. Waiting %s seconds." % (filesTarget, wait_time)
            fbFile = check_file(rdMain['GTM_APPS_ES_S3_ACCESS_KEY'],rdMain['GTM_APPS_ES_S3_SECRET_KEY'],rdMain['GTM_APPS_ES_S3_BUCKET'])
            x = filesTarget == fbFile
            time.sleep(wait_time)

        es_write_log("info", "Download file backup to aws S3 ...")
        s3.download_file(rdMain['GTM_APPS_ES_S3_BUCKET'], GTM_APPS_ES_FILE_RESTORE , rdMain['GTM_APPS_ES_BACKUPDIR']+"/"+GTM_APPS_ES_FILE_RESTORE)
        es_write_log("info", "Downloading S3 done ...")

        es_write_log("info", "Trying to extract snapshots ...")
        check_call(["sudo", "tar", "-xf",rdMain['GTM_APPS_ES_BACKUPDIR']+"/"+rdMain['GTM_APPS_ES_REPONAME']+"_"+datetime.now().strftime("%Y%m%d")+".tar.gz", "-P"])

        es = Elasticsearch([{'host': host, 'port': port, 'timeout': 7200}])

        indx = ['news','promotion_suggestions','coupon_suggestions','store_details','stores','store_suggestions','malls','promotions','coupons','activities','news_suggestions','mall_suggestions']
        for x in indx:
            es_write_log("info","Trying to clean index ... "+x)
            cleanAll = es.indices.delete(index=x)
        regRepo = es.snapshot.create_repository(repository=rdMain['GTM_APPS_ES_REPONAME'], body=json.dumps(rdRepo))
        try:
            es_write_log("info","Trying to restore snapshots ...")
            es.snapshot.restore(repository=rdMain['GTM_APPS_ES_REPONAME'], snapshot=rdMain['GTM_APPS_ES_SNAPNAME'], body=json.dumps(rdSnapIndecs), wait_for_completion=True)
            es_write_log("info", "Restore snapshots done")
            es_write_log("info", "Remove local download file")
            check_call(["sudo", "rm", "-f",rdMain['GTM_APPS_ES_BACKUPDIR']+"/"+GTM_APPS_ES_FILE_RESTORE])
            es_write_log("info", "Service restore Stopped ...")
            es_write_log("info", "Trying restarting Elasticsearch ...")
            check_call(["sudo", "service", "elasticsearch","restart"])
            es_write_log("info", "Restarting Elasticsearch done ...")

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
