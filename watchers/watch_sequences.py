#!/usr/bin/env python
import firebase_admin

from firebase_admin import credentials
from firebase_admin import storage

import six
import os, re
import itertools as it

from process_frontend import make_files
from init_blat import init_blat
from init_tophat import init_tophat
from shutil import copyfile

import requests



#DO_FRONTEND = False
#DO_BACKEND = True

cred = credentials.Certificate('/home/ben_coolship_io/.ssh/jwein-206823-firebase-adminsdk-askpb-7709f1ff8f.json')
firebase_admin.initialize_app(cred, {
    'databaseURL' : 'https://jwein-206823.firebaseio.com/',
    'storageBucket': 'slides.dna-microscopy.org'

})

from firebase_admin import db
root = db.reference("datasets/all_v2")
DATAROOT = "/slides/all_v2/website_datasets"

NAME=__file__[:-3]
tmp_root = os.path.join(f"/data/tmp/{NAME}/")
if not os.path.isdir(tmp_root): os.makedirs(tmp_root)


def process_frontend(folder, dataset):
    #get transformed files
    files_for_upload = make_files(folder, dataset)
    return files_for_upload


def upload_frontend_files(files,dataset, dataset_key):
    bucket = storage.bucket()

    urls = []
    for nm,fpath in files.items():
        with open(fpath,"rb") as file_stream:
            basename,extension = (fpath.split("/")[-1].split(".")[0]\
                                 ,".".join( fpath.split("/")[-1].split(".")[1:]))
            bucket_fn = os.path.join(f"all_v2/website_datasets/{dataset['userId']}/{basename}_{dataset['dataset']}.{extension}")
            textblob = bucket.blob(bucket_fn)
            textblob.cache_control = 'no-cache'
            textblob.content_encoding = 'gzip'
            textblob.upload_from_string(file_stream.read(),content_type="application/octet-stream")
            textblob.reload()
            req = requests.get(textblob.public_url)
            url = textblob.public_url
        
            if isinstance(url, six.binary_type):
                url = url.decode('utf-8')
            urls.append(url)

            print(url)

            if "annotation" in basename:
                annotations_url = url
                afname = bucket_fn
            elif "coords" in basename:
                download_url = url
                cfname = bucket_fn
           

    val = root.get()[dataset_key]

    val.update(dict(
        annotations_url = annotations_url,
        downloadUrl= download_url,
        filename= cfname,
    ))
    val["allfiles"].update({
            "coords":cfname,
            "annotations":afname,
    })

    root.update({dataset_key:val})



def _create_tmpfiles(folder, dataset):
    #TEMP DIRECTORY SETUP
    nm = dataset["dataset"]
    tmpdir = os.path.join(tmp_root,nm)
    files = [fn for fn in os.listdir(folder) if nm in fn]
    if not os.path.isdir(tmpdir):
        os.makedirs(tmpdir)

    #clear the temp directory:
    for f in os.listdir(tmpdir):
        os.remove(os.path.join(tmpdir,f))
        print (f)

    print (tmpdir)
    #raise Exception()
    for fn in files:
        cur_loc = os.path.join(folder,fn)
        new_loc = os.path.join(tmpdir,fn)
        print (cur_loc, new_loc)
        if  not "xumi" in cur_loc:continue
        copyfile(cur_loc,new_loc)
    return tmpdir


    
def process_dataset(gcs_folder, dataset, dataset_key):
    status = dataset["server_process_status"]
    # if processing has not yet started, begin

    print(f"processing job, {dataset_key}")

    #enumerate job names
    jobs = {
        INIT_FRONTEND:"INIT_FRONTEND",
        INIT_BLAT:"INIT_BLAT",
        INIT_TOPHAT:"INIT_TOPHAT",        
    }
    #enumerate job statuses
    status = {
        WAITING:"WAITING",
        RUNNING:"RUNNING",
        COMPLETE:"COMPLETE",
        FAILED:"FAILED",
    }
    
    #initialize job handling for this dataset
    val = root.get()[dataset_key]
    if not server_jobs_status in val:
        val.update(dict(
            server_job_progresses= dict([(k,0) for k in jobs.values()]),
            server_job_statuses = dict([(k,"WAITING") for k in jobs.values()])
        ))
    tmpdir = _create_tmpfiles(gcs_folder, dataset)
    val.update(dict(server_process_status = "RUNNING"))
    root.update({dataset_key:val})


    #process input xumi data to coords / annotations formats for the frontend
    if get_jobstatus(dskey,jobs["INIT_FRONTEND"]) == status["WAITING"]:
        set_jobstatus(dataset_key, jobs["INIT_FRONTEND"], status["RUNNING"])
        files_for_upload = process_frontend(tmpdir,dataset)
        upload_frontend_files(files_for_upload,dataset,dataset_key)
        set_jobstatus(dataset_key, jobs["INIT_FRONTEND"], status["COMPLETE"])

    #initialize a blat database for alignments within this dataset
    if get_jobstatus(dskey,jobs["INIT_BLAT"]) == status["WAITING"]:
        set_jobstatus(dataset_key, jobs["INIT_BLAT"], status["RUNNING"])
        output_status = init_blat(folder,dataset)
        
        if output_status == 0:
            set_jobstatus(dataset_key, jobs["INIT_BLAT"], status["COMPLETE"])
        else: raise Exception(f"nonzero output status for {jobs['INIT_BLAT']}"

    #align all reads in this dataset to the reference genome and create GO lookups
    if get_jobstatus(dataset_key, jobs["INIT_TOPHAT"]) == "WAITING":
        set_jobstatus(dataset_key, jobs["INIT_TOPHAT"], status["RUNNING"])
        output_status = init_tophat(folder,dataset)
                              
        if output_status == 0:
            set_jobstatus(dataset_key, jobs["INIT_TOPHAT"], status["COMPLETE"])
        else: raise Exception(f"nonzero output status for {jobs['INIT_TOPHAT']}"
          
    return

def get_jobstatus(dskey, name):
    val = root.get()[dskey]
    status = val["server_job_statuses"][name]
    return status

def set_jobstatus(dskey, name,values):
    val = root.get()[dskey]
    val["server_job_statuses"][name] = value
    
                                   
def loop_queue(): 
    #create a list of all datasets in firebase
    datasets = root.get()
    users = set([v["userId"] for k,v in list( datasets.items())])

    #for each user, check for incomplete jobs
    for u in users:
        fpath = os.path.join(DATAROOT,"", u)
        userfiles = os.listdir(fpath)
        dsre = re.compile("[^_](\d+)$")

        filtered_sorted = sorted([u for u in userfiles if dsre.search(u)],key= lambda x: dsre.search(x).group())
        
        for dataset, g in it.groupby(filtered_sorted,lambda x:dsre.search(x).group()):
            grps = list(g)
            matched = [ (k, d) for k,d in datasets.items() if d["dataset"] ==dataset]
            if len(matched)==1:\
                k,d = matched[0]
               if d["server_process_status"] =="COMPLETE": continue
               process_dataset(fpath, d, k)
            else:
                print (f"no matching record found for dataset files {dataset}")
    time.sleep(1)
    
def main():
    print("starting up gcsfuse job watcher")
    while 1: loop_queue()
  
            
if __name__ =="__main__":
    main()





