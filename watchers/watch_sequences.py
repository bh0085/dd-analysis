#!/usr/bin/env python
import firebase_admin

from firebase_admin import credentials
from firebase_admin import storage

import six
import os, re, time, requests, sys
import itertools as it

from process_frontend import make_files

from init_blat import init_blat
from init_tophat_transcripts import init_tophat_transcripts
from init_go_terms import init_go_terms
from init_coords_and_colors import init_color_buffers, init_xy_buffers

from shutil import copyfile



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

RESTART_FAILED_JOBS = True


def process_frontend(folder, dataset):
    #get transformed files
    files_for_upload = make_files(folder, dataset)
    return files_for_upload

def process_coords(folder,dataset):
    
    return -1

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



def _create_tmpfiles(folder, dataset,reset_tmpfiles):
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

    #raise Exception()
    for fn in files:
        cur_loc = os.path.join(folder,fn)
        new_loc = os.path.join(tmpdir,fn)
        print (cur_loc, new_loc)
        if  not "xumi" in cur_loc:continue
        if reset_tmpfiles or (not os.path.isfile(new_loc)):
            copyfile(cur_loc,new_loc)
    return tmpdir

def init_frontend(tmpdir, dataset, key = None):
    if not key: raise Exception()
    files_for_upload = process_frontend(tmpdir,dataset)
    upload_frontend_files(files_for_upload,dataset,key)
    return 0
    
def process_dataset(gcs_folder, dataset, dataset_key,**kwargs):
    force_resets = kwargs.get("force_resets",[])
    reset_tmpfiles = kwargs.get("reset_tmpfiles")

    status = dataset["server_process_status"]
    # if processing has not yet started, begin

    print(f"processing job, {dataset_key}")

    #enumerate job names
    jobs = {
        "INIT_FRONTEND":"INIT_FRONTEND",
        "INIT_BLAT":"INIT_BLAT",
        "INIT_TOPHAT_TRANSCRIPTS":"INIT_TOPHAT_TRANSCRIPTS", #csv having umis--> tx ids
        "INIT_GO_TERMS":"INIT_GO_TERMS",
        "INIT_XY_BUFFERS":"INIT_XY_BUFFERS",
        "INIT_COLOR_BUFFERS":"INIT_COLOR_BUFFERS",
    }
    #enumerate job statuses
    status = {
        "WAITING":"WAITING",
        "RUNNING":"RUNNING",
        "COMPLETE":"COMPLETE",
        "FAILED":"FAILED",
    }

    jobfuns = {
        "INIT_FRONTEND":init_frontend,
        "INIT_TOPHAT_TRANSCRIPTS":init_tophat_transcripts,
        "INIT_GO_TERMS":init_go_terms,
        "INIT_BLAT":init_blat,
        "INIT_XY_BUFFERS":init_xy_buffers,
        "INIT_COLOR_BUFFERS":init_color_buffers,
    }
    
    #initialize job handling for this dataset
    val = root.get()[dataset_key]
    if not "server_job_statuses" in val:
        val.update(dict( server_job_statuses = {}))
        val.update(dict( server_job_progresses= {}))
        
    for k,v in jobs.items():
        if not val["server_job_statuses"].get(v,None) == status["COMPLETE"]:
            if RESTART_FAILED_JOBS or (
                    val["server_job_statuses"].get(v,None) != status["FAILED"]):
                val["server_job_statuses"][v] = status["WAITING"]
                val["server_job_progresses"][v] = 0
        if v in force_resets:
            print (force_resets)
            print (v)
            val["server_job_statuses"][v] = "WAITING"



    #val["server_job_statuses"]["INIT_XY_BUFFERS"] = "WAITING"            
    root.update({dataset_key:val})
    val = None

    tmpdir = _create_tmpfiles(gcs_folder, dataset,reset_tmpfiles = reset_tmpfiles)
    dskey = dataset_key
    
    #process input xumi data to coords / annotations formats for the frontend
    jobname=jobs["INIT_FRONTEND"]
    if get_jobstatus(dskey,jobs["INIT_FRONTEND"]) == status["WAITING"]:
        set_jobstatus(dataset_key, jobs["INIT_FRONTEND"], status["RUNNING"])
        jobfuns[jobname](tmpdir,dataset,key=dataset_key)
        set_jobstatus(dataset_key, jobs["INIT_FRONTEND"], status["COMPLETE"])

    #initialize a blat database for alignments within this dataset
    if get_jobstatus(dskey,jobs["INIT_BLAT"]) == status["WAITING"]:
        set_jobstatus(dataset_key, jobs["INIT_BLAT"], status["RUNNING"])
        output_status = init_blat(tmpdir,dataset)
        
        if output_status == 0:
            set_jobstatus(dataset_key, jobs["INIT_BLAT"], status["COMPLETE"])
        else: raise Exception(f"nonzero output status for {jobs['INIT_BLAT']}")

    #align all reads in this dataset to the reference genome and create GO lookups
    if get_jobstatus(dataset_key, jobs["INIT_TOPHAT_TRANSCRIPTS"]) == "WAITING":
        set_jobstatus(dataset_key, jobs["INIT_TOPHAT_TRANSCRIPTS"], status["RUNNING"])
        output_status = init_tophat_transcripts(tmpdir,dataset)
                              
        if output_status == 0:
            set_jobstatus(dataset_key, jobs["INIT_TOPHAT_TRANSCRIPTS"], status["COMPLETE"])
        else: raise Exception(f"nonzero output status for {jobs['INIT_TOPHAT_TRANSCRIPTS']}")

    jobname = jobs["INIT_GO_TERMS"]
    #align all reads in this dataset to the reference genome and create GO lookups
    if get_jobstatus(dataset_key, jobname) == "WAITING":
        set_jobstatus(dataset_key, jobname, status["RUNNING"])
        try: output_status = init_go_terms(tmpdir,dataset)
        except Exception as e:
            set_jobstatus(dataset_key,jobname,"FAILED")
            raise(e)
        if output_status == 0:
            set_jobstatus(dataset_key,jobname, status["COMPLETE"])
        else: raise Exception(f"nonzero output status for {jobname}")

    #TODO: REPLACE ALL JOB QUEUEING WITH THIS LOOP
    for jobkey in ["INIT_XY_BUFFERS", "INIT_COLOR_BUFFERS"]:
        jobname = jobs[jobkey]
        if get_jobstatus(dataset_key, jobname) == "WAITING":
            set_jobstatus(dataset_key, jobname, status["RUNNING"])
            try: output_status = jobfuns[jobname](tmpdir,dataset)
            except Exception as e:
                set_jobstatus(dataset_key,jobname,"FAILED")
                raise(e)
            if output_status == 0:
                set_jobstatus(dataset_key,jobname, status["COMPLETE"])
            else: raise Exception(f"nonzero output status for {jobname}")
    
           
        

    val = root.get()[dataset_key]
    #no jobs are still running. RESET
    for k,v in val["server_job_statuses"].items():
        if v =="RUNNING": val["server_job_statuses"][k]="WAITING"
    root.update({dskey:val})
    
    return

def get_jobstatus(dskey, name):
    val = root.get()[dskey]
    status = val["server_job_statuses"][name]
    return status

def set_jobstatus(dskey, name,value):
    val = root.get()[dskey]
    val["server_job_statuses"][name] = value
    root.update({dskey:val})
        
    
                                   
def loop_queue(**kwargs): 
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
            if len(matched)==1:
                k,d = matched[0]
                if d["server_process_status"] =="COMPLETE": continue
                process_dataset(fpath, d, k, **kwargs)
            else:
                print (f"no matching record found for dataset files {dataset}")
    time.sleep(1)
    
def main():
    import argparse

    parser = argparse.ArgumentParser(description='Watch for webserver file uploads and process datasets, finding genome alignments and functional annotations.')
    
    #parser.add_argument('integers', metavar='N', type=int, nargs='?',
    #                help='an integer for the accumulator')
    parser.add_argument('--reset-one', dest="r", nargs='?')
    
    parser.add_argument('--noloop', dest='noloop', action='store_const',
                        const=True, default=False,
                        help='Keep looping to wait for new sequences')

    parser.add_argument('--reset-tmpfiles', dest='reset_tmpfiles', action='store_const',
                        const=True, default=False,
                        help='Recreate tmpfiles')

    
    args = parser.parse_args()

    force_resets = []
    if args.r:
        force_resets = [args.r]
        
    while 1:
        loop_queue(force_resets = force_resets,
                   reset_tmpfiles = args.reset_tmpfiles)
        
        if args.noloop: break
    exit


    

    
if __name__ =="__main__":
    main()





