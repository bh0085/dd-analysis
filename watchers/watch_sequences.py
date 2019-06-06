#!/usr/bin/env python


import os, re, time, requests, sys, six
import itertools as it

from init_blat import init_blat
from init_tophat_transcripts import init_tophat_transcripts
from init_go_terms import init_go_terms
from init_coords_and_colors import init_color_buffers, init_xy_buffers
from init_frontend import init_frontend
from init_dataset_database import init_dataset_database

from shutil import copyfile
from init_database_files import init_database_files
from watchers_firebase import root, storage


DATAROOT = "/slides/all_v2/website_datasets"
NAME=__file__[:-3]
tmp_root = os.path.join(f"/data/tmp/{NAME}/")
if not os.path.isdir(tmp_root): os.makedirs(tmp_root)
RESTART_FAILED_JOBS = True


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

    
def process_dataset(gcs_folder, dataset, dataset_key,**kwargs):
    force_resets_step = kwargs.get("force_resets_step",[])
    force_resets_dataset = kwargs.get("force_resets_dataset",[])
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
        "INIT_DATABASE_FILES":"INIT_DATABASE_FILES",
        "INIT_DATASET_DATABASE":"INIT_DATASET_DATABASE",
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
        "INIT_DATABASE_FILES":init_database_files,
        "INIT_DATASET_DATABASE":init_dataset_database,

    }



    
    #initialize job handling for this dataset
    val = root.get()[dataset_key]
    if not "server_job_statuses" in val:
        val.update(dict( server_job_statuses = {}))
        val.update(dict( server_job_progresses= {}))
    
    val["server_process_status"] = status["RUNNING"]
    root.update({dataset_key:val})
    
    for k,v in jobs.items():
        if dataset["dataset"] in force_resets_dataset:
            print("resetting", v, f"for {dataset['dataset']}")
            val["server_job_statuses"][v] = "WAITING"
            continue

        #reset this step if forced
        if v in force_resets_step:
            val["server_job_statuses"][v] = "WAITING"
            continue

        #check if the job is incomplete
        if not val["server_job_statuses"].get(v,None) == status["COMPLETE"]:
            #if so, then if it has not failed, restart. Otherwise, test if we
            #should restart failed jobs
            if RESTART_FAILED_JOBS or (
                    val["server_job_statuses"].get(v,None) != status["FAILED"]):
                val["server_job_statuses"][v] = status["WAITING"]
                val["server_job_progresses"][v] = 0


    root.update({dataset_key:val})
    val = None

    tmpdir = _create_tmpfiles(gcs_folder, dataset,reset_tmpfiles = reset_tmpfiles)
    dskey = dataset_key
    
    #TODO: REPLACE ALL JOB QUEUEING WITH THIS LOOP
    for jobkey in [
        "INIT_FRONTEND",
        "INIT_BLAT",
        "INIT_TOPHAT_TRANSCRIPTS", 
        "INIT_GO_TERMS", 
        "INIT_XY_BUFFERS", 
        "INIT_COLOR_BUFFERS",
        "INIT_DATABASE_FILES",
        "INIT_DATASET_DATABASE"]:

        #pass additional args (such as the storage key)
        #if a job requires it
        kw = {
            "key":dskey
        } if jobkey == "INIT_FRONTEND" else {}

        jobname = jobs[jobkey]
        if get_jobstatus(dataset_key, jobname) == "WAITING":
            set_jobstatus(dataset_key, jobname, status["RUNNING"])
            try: output_status = jobfuns[jobname](tmpdir,dataset, **kw)
            except Exception as e:
                set_jobstatus(dataset_key,jobname,"FAILED")
                raise(e)
            if output_status == 0:
                set_jobstatus(dataset_key,jobname, status["COMPLETE"])
            else: raise Exception(f"nonzero output status for {jobname}")
    
           
    val = root.get()[dataset_key]
    for k,v in val["server_job_statuses"].items():
        if v =="RUNNING": val["server_job_statuses"][k]="WAITING"

    
    val["server_process_status"] = status["COMPLETE"]
    root.update({dataset_key:val})
    
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
    #list all datasets in firebase
    datasets = root.get()
    #create a list of users from all datasets
    users = set([v["userId"] for k,v in list( datasets.items())])

    for u in users:
        #for each user, look at the list of all uploaded files, searching for unique dataset ids
        fpath = os.path.join(DATAROOT,"", u)
        userfiles = os.listdir(fpath)
        dsre = re.compile("[^_](\d+)$")
        
        #identify all files which can be matched to a dataset
        filtered_sorted = sorted([u for u in userfiles if dsre.search(u)],key= lambda x: dsre.search(x).group())
        #cycle through dataset file groups
        for dataset, g in it.groupby(filtered_sorted,lambda x:dsre.search(x).group()):
            #check firebase for a database record associated with the uploaded files
            matched = [ (k, d) for k,d in datasets.items() if d["dataset"] ==dataset]
            if len(matched)==1:
                k,d = matched[0]
                #check job status on the server
                if d["server_process_status"] =="COMPLETE": continue
                #process the upload if necessary
                process_dataset(fpath, d, k, **kwargs)
            else:
                print (f"no matching firebase entry found for dataset files {dataset}")
    time.sleep(1)
    
def main():
    import argparse
    parser = argparse.ArgumentParser(description='Watch for webserver file uploads and process datasets, finding genome alignments and functional annotations.')
    parser.add_argument('--reset-dataset', dest="rd", nargs='?')
    parser.add_argument('--reset-step', dest="rs", nargs='?')
    parser.add_argument('--noloop', dest='noloop', action='store_const',
                        const=True, default=False,
                        help='Keep looping to wait for new sequences')
    parser.add_argument('--reset-tmpfiles', dest='reset_tmpfiles', action='store_const',
                        const=True, default=False,
                        help='Recreate tmpfiles')

    
    args = parser.parse_args()
    force_resets_step = []
    if args.rs:
        force_resets_step = [args.rs]

    force_resets_dataset = []
    if args.rd:
        force_resets_dataset = [args.rd]
        
    while 1:
        loop_queue(force_resets_step = force_resets_step,
            force_resets_dataset = force_resets_dataset,
                   reset_tmpfiles = args.reset_tmpfiles)
        
        if args.noloop: break
    exit


    

    
if __name__ =="__main__":
    main()





