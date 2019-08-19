#!/usr/bin/env python
from watchers_firebase import root, storage
import os, random, math, json, gzip, re, time, requests, sys, six
from xumi_files import parse_coords_file, parse_annotation_file, parse_segmentation_file, annotation_cols, seg_cols

if not os.path.isdir("/data/tmp"):
    os.makedirs("/data/tmp")


#takes as input, SMLE files, processes them to create gzipped annotation files
#in a temporary director, a dictionary with file names and paths for each file
def make_files(inp_folder, dataset):

    nm = dataset["dataset"]
    segfn = os.path.join(inp_folder, "xumi_segment_base_"+nm)
    featfn = os.path.join(inp_folder, "xumi_feat_"+nm)
    basefn = os.path.join(inp_folder, "xumi_base_"+nm)

    out_folder = os.path.join(f"/data/tmp/{nm}")
    if not os.path.isdir(out_folder):
        os.makedirs(out_folder)
    
    coord_data= parse_coords_file(basefn)
    annotation_data = parse_annotation_file(featfn)
    seg_data = parse_annotation_file(segfn)
    
    #produce a small gzip output file having x,y,t for each point
    cfpath = os.path.join(out_folder,"coords.json.gz")
    with gzip.open(cfpath,"w") as f:
        f.write(
            bytes(json.dumps(
            [[annotation_data[i][1]]+coord_data[i][1:]
             for i in range(len(coord_data))]),'utf-8'
        ))
              
    types = {"-1":{
        "color":[255,255,255,.5],
        "size":1,
        "z":.75,
    },"0":{
        "color":[0,0,255,1],
        "size":1,
        "z":.5,
    },"1":{
        "color":[255,0,0,1],
        "size":1,
        "z":.5,
    },"2":{
        "color":[0,255,0,1],
        "size":1,
        "z":.5,
    }}

        
    #produce a larger, gzipped output file with all annotation data
    afpath = os.path.join(out_folder,"annotations.json.gz")
    with gzip.open(afpath,"w") as f:
        f.write(bytes(json.dumps(
            {"features":[annotation_data[i][:]+[seg_data[i][1]] for i in range(len(coord_data))],
             "types":types,
             "feature_cols":[e[0] for e in ( annotation_cols + seg_cols[1:2])]}
        ),'utf-8'))

    
    return  {
        "coordinates": cfpath,
        "annotations": afpath,
    }


def init_frontend(tmpdir, dataset, key = None):
    if not key: raise Exception()
    files_for_upload =  make_files(folder, dataset)
    upload_frontend_files(files_for_upload,dataset,key)
    return 0



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
