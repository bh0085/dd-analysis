#!/usr/bin/env python
from watchers_firebase import root, storage
import os, random, math, json, gzip, re, time, requests, sys, six
from xumi_files import parse_coords_file, parse_annotation_file, parse_segmentation_file, annotation_cols, seg_cols

import pandas as pd
import numpy as np
if not os.path.isdir("/data/tmp"):
    os.makedirs("/data/tmp")



def make_pp_files(tmpdir, dataset):
    nm = dataset["dataset"]
    out_folder = os.path.join(f"/data/tmp/{nm}")
    if not os.path.isdir(out_folder):
        os.makedirs(out_folder)


    import gzip


    # db_string = "postgres://ben_coolship_io:password@localhost/dd"
    # db2 = create_engine(db_string)  
    # Session = sessionmaker(db2)  
    # session = Session()

    from dataset_models import Umi,  Dataset, GeneGoTerm, UmiGeneId, GoTerm, NcbiGene, Segment
    from db import session

    rsq = pd.read_sql_query
    sq = session.query
    getDatasetId =  lambda dsname: int(dsname[:8])

    #WRITE UMI METADATA TO A FILE
    umi_ids_path = os.path.join(out_folder,"umi_ids.json.gz")
    umis_df = rsq(sq(Umi.idx,Umi.umap_x,Umi.umap_y,Umi.umap_z,Umi.id,Umi.seg).filter(Umi.dsid==getDatasetId(nm)).statement,session.connection())
    print("umis")
    print(umis_df.iloc[:10])
    with gzip.open(umi_ids_path,"w") as f:
        f.write(
            bytes(
                umis_df.to_json(orient="columns"),'utf-8'
        ))


    #WRITE IMAGE SEGMENTATION INFO TO A FILE
    winning_segments_grid_path = os.path.join(out_folder,"winning_segments_grid.json.gz")
    passing_segments_grid_path = os.path.join(out_folder,"passing_segments_grid.json.gz")

    segmentations_folder = os.path.join(
        f"/data/dd-analysis/datasets/{nm}/segmentations/")

    winning_path = os.path.join(segmentations_folder,"winning_pixels.csv")
    passing_path = os.path.join(segmentations_folder,"passing_pixels.csv")

    print("DANGER! POST PROCESSING FRONTEND. UNCOMMENT THIS SECTION OF THE FILE WHEN RUNNING ON NEW DATASETS")
    winning_segments_df = pd.read_csv(winning_path, index_col=["x","y"])
    winning_nested = winning_segments_df.reset_index("y").groupby("x").apply(lambda x:  x.set_index("y").to_dict()['segment']).to_dict()
      
    passing_segments_df = pd.read_csv(passing_path, index_col=["x","y","segment"])
    passing_nested = passing_segments_df.reset_index(["y","segment"]).groupby("x").apply(lambda x:  x.groupby("y").apply(lambda g: [int(e) for e in g.segment.unique()]).to_dict()).to_dict()
    
    with gzip.open(winning_segments_grid_path,"w") as f: f.write( bytes(json.dumps(winning_nested),'utf-8'))
    with gzip.open(passing_segments_grid_path,"w") as f: f.write( bytes(json.dumps(passing_nested),'utf-8'))
        
    #WRITE SEGMENT METADATA TO A FILE
    segment_metadata_path = os.path.join(out_folder,"segment_metadata.json.gz")
    with gzip.open(segment_metadata_path,"w") as f:

        f.write(
            bytes(
                rsq(sq(Segment).filter(Segment.dsid==getDatasetId(nm)).statement,session.connection()).set_index("id").to_json(orient="index"),'utf-8'
        ))

    return {
        "umi_ids":umi_ids_path,
        "winning_segments_grid":winning_segments_grid_path,
        "passing_segments_grid":passing_segments_grid_path,
        "segment_metadata":segment_metadata_path,
    }

def init_postprocessing_frontend(tmpdir, dataset, key= None):
    if not key: raise Exception()
    upload_gzipped_files(make_pp_files(tmpdir,dataset),dataset,key)
    return 0

def upload_gzipped_files(files,dataset, dataset_key):
    bucket = storage.bucket()
    file_urls = {}
    print("uploading gzipped files")
    print(files)

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
            file_urls[nm+"_url"] = url
            print(url)
           
    val = root.get()[dataset_key]
    print(val)
    val.update(file_urls)
    root.update({dataset_key:val})

