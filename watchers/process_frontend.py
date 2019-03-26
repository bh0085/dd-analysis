#!/usr/bin/env python
import os, random, math, json, gzip
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
