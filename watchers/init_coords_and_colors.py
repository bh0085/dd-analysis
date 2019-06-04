#!/usr/bin/env python
import os, random
import numpy as np
import pandas as pd
from xumi_files import annotation_cols

#takes as input, SMLE files, processes them to create gzipped annotation files
#in a temporary director, a dictionary with file names and paths for each file
def init_xy_buffers(inp_folder, dataset, **kwargs):

    dsname = dataset["dataset"]
    xumi_base = "xumi_base_"+dsname
    xumi_basepath = os.path.join(inp_folder,xumi_base)
    coords_df = pd.read_csv(xumi_basepath, names =["id","x","y"])
    x_array = np.array(coords_df.x.values,dtype =np.float32)
    y_array = np.array(coords_df.y.values,dtype =np.float32)
    z_array = y_array*0+.5
    
    OUTDIR_XY =os.path.join( f"/data/dd-frontend/datasets/{dsname}/buffers/xy/")
    if not os.path.isdir(OUTDIR_XY): os.makedirs(OUTDIR_XY)
    with open(os.path.join(OUTDIR_XY,"x.buffer"),"wb") as fopen:
        fopen.write(x_array.tobytes())
    with open(os.path.join(OUTDIR_XY,"y.buffer"),"wb") as fopen:
        fopen.write(y_array.tobytes())
    with open(os.path.join(OUTDIR_XY,"z.buffer"),"wb") as fopen:
        fopen.write(z_array.tobytes())
 
        
    return 0


def init_color_buffers(inp_folder, dataset, **kwargs):

    dsname = dataset["dataset"]
    featfn = os.path.join(inp_folder, "xumi_feat_"+dsname)

    print(featfn)
    annotations_df = pd.read_csv(featfn,names=[a[0] for a in annotation_cols])
    red = annotations_df["type"].apply(lambda x: 255)
    green = annotations_df["type"].apply(lambda x: 255)
    blue =annotations_df["type"].apply(lambda x: 255)
    alpha = annotations_df["type"].apply(lambda x: .25 if x =="-1" else 1)


    OUTDIR_COLORS =os.path.join( f"/data/dd-frontend/datasets/{dsname}/buffers/colors/")
    if not os.path.isdir(OUTDIR_COLORS): os.makedirs(OUTDIR_COLORS)
    
    for nm,df in [("red",red),("green",green),("blue",blue),("alpha",alpha)]:
        with open( os.path.join(OUTDIR_COLORS,f"{nm}.buffer"),"wb") as fopen:
            fopen.write(np.array(df.values,dtype =np.float32).tobytes())
            
    
    #default_types = {"-1":{
    #    "color":[255,255,255,.25],
    #    "size":1, #ACTB
    #    "name":"ACTB",
    #
    #},"0":{
    #    "color":[255,255,255,1],
    #    "size":1, #GAPDH --opaque white
    #    "name":"GAPDH",
    #},"1":{
    #    "color":[0,255,0,.7],
    #    "size":1, #GFP
    #    "name":"GFP",
    #},"2":{
    #    "color":[255,0,0,.7],
    #    "size":1, #RFP
    #    "name":"RFP"
    #}};

    

    return 0
