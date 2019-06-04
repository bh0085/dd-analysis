#!/usr/bin/env python
import os, random
import numpy as np
import pandas as pd
    
#takes as input, SMLE files, processes them to create gzipped annotation files
#in a temporary director, a dictionary with file names and paths for each file
def init_xy_buffers(inp_folder, dataset):


    dsname = dataset["dataset"]
    xumi_base = "xumi_base_"+dataset
    xumi_basepath = os.path.join(inp_folder,xumi_base)
    coords_df = pd.read_csv(xumi_basepath, names =["id","x","y"])
    x_array = np.array(coords_df.x.values,dtype =np.float32)
    y_array = np.array(coords_df.y.values,dtype =np.float32)

    
    OUTDIR_XY =os.path.join( f"/data/dd-frontend/datasets/{dataset['dataset'] }/buffers/xy/")
    if not os.path.isdir(xy): os.makedirs(OUTDIR_XY)
    with open(os.path.join(OUTDIR_XY,"x.buffer"),"wb") as fopen:
        fopen.write(x_array.to_bytes())

    with open(os.path.join(OUTDIR_XY,"y.buffer"),"wb") as fopen:
        fopen.write(y_array.to_bytes())
    
    
    return 0
