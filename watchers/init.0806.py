#!/usr/bin/env python
import os, random, math, json, gzip, csv


inp_root = "/slides/josh/0806/"
out_root = "/slides/0806/"

coords_inp_pattern = "final_Xumi_smle_data_minuei2dim2DMv1.0_.csv"
annotations_inp_pattern = "final_feat_Xumi_smle_data_minuei2dim2DMv1.0_.csv"
segmentation_inp_pattern= "final_Xumi_segment_0.2_data_minuei2dim2DMv1.0_.csv"

samples =["{0}".format(e) for e in [388,786,190,236,332,34]]

#read each of the samples into fo
for s in samples:
    
    inp_folder = os.path.join(inp_root, "i{0}_smle".format(s))
    out_folder = os.path.join(out_root, s)
    if not os.path.isdir(out_folder):os.makedirs(out_folder)    
    
    #convert the input coordinate csv file into a rounded array for display purposes
    coord_cols = [["id", id],
                  ["x",lambda x: round(float(x),4)],
                  ["y",lambda y: round(float(y),4)]]


    annotation_cols = [["id", int],
                       ["type",int],
                       ["ar",int],
                       ["tr",int],
                       ["seq",str]]
    seg_cols =  [["id", int],
                 ["seg",lambda x: int(float(x))],
                 ["unk1",lambda x:None],
                 ["unk2",lambda x:None],
                 ["unk3",lambda x:None],
                 ["unk4",lambda x:None],
                 ["unk5",lambda x:None]] #usually only contains 3 unk elements. 388 has extra

    print os.path.join(inp_folder,segmentation_inp_pattern)
    coord_data = [ [coord_cols[i][1](e) for i,e in enumerate(l) ]
                   for l in csv.reader(open(os.path.join(inp_folder,coords_inp_pattern)))]

    annotation_data =  [ [annotation_cols[i][1](e) for i,e in enumerate(l) ]
                   for l in csv.reader(open(os.path.join(inp_folder,annotations_inp_pattern)))]
    
    seg_data =  [ [seg_cols[i][1](e) for i,e in enumerate(l) ]
                   for l in csv.reader(open(os.path.join(inp_folder,segmentation_inp_pattern)))]


    #produce a small gzip output file having x,y,t for each point
    cfpath = os.path.join(out_folder,"coords.json.gz")
    with gzip.open(cfpath,"w") as f:
        f.write(json.dumps(
            [[annotation_data[i][1]]+coord_data[i][1:]
             for i in range(len(coord_data))]
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

    print [e[0] for e in ( annotation_cols + seg_cols[1:2])]
        
    #produce a larger, gzipped output file with all annotation data
    afpath = os.path.join(out_folder,"annotations.json.gz")
    with gzip.open(afpath,"w") as f:
        f.write(json.dumps(
            {"features":[annotation_data[i][:]+[seg_data[i][1]] for i in range(len(coord_data))],
             "types":types,
             "feature_cols":[e[0] for e in ( annotation_cols + seg_cols[1:2])]}
        ))

    
