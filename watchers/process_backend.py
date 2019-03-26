#!/usr/bin/env python
'''
Scripts to take xumi files uploaded from the web frontend to google cloud storage
and process each one to create annotations which will be queried from the GUI.
'''

import os
import pandas as pd
from xumi_files import annotation_cols
import subprocess as spc

DATA_DIR = "/data/dd-analysis"

#helper script, uses tophat to align spliced reads to the genome
def _align_reads_to_genome(folder,dataset):
    raise Exception("TODO")



#blat server for rapid substring alignments to the data
def create_blat(tmpfolder,dataset):
    _write_blat_fa(tmpfolder,dataset)
    _write_master_blat_fa()
    


def _write_blat_fa(tmpfolder,dataset):
    nm = dataset["dataset"]
    featfn = os.path.join(tmpfolder, "xumi_feat_"+nm)
    annotations = pd.read_csv(featfn, names = [a[0] for a in annotation_cols]) # parse annotation_file( featfn)
    sequences = annotations.seq



    database_data_dir  = os.path.join(DATA_DIR,"datasets",dataset["dataset"])
    blat_dir = os.path.join(database_data_dir,"blat")
    if not os.path.isdir(blat_dir):
        os.makedirs(blat_dir)
    
    with open(os.path.join(blat_dir, "raw_sequences.fa"),"w") as seqsfile:
              seqsfile.write("".join([">{}\n{}\n".format(idx,x)
                                      for idx,x in sequences.iteritems() if x != "N"]))

def _write_master_blat_fa():
    master_fa = os.path.join("/data/dd-analysis/master_blat","master.fa")

    if not os.path.isdir("/data/dd-analysis/master_blat"):
        os.makedirs("/data/dd-analysis/master_blat")
        
    dataset_folders =  os.listdir(os.path.join(DATA_DIR, "datasets/"))

    alltext = ""
    for f in dataset_folders:
        with open(os.path.join(DATA_DIR,"datasets",f,"blat","raw_sequences.fa")) as fopen:
            alltext += fopen.read()
    
    with open(master_fa,"w") as fopen:
        fopen.write(alltext)
    cmds = ["faToTwoBit", master_fa, master_fa[:-2]+"2bit"]
    out = spc.call(cmds)

    relaunch_cmds = ["god","restart", "run-blat"]
    out = spc.call(relaunch_cmds)

    return {"status": complete}
    raise Exception("TODO")

#pandas database of gene names and GO annotations
def  create_gene_and_functional_lookups(folder,dataset):
    _align_reads_to_genome(folder,dataset)
    
    raise Exception("TODO")



def test_blat():
    annotation_cols = [["id", int],
                       ["type",int],
                       ["ar",int],
                       ["tr",int],
                       ["seq",str]]

    annotation_data =  [ [annotation_cols[i][1](e) for i,e in enumerate(l) ]
                   for l in csv.reader(open(featfn))]

if __name__ == "__main__":
    test_blat()
