
from xumi_files import annotation_cols
import os
import subprocess as spc
import pandas as pd

DATA_DIR = "/data/dd-analysis"


def init_tophat_transcripts(folder, dataset):
    #try:
    _align_reads(folder,dataset)
        #_create_gene_and_functional_lookups(folder,dataset)
    #except Exception as e:
    #    print( e)
    #    print( "TOPHAT NOT YET IMPLEMENTED, LEAVING IN RUN STATUS")
    #    return -1


#use tophat to align spliced reads to the genome
def _align_reads(tmpfolder,dataset):



    GENOME_BWT="/data/transcriptomes/Homo_sapiens.GRCh38.cdna.all"
    
    nm = dataset["dataset"]
    featfn = os.path.join(tmpfolder, "xumi_feat_"+nm)
    annotations = pd.read_csv(featfn, names = [a[0] for a in annotation_cols]) # parse annotation_file( featfn)
    sequences = annotations.seq


    database_data_dir  = os.path.join(DATA_DIR,"datasets",dataset["dataset"])
    tophat_dir = os.path.join(database_data_dir,"tophat")
    if not os.path.isdir(tophat_dir):
        os.makedirs(tophat_dir)

    sfn = os.path.join(tophat_dir, "raw_sequences.fa")
    
    with open(sfn,"w") as seqsfile:
        seqsfile.write("".join([">{}_{}\n{}\n".format(dataset["dataset"],idx,x)
                                      for idx,x in sequences.iteritems() if len(x) > 20]))
    
    OUTDIR=os.path.join( f"/data/dd-analysis/datasets/{dataset['dataset'] }/tophat/")    
    if not os.path.isdir(OUTDIR): os.makedirs(OUTDIR)
    
    cmds = ["tophat" , "--output-dir" ,OUTDIR,  GENOME_BWT, sfn]
    print(cmds)
    out = spc.call(cmds)
    


#pandas database of gene names and GO annotations
def _create_gene_and_functional_lookups(folder,dataset):
    _align_reads_to_genome(folder,dataset)
    
    raise Exception("TODO")
