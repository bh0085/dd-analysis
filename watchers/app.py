from flask import Flask
import subprocess as spc
import os, json
import pandas as pd
import numpy as np
app = Flask(__name__)

#from flask_cors import CORS
#CORS(app)
#api = Api(app)

import exports, queries, dataset_manager



@app.after_request
def after_request(response):
  response.headers.add('Access-Control-Allow-Origin', '*')
  response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
  response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
  return response


from flask import Flask,redirect    
from rq import Queue
from rq.job import Job
from worker_app import conn

#app.config.from_object(os.environ['APP_SETTINGS'])
#app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
#db = SQLAlchemy(app)

q = Queue(connection=conn)



TMPDIR = "/data/tmp"
BLAT_PORT = "8080"
BLAT_HOST = "localhost"
BLAT_TMPDIR = os.path.join(TMPDIR, "blat")
if not os.path.isdir(BLAT_TMPDIR):os.makedirs(BLAT_TMPDIR)

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route("/results/<job_key>", methods=['GET'])
def get_results(job_key):

    job = Job.fetch(job_key, connection=conn)

    if job.is_finished:
        return str(job.result), 200
    else:
        return "Nay!", 202
    

def sample_save_text(inp_txt):
    import random
    print ("SAVING TEXT!")
    tmpfolder = os.path.join(TMPDIR,"sampletext")
    if not os.path.isdir(tmpfolder):os.makedirs(tmpfolder)
    txt = str(random.randint(0,1000000))
    with open(os.path.join(tmpfolder,"randn.txt"),"w") as fopen:
        fopen.write(txt)
    return txt

@app.route("/worker/")
def start_worker():
    print ("STARTING WORKER, EH?")
    txt ="hello"
    job = q.enqueue_call(
        func=sample_save_text, args=(txt,), result_ttl=5
    )
    print("STARTING A JOB", job.get_id())
    print(job.get_id())
    return  redirect("results/"+str(job.get_id()),code = 307)




@app.route("/exports/umis/fasta/<dataset>/")
def export_fasta(dataset):
  output = exports.make_fasta_file(dataset)
  return output

@app.route("/queries/umis/geneids/<dataset>/<query>/")
def umigeneids(dataset,query):
  df = dataframes[dataset]["umis2geneids"]
  matches = df.loc[df.ncbi_gene == query].umi
  return json.dumps([[v,0] for v in  matches.values])

@app.route("/queries/umis/goterms/<dataset>/<query>/")
def umigoterms(dataset,query):
  df = dataframes[dataset]["go2umis"]
  matches = df.loc[df.go_name.str.contains(query)].umi
  return json.dumps(matches.values)

  
@app.route("/queries/cells/geneids/<dataset>/<query>/")
def cellgeneids(dataset,query):
  df = dataframes[dataset]["cells2geneids"]
  matches = df.loc[df.ncbi_gene.str.contains(query)].segment
  return json.dumps(matches.values)
  
@app.route("/queries/cells/goterms/<dataset>/<query>/")
def cellgoterms(dataset,query):
  df = dataframes[dataset]["go2cells"]


  
  
  seg_lookup = dataframes[dataset]["umi2seg"]
  seg_sizes = seg_lookup.seg.value_counts()
  
  matches = df.loc[df.go_name.str.contains(query)]
  print ("n total segment matches: {}".format(len(matches)))

  minsize= 0
  
  matches = matches.loc[matches.segment.isin(seg_sizes.loc[seg_sizes>=minsize].index)].dropna()
  print ("n segment matches with size >{}: {}".format(minsize,len(matches))) 

  matchcounts = matches.groupby("segment").apply(lambda x: x["count"].sum())
  
  normalized_counts = (matchcounts / seg_sizes).dropna()
  saturation_thresh = 90
         
  normalized_counts = normalized_counts / np.percentile(normalized_counts,saturation_thresh)
  normalized_counts.loc[normalized_counts >= 1] = 1
  umis_with_counts =seg_lookup.join(pd.Series(normalized_counts,name="enrichment"),on="seg")[["umi","enrichment"]].dropna()

  print( "total number of umis matched {}".format(len(umis_with_counts)))

  
  inp_folder = "/data/tmp/watch_sequences/"+dataset
  xumi_base = "xumi_base_"+dataset
  xumi_feat = "xumi_feat_"+dataset
  xumi_basepath = os.path.join(inp_folder,xumi_base)
  xumi_featpath = os.path.join(inp_folder,xumi_feat)
  
  
  #map from ids to frontend indexes
  feats_df = pd.read_csv(xumi_featpath,names =["id","type","ar","tr","seq"])
  coords_df = pd.read_csv(xumi_basepath, names =["id","x","y"])
  coords_df = coords_df.loc[feats_df.type!=-1]
  coords_df["idx"] = coords_df.index
  coords_df = coords_df.set_index(coords_df.id)
  umi_ids_to_idxs = umis_with_counts.join(coords_df, on = "umi").sort_values("enrichment")

  
  
  return json.dumps([list(e) for e in  umi_ids_to_idxs[["idx","enrichment"]].values])

@app.route("/queries/umis/alignments/<dataset>/<query>/")
def umialignments(dataset,query):
  return []

@app.route("/queries/umis/spliced/<dataset>/<query>/")
def umispliced(dataset,query):
  return []

@app.route("/queries/umis/sequence/<dataset>/<query>/")
def umis_by_sequence(dataset,query):
  
    queryname = query
    queryfn = "{}.fa".format(queryname)
    querypath = os.path.join(BLAT_TMPDIR,queryfn)
    with open(querypath, "w") as qf:
        qf.write(f">{query}\n{query}\n")

    outfn = "{}.psf".format(queryname)
    outpath = os.path.join(BLAT_TMPDIR,outfn)
    server_dir = "/data/dd-analysis/master_blat/"
    
    cmds = ["gfClient",BLAT_HOST,BLAT_PORT,server_dir,querypath,outpath]
    cmd = " ".join(cmds)
    out = spc.call(cmds)
    results = parse_psl(os.path.join(BLAT_TMPDIR, outfn))

    ds_umi_results = results["T name"].values
    umis = [int(u.split("_")[1]) for u in ds_umi_results if u.split("_")[0] == str(dataset)]
    return json.dumps(list(set(umis))) 


@app.route("/blat/<dataset>/<query>/")
def blat_sequence_to_dataset(dataset,query):
    
    queryname = query
    queryfn = "{}.fa".format(queryname)
    querypath = os.path.join(BLAT_TMPDIR,queryfn)
    with open(querypath, "w") as qf:
        qf.write(f">{query}\n{query}\n")

    outfn = "{}.psf".format(queryname)
    outpath = os.path.join(BLAT_TMPDIR,outfn)

    server_dir = "/data/dd-analysis/master_blat/"

    #gfClient localhost 8080 /data/dd-analysis/master_blat/ CTTCTCCAGCAGTGCGGGTCCTGGGCTCCTGAAGGCTTATT.fa CTTCTCCAGCAGTGCGGGTCCTGGGCTCCTGAAGGCTTATT.psf
    
    cmds = ["gfClient",BLAT_HOST,BLAT_PORT,server_dir,querypath,outpath]
    cmd = " ".join(cmds)
    print(cmd)
    out = spc.call(cmds)
    #prc = spc.Popen(cmd)
    print(cmds)
    print( out)

    out = ""
    #with open(os.path.join(BLAT_TMPDIR, outfn)) as f:
    #    out = f.read()
    results = parse_psl(os.path.join(BLAT_TMPDIR, outfn))
    print(results["T name"])
    return json.dumps([e for e in list(results["T name"].values)])


def parse_psl(fn):
    columns = ["match","mismatch","Ns","Q gap count", "Q gap bases", "T gap count", "T gap bases","strand","Q name", "Q size", "Q start", "Q end", "T name", "T size", "T start", "T end", "blockCount","blockSizes","qStarts","tStarts"]

    with open(fn) as fopen:
        print(fopen.readlines())
    results = pd.read_csv( fn, delimiter = "\t",skiprows=5, names = columns)
    print( results)
    return results

def query_seqs():
    pass
