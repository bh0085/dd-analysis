from flask import Flask
import subprocess as spc
import os, json
import pandas as pd
import numpy as np
app = Flask(__name__)

#from flask_cors import CORS
#CORS(app)
#api = Api(app)

import exports, queries
from dataset_models import Umi, session, db, Dataset, UmiGoTerm, UmiGeneId, GoTerm, NcbiGene, Segment
rsq = pd.read_sql_query
sq = session.query
from sqlalchemy import desc, asc, func


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




def datasetId(full_id):
      return int(full_id[:8])

@app.route("/exports/umis/fasta/<dataset>/")
def export_fasta(dataset):
  output = exports.make_fasta_file(dataset)
  return output

@app.route("/queries/umis/geneids/<dataset>/<query>/")
def umigeneids(dataset,geneid_query):
  return json.dumps(list(rsq(sq(Umi.id)\
    .join(UmiGeneId).join(NcbiGene).join(Dataset)\
      .filter(Dataset.id==datasetId(dataset)).filter(NcbiGene.geneid==int(geneid_query)).statement,db)\
        .id.values))
  

@app.route("/queries/umis/goterms/<dataset>/<query>/")
def umigoterms(dataset,query):
    return json.dumps(list(rsq(sq(Umi.id)\
    .join(UmiGoTerm).join(GoTerm).join(Dataset)\
      .filter(Dataset.id==datasetId(dataset)).filter(GoTerm.go_name.ilike(f"%{goterm_query}%")).statement,db)\
        .id.values))
  
@app.route("/queries/cells/geneids/<dataset>/<geneid_query>/")
def cellgeneids(dataset,geneid_query):

  

  return json.dumps(matches.values)
  
@app.route("/queries/cells/goterms/<dataset>/<goterm_query>/")
def cellgoterms(dataset,goterm_query):

  subq = session.query(Umi.seg,func.count(distinct(Umi.id)).label('count_matched')).join(UmiGoTerm).join(GoTerm)\
      .filter(GoTerm.go_name.ilike(f"%{goterm_query}%")).group_by(Umi.seg).subquery()

  subq2 = session.query(Umi.seg,func.count(distinct(Umi.id)).label('count_all'))\
      .group_by(Umi.seg).subquery()

  out4 = sq(distinct(Segment.id),subq.c.count,subq2.c.count).join(Umi).join(UmiGoTerm).join(GoTerm)\
    .join(subq, subq.c.seg == Segment.id)\
    .join(subq2, subq2.c.seg == Segment.id)\
    .filter(GoTerm.go_name.ilike(f"%{goterm_query}%"))\
    .all()

  out4["enrichment"]  = out4.count_matched / out4.count_all
  wholecells=out4.loc[out4.count_all >1]

  return json.dumps([list(e) for e in  wholecells[["segment","enrichment"]].values])

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
