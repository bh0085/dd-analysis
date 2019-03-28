from flask import Flask
import subprocess as spc
import os, json
import pandas as pd
app = Flask(__name__)

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


@app.route("/umi/go_keywords/<dataset>/<query>/")
def go_keywords(dataset,query):
    umis_gofile = f"/data/dd-analysis/datasets/{dataset}/goterms/umis2go.csv"
    with pd.read

@app.route("/segment/go_keywords/<dataset>/<query>/")
def go_keywords(dataset,query):
    segments_gofile = f"/data/dd-analysis/datasets/{dataset}/goterms/segments2go.csv"
    


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
