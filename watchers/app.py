from flask import Flask, Response, request
import subprocess as spc
import os, json
import pandas as pd
import numpy as np

from flask_cors import CORS
app = Flask(__name__)




import io

#cors = CORS(app, resources={r"/*": {"origins": "*"}})
#from flask_cors import CORS
#CORS(app)
#api = Api(app)



import exports, queries
from dataset_models import Umi, session, db, Dataset, GeneGoTerm, UmiGeneId, GoTerm, NcbiGene, Segment
rsq = pd.read_sql_query
sq = session.query
from sqlalchemy import desc, asc, func, distinct


@app.after_request
def after_request(response):
  response.headers.add('Access-Control-Allow-Origin', '*')
  response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
  response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
  return response

from flask import Flask,redirect    



TMPDIR = "/data/tmp"
BLAT_PORT = "8080"
BLAT_HOST = "localhost"
BLAT_TMPDIR = os.path.join(TMPDIR, "blat")
if not os.path.isdir(BLAT_TMPDIR):os.makedirs(BLAT_TMPDIR)

def datasetId(full_id):
      return int(str(full_id)[:8])

@app.route("/queries/umis/geneids/<dataset>/<query_input>/")
def umigeneids(dataset,query_input):
      
  if(":" in query_input):
        queries = query_input.split(":")
  else:
    queries = [query_input]

  out_arrays = {}
  for geneid_query in queries:
  
    if len(geneid_query)<3: continue
    try:
      ncbi_geneid = int(geneid_query)
            
      out_arrays[geneid_query] = list(rsq(sq(Umi.idx)\
        .join(UmiGeneId).join(NcbiGene).join(Dataset)\
          .filter(Dataset.id==datasetId(dataset)).filter(NcbiGene.geneid==ncbi_geneid).statement,db)\
            .idx.astype(object).values)

    except Exception:
      out_arrays[geneid_query] = list(rsq(sq(Umi.idx)\
        .join(UmiGeneId).join(NcbiGene).join(Dataset)\
          .filter(Dataset.id==datasetId(dataset)).filter(NcbiGene.symbol==str(geneid_query).upper()).statement,db)\
            .idx.astype(object).values)
  return json.dumps(out_arrays)

@app.route("/queries/cells/goterms/<dataset>/<goterm_query>/")
def cellgoterms(dataset,goterm_query):

  datasetId = lambda x: str(x)[:8]


  subq = session.query(Umi.seg,func.count(distinct(Umi.id)).label('count_matched')).join(UmiGeneId).join(NcbiGene).join(GeneGoTerm).join(GoTerm)\
    .filter(GoTerm.go_name.ilike(f"%{goterm_query}%")).group_by(Umi.seg).subquery()

  subq2 = session.query(Umi.seg,func.count(distinct(Umi.id)).label('count_all'))\
    .group_by(Umi.seg).subquery()

  out4 = rsq(sq(distinct(Segment.id).label("segment")).join(Umi).join(UmiGeneId).join(NcbiGene).join(GeneGoTerm).join(GoTerm)\
  .filter(GoTerm.go_name.ilike(f"%{goterm_query}%"))\
  .filter(Segment.dsid == datasetId(dataset))\
  .statement, db)


  # out4["enrichment"]  = out4.count_matched / out4.count_all
  # wholecells=out4.loc[out4.count_all >1]

  out5 = rsq(sq(Umi.idx, Umi.seg).filter(Umi.seg.in_(out4.segment.astype(object).unique())).statement, db).dropna()[["idx"]]

  return json.dumps([list(e) for e in out5[["idx"]].astype(object).values])

@app.route("/queries/umis/sequence/<dataset>/<query_input>/")
def umis_by_sequence(dataset,query_input):
        
  if(":" in query_input):
        queries = query_input.split(":")
  else:
    queries = [query_input]
  out_arrays = {}

  for sequence_query in queries:
    

    if len(sequence_query)<3: continue

    out_arrays[sequence_query]= [e[0] for e in sq(Umi.idx).filter(Umi.dsid == datasetId(dataset)).filter(Umi.sequence.ilike(f"%{sequence_query}%")).all()]


  return json.dumps(out_arrays)

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
    out = spc.call(cmds)

    #with open(os.path.join(BLAT_TMPDIR, outfn)) as f:
    #    out = f.read()

    results = parse_psl(os.path.join(BLAT_TMPDIR, outfn))

    return json.dumps([e for e in list(results["T name"].values)])


def parse_psl(fn):
    columns = ["match","mismatch","Ns","Q gap count", "Q gap bases", "T gap count", "T gap bases","strand","Q name", "Q size", "Q start", "Q end", "T name", "T size", "T start", "T end", "blockCount","blockSizes","qStarts","tStarts"]

    with open(fn) as fopen:
        print(fopen.readlines())
    results = pd.read_csv( fn, delimiter = "\t",skiprows=5, names = columns)
    print( results)
    return results


from flask import send_file

@app.route('/export/<dataset>/fasta') 
def export_fasta(dataset):
      
    umis = sq(Umi).filter(Umi.dsid==datasetId(dataset)).all()
    fasta = "\n".join([f">{u.idx}_({u.x}, {u.y})\n{u.sequence}" for u in umis] )

    return Response(
        fasta,
        mimetype="text/csv",
        headers={"Content-disposition":
                 f"attachment; filename={dataset}.fa"})


@app.route('/export/<dataset>/csv') 
def export_csv(dataset):
    umis = rsq(sq(Umi).filter(Umi.dsid==datasetId(dataset)).statement, db)
    s = io.StringIO()
    umis.to_csv(s)
    s.seek(0)
    csv = s.read()

    return Response(
        csv,
        mimetype="text/csv",
        headers={"Content-disposition":
                 f"attachment; filename={dataset}_export.csv"})

@app.route('/export/<dataset>/query/csv') 
def export_selection_csv(dataset):
       
    umi_list = json.loads(request.args.get('umis'))
    umis = rsq(sq(Umi).filter(Umi.dsid==datasetId(dataset)).filter(Umi.idx.in_(umi_list)).statement, db)

    s = io.StringIO()
    umis.to_csv(s)
    s.seek(0)
    csv = s.read()

    return json.dumps({"filename":f"{dataset}_query_export.csv", "text":csv})
    # return Response(
    #     csv,
    #     mimetype="text/csv",
    #     headers={"Content-disposition":
    #              f"attachment; filename={dataset}_query_export.csv"})

@app.route('/export/<dataset>/query/fasta') 
def export_selection_fasta(dataset):

    umi_list = json.loads(request.args.get('umis'))
    umis = rsq(sq(Umi).filter(Umi.dsid==datasetId(dataset)).filter(Umi.idx.in_(umi_list)).statement, db)

    fasta = "\n".join(
      list(umis.apply(lambda u: f">{u.idx}_({u.x}, {u.y})\n{u.sequence}"  ,axis =1 ).values))

    return json.dumps({"filename":f"{dataset}_query_export.fa", "text":fasta})
    # return Response(
    #     fasta,
    #     mimetype="text/csv",
    #     headers={"Content-disposition":
    #              f"attachment; filename={dataset}_query_export.fasta"})



def query_seqs():
    pass
