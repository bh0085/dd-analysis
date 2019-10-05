from flask import Flask, Response, request,make_response
import subprocess as spc
import os, json
import pandas as pd
import numpy as np

from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate


app = Flask(__name__,static_url_path='') 
app.config['SQLALCHEMY_DATABASE_URI'] = "postgres://ben_coolship_io:password@localhost/dd"

from flask import Flask, request, send_from_directory
import io

import exports, queries
from dataset_models import Umi, Dataset, GeneGoTerm, UmiGeneId, GoTerm, NcbiGene, Segment, db2
rsq = pd.read_sql_query
from sqlalchemy import desc, asc, func, distinct

db2.init_app(app) 
migrate = Migrate(app, db2)

from db import Session
from flask_sqlalchemy_session import flask_scoped_session
session = flask_scoped_session(Session, app)

sq = session.query
db = session.connection()


queries = {}

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

# set the project root directory as the static folder, you can set others.

# @app.route('/notebooks/<path:path>')
# def send_js(path):
#     print("TRIGGERED ROUTE")
#     return send_from_directory('/home/ben_coolship_io/dd-alignment-server/notebooks/', path)

import re
#!/usr/bin/python
import os, requests, sys, json


@app.route("/analysis/<dataset>/generate_notebook/")
def generate_notebook(dataset):
  cred = json.loads(open("/home/ben_coolship_io/github_credentials.json").read())
  username=cred["username"]
  password=cred["password"]
  filepath = "../notebooks/test_002.ipynb"
  filename = os.path.basename(filepath)

  content=open(filepath, 'r').read()
  custom_content = re.compile('{dataset}').sub(dataset,content)


  sample_query_string = f'http://35.237.243.111:5000/queries/umis/sequence/29045011805220233/GAGAAG?format=csv'
  if "/" in request.args.get('last_query'):
    # here we want to get the value of user (i.e. ?user=some-value)
    last_query = request.args.get('last_query')
    custom_content = re.compile('{query}').sub(last_query,custom_content)
  else:
    custom_content = "\n".join([
      l if not "_LAST_QUERY" 
      else re.compile("LAST_QUERY").sub("SAMPLE_QUERY", re.compile('{query}').sub(sample_query_string,l))
      for l in custom_content.splitlines() 
      ]
      )
    

  r = requests.post('https://api.github.com/gists',
                    json.dumps({'files':{filename:{"content":custom_content}},
                              'public':True}),
                    auth=requests.auth.HTTPBasicAuth(username, password)) 
  out = r.json()

  return json.dumps({"url":f"https://colab.research.google.com/gist/bh0085/{out['id']}"})

@app.route("/dataset_stats/<dataset>")
def dataset_stats(dataset):
    dsid = getDatasetId(dataset)
    return json.dumps({
    "n_umis":sq(Umi).filter(Umi.dsid==dsid).count(),
    "n_segments":sq(Segment).filter(Segment.dsid==dsid).count()
    })




@app.route("/api/<dataset>/cells/")
def api_cells(dataset):
    ids = json.loads(request.args.get("ids"))
    return  json.dumps({s.id:s.as_detailed_frontend_dict()  
                     for s in sq(Segment).filter(Segment.dsid==getDatasetId(dataset))
                     .filter(Segment.id.in_(ids)).all()})


def round_floats(o):
    if isinstance(o, float): return round(o, 2)
    if isinstance(o, dict): return {k: round_floats(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)): return [round_floats(x) for x in o]
    return o


json.dumps(round_floats([23.63437, 23.93437, 23.842347]))
@app.route("/api/<dataset>/cell_neighborhoods/")
def api_cell_neighborhoods(dataset):
    ids = json.loads(request.args.get("ids"))
    attrs = json.loads(request.args.get("attrs"))
    cells = [  seg for cid in ids  for seg in sq(Segment).get(cid).intersecting_cells ]
    return  json.dumps(round_floats({s.id:s.as_custom_frontend_dict(attrs)  
                     for s in cells}))



@app.route("/cells/all/<dataset>")
def cells_all(dataset):
  return json.dumps({s.id:s.as_frontend_dict()  
                     for s in sq(Segment).filter(Segment.dsid==getDatasetId(dataset)).filter(Segment.n_umis > 20).limit(250).all()})
  # return json.dumps(rsq(sq(Segment).filter(Segment.dsid==getDatasetId(dataset)).filter(Segment.n_umis > 20).statement, session.connection())[["id","n_umis","meanx","meany","rval", "gval", "bval","area12","hull128"]].to_dict(orient="records"))







@app.route("/segmentations/<dataset>/passing/")
def segmentations_passing_pixels(dataset):
      
  #passing_path =  os.path.join( f"/data/dd-analysis/datasets/{dataset}/segmentations/", "passing_pixels.csv")
  #passing_segments_df = pd.read_csv(passing_path, index_col=["x","y","segment"])
  #nested = passing_segments_df.reset_index(["y","segment"]).groupby("x").apply(lambda x:  x.groupby("y").apply(lambda g: list(g.segment.unique())).to_dict()).to_dict()
  return json.dumps({})
  
getDatasetId =  lambda dsname: int(dsname[:8])


from io import BytesIO
from flask import Flask, Response
from werkzeug import FileWrapper
from sqlalchemy import text



@app.route("/dataset/<dataset>/preview2k")
def preview2k(dataset):
      dsid = getDatasetId(dataset)
      import tifffile
      # tiffb = tifffile.imread(io.BytesIO(session.execute(text("SELECT ST_AsTIFF(raster_2k_all, 'GTiff') from dataset;")).fetchone()[0]))
      # b = io.BytesIO(session.execute(text("SELECT ST_AsTIFF(raster_2k_all, 'GTiff') from dataset;")).fetchone()[0])
      # b = io.BytesIO(session.execute(text("SELECT ST_AsTIFF(ST_Resample(raster_2k_all,100,100), 'GTiff') from dataset;")).fetchone()[0])
      b = io.BytesIO(session.execute(text("SELECT ST_AsJPEG(ST_Resample(raster_2k_all,200,200)) from dataset;")).fetchone()[0])
      w = FileWrapper(b)
      return Response(w, mimetype="image/jpeg", direct_passthrough=True)

@app.route("/segmentations/<dataset>/annotations/")
def segmentations_annotations(dataset):
    dsid = getDatasetId(dataset)
    segs =rsq(sq(Segment).filter(Segment.dsid == dsid).statement,db)
    return segs.to_json(orient="index")

@app.route("/segmentations/<dataset>/segments_by_xy/")
def segmentations_by_xy(dataset):
  x0 = request.args.get("x0")
  x1 = request.args.get("x1")
  y0 = request.args.get("y0")
  y1 = request.args.get("y1")

  query_df = rsq(sq(Segment).filter(Segment.dsid==getDatasetId(dataset))\
    .filter(Segment.meanx >= x0).filter( Segment.meanx <= x1).filter( Segment.meany >= y0).filter( Segment.meany <= y1).statement,db)
  return query_df.to_json(orient = "index")


import gzip
@app.route("/segmentations/<dataset>/winning")
def segmentations_winning_pixels(dataset):

  winning_path =  os.path.join( f"/data/dd-analysis/datasets/{dataset}/segmentations/", "winning_pixels.csv")
  winning_segments_df = pd.read_csv(winning_path, index_col=["x","y"])
  nested = winning_segments_df.reset_index("y").groupby("x").apply(lambda x:  x.set_index("y").to_dict()['segment']).to_dict()
  return json.dumps(nested)




@app.route("/queries/xyrect/<dataset>/")
def query_xyrect(dataset):
  region_json = request.args.get("xy_rect_json")
  region = json.loads(region_json)
  return json.dumps({
    f"({region['x0']:.01f},{region['y0']:.01f}) ({region['x1']:.01f},{region['y1']:.01f})":list(rsq(sq(Umi.idx)\
        .filter(Umi.x>region['x0'])\
          .filter(Umi.x<region['x1'])\
            .filter(Umi.y>region['y0'])\
              .filter(Umi.y<region['y1']).statement,db).idx.astype(object).values)\
  })

@app.route("/queries/umis/geneids/<dataset>/<query_input>/")
def umigeneids(dataset,query_input):
      
  fmt = request.args.get("format","json")

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

  ds = dataset
  datasetId = lambda x: str(x)[:8]

  subq = session.query(Umi.seg,func.count(distinct(Umi.id)).label('count_matched')).join(UmiGeneId).join(NcbiGene).join(GeneGoTerm).join(GoTerm)\
  .filter(GoTerm.go_name.ilike(f"%{goterm_query}%")).group_by(Umi.seg).subquery()

  subq2 = session.query(Umi.seg,func.count(distinct(Umi.id)).label('count_all'))\
  .group_by(Umi.seg).subquery()

  out4 = rsq(sq(Segment.id.label("segment"),func.count(Segment.id).label("n_go_umis")).join(Umi).join(UmiGeneId).join(NcbiGene).join(GeneGoTerm).join(GoTerm)\
  .filter(GoTerm.go_name.ilike(f"%{goterm_query}%"))\
  .filter(Segment.dsid == datasetId(ds))\
            .group_by(Segment.id)
  .statement, db)

  out4a0 = rsq(sq(Segment.id.label("segment"),func.count(Segment.id).label("n_go_umis")).join(Umi).join(UmiGeneId).join(NcbiGene).join(GeneGoTerm).join(GoTerm)\
  .filter(Segment.dsid == datasetId(ds))\
            .group_by(Segment.id)
  .statement, db)

  out4a = rsq(sq(Segment.id.label("segment"),func.count(Segment.id).label("n_umis")).join(Umi).group_by(Segment.id).filter(Segment.dsid == datasetId(ds)).statement,db)

  out5b = pd.concat([(out4.set_index("segment") / out4a0.set_index("segment")),out4a.set_index("segment")],axis=1)
  real_cells = out5b.loc[lambda x: x.n_umis >1]
  output_by_segment = real_cells[["n_go_umis"]]
  top_segs = output_by_segment.loc[lambda x: x.n_go_umis > x.n_go_umis.quantile(.95)]
  out5 = rsq(sq(Umi.idx.label("umi_idx"), Umi.seg.label("segment")).filter(Umi.seg.in_(top_segs.reset_index().segment.astype(object).unique())).statement, db)
  joined = top_segs.join(out5.set_index('segment'),on="segment")
  out= json.dumps( {goterm_query: list([[int(e[0]),round(e[1]*1000)/1000] for e in joined[["umi_idx","n_go_umis"]].values])})
  return out

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
