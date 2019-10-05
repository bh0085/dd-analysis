import os
import pandas as pd

import pandas as pd
import numpy as np
import os
import scipy
from sqlalchemy import create_engine , cast, Index, func,ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String,BigInteger
from geoalchemy2 import Geometry
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship

from dataset_models import NcbiGene,Segment, EnsemblGene, UmiGeneId, GeneGoTerm, Umi, Dataset, GoTerm
from sqlalchemy import MetaData
from db import session
from _config import DATASETS_ROOT
meta = MetaData()

def init_transcripts_database(folder, dataset):

    d = dataset["dataset"]
    dskey = int(d[:8])
    print (f"entering data for dataset {d}")
    print("deleting if exists")

    matches = session.query(Dataset).filter(Dataset.id == dskey).first()
    dataset_folder = os.path.join(DATASETS_ROOT,d)
    database_files_folder = os.path.join(dataset_folder, "database_init_files")

    genes2go_path = os.path.join(DATASETS_ROOT, d, "goterms/genes2go.csv")

    umis2geneids_path = os.path.join(
        DATASETS_ROOT, d, "genesymbols/umi_symbols.csv")
    genes2go = pd.read_csv(genes2go_path).dropna().rename(
        {"ncbi_gene": "ncbi_gene", "symbol":"symbol", "GO_NAME": "go_name", "GO_ID": "go_id"}, axis=1)    
    umis2geneids = pd.read_csv(umis2geneids_path).dropna().rename(
        {"umi": "umi_idx", "GO_NAME": "go_name", "GO_ID": "go_id"}, axis=1)

    for df in [ umis2geneids]:
            df.index = df.index.rename("ignored_index")
            df["dsid"] = int(d[:8])
            
    # # this code is redundant... simple resulting from pulling code out of the "file writing" script
    # # and the "database entry" script
    # # ... commented it out
    #
    # umis2geneids.to_csv(os.path.join(out_folder, "database_geneids.csv"))
    # genes2go.to_csv(os.path.join(out_folder, "database_gene2go.csv"))
    # ###
    # geneid_data = pd.read_csv(os.path.join(database_files_folder,"database_geneids.csv"),index_col="ignored_index")
    # gene_go_data = pd.read_csv(os.path.join(database_files_folder,"database_gene2go.csv"))
    # # should just compute te above and run from there

    geneid_data = umis2geneids
    gene_go_data = genes2go


    umis = session.query(Umi).filter(Umi.dsid == dskey).all()
    umi_map = dict([(u.idx,u.id) for u in umis])


    #remove gene duplicate information
    ncbi_genedata = geneid_data[["desc","symbol","ncbi_gene"]].rename({"ncbi_gene":"geneid"},axis="columns").drop_duplicates(["geneid"])
    existing_ncbi_ids = set([e[0] for e in session.query(NcbiGene.geneid).all()])
    ncbi_genedata = ncbi_genedata.loc[~ncbi_genedata.geneid.isin(existing_ncbi_ids)]
    #ncbi_genedata["desc_ts"] = ncbi_genedata["desc"]

    ens_genedata = geneid_data[["ensembl_gene"]].rename({"ensembl_gene":"geneid"},axis="columns").drop_duplicates(["geneid"])
    existing_ens_ids = set([e[0] for e in session.query(EnsemblGene.geneid).all()])
    ens_genedata = ens_genedata.loc[~ens_genedata.geneid.isin(existing_ens_ids)]

    goterm_data = gene_go_data[["go_name","go_id"]].drop_duplicates(["go_id"])
    existing_goterms = set([e[0] for e in session.query(GoTerm.go_id).all()])
    goterm_data = goterm_data.loc[~goterm_data.go_id.isin(existing_goterms)]

    #duplicates removed in earlier step
    ncbi_genedata.to_sql("ncbigene",session.connection(), if_exists='append', index = False)
    ens_genedata.to_sql("ensemblgene",session.connection(), if_exists='append', index = False)
    goterm_data.to_sql("goterm",session.connection(), if_exists='append', index = False)


    #transform UMI x GENE data to use newly assigned ids
    geneid_data["umi_id"] = geneid_data["umi_idx"].apply(lambda x:umi_map[x])
    geneid_data_subs= geneid_data[["umi_id","ensembl_gene","ncbi_gene"]]\
            .rename({"ensembl_gene":"ensembl_geneid","ncbi_gene":"ncbi_geneid"},axis ="columns").drop_duplicates()

 
    print("entering additional gene information")
    #transform UMIX x GO ids to newly assigned ids
    #gene_go_data["umi_id"] = umi_go_data["umi_idx"].apply(lambda x: umi_map[x])
    gene_go_data[["ncbi_gene","go_id"]].to_sql("genegoterm",session.connection(), if_exists='append',index = False)
    #segment_go_data.to_sql("segmentgoterm",session.connection(), if_exists='append',index = False)

    geneid_data_subs.to_sql("umigeneid",session.connection(), if_exists='append', index = False)

    session.commit()
    return 0