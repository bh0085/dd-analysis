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

def init_dataset_database(folder, dataset):

    d = dataset["dataset"]
    dskey = int(d[:8])
    print (f"entering data for dataset {d}")
    print("deleting if exists")

    matches = session.query(Dataset).filter(Dataset.id == dskey).first()
    print(matches)
    if matches:
        print(matches)
        session.query(Dataset).filter(Dataset.id == dskey).delete()
        session.commit()

    print("done")
    dataset_folder = os.path.join(DATASETS_ROOT,d)

    print("reading csv data)")
    database_files_folder = os.path.join(dataset_folder, "database_init_files")
    umi_data = pd.read_csv(os.path.join(database_files_folder,"database_umis.csv"),index_col="idx")
    geneid_data = pd.read_csv(os.path.join(database_files_folder,"database_geneids.csv"),index_col="ignored_index")
    gene_go_data = pd.read_csv(os.path.join(database_files_folder,"database_gene2go.csv"))
    dataset_data= pd.DataFrame(pd.Series({"id":int(dskey),"name":"dataset1"})).T.set_index("id")

    print (f"{len(dataset_data)} datasets, {len(umi_data)} umis, {len(gene_go_data)} go terms")
    print("entering umi & dataset info")
    dataset_data.to_sql("dataset",session.connection(), if_exists='append')

    dsname = d

    print("BEWARE: THIS REQUIRES A CUSTOM DIRECTORY @ /DATA/JOSH/{dsname} THESE REFERENCES MUST BE REDIRECTED")
    xumi_base_fn = os.path.join(f'/data/josh/{dsname}/',f'xumi_base_{dsname}')
    xumi_feat_fn = os.path.join(f'/data/josh/{dsname}/',f'xumi_feat_{dsname}')
    xumi_base_seg_fn = os.path.join(f'/data/josh/{dsname}/',f'xumi_segment_base_{dsname}')
    xumi_feat_seg_fn = os.path.join(f'/data/josh/{dsname}/',f'xumi_segment_feat_{dsname}')

    xumi_base_df = pd.read_csv(xumi_base_fn, names = ["xumi_id","x","y"])
    xumi_feat_df = pd.read_csv(xumi_feat_fn, names = ["xumi_id","mol","reads1","reads2","seq"])
    xumi_feat_seg_df = pd.read_csv(xumi_feat_seg_fn)#, names = cols)
    xumi_base_seg_df = pd.read_csv(xumi_base_seg_fn, names = ["id","seg","empty1","empty2","empty3"])

    #parse beacons and targets in xumi file
    beacons = xumi_feat_df.loc[lambda x: x.mol==-1]
    targets = xumi_feat_df.loc[lambda x: x.mol==0]
    beacons.index.name = "xumi_line_number"
    targets.index.name = "xumi_line_number"

    beacons_with_segs = beacons.join(xumi_base_seg_df).reset_index().set_index("xumi_id")
    tgts_with_segs = targets.join(xumi_base_seg_df).reset_index().set_index("xumi_id")


    #load and parse full dataset with xumi files as reference
    full_ds_key = pd.read_csv(f"/data/full_datasets/{dsname}/key_minuei2dim2DMv1.0_.csv",names=["key_mol","xumi_id","mle_id"])
    full_ds = pd.read_csv(f"/data/full_datasets/{dsname}/data_minuei2dim2DMv1.0_.csv", names=["beacon_mle_id","target_mle_id","n_uei","n_reads"])

    beacon_key_lookups = full_ds_key.loc[full_ds_key.key_mol== 0].set_index("mle_id").xumi_id.sort_index()
    target_key_lookups = full_ds_key.loc[full_ds_key.key_mol== 1].set_index("mle_id").xumi_id.sort_index()

    full_ds["beacon_xumi_id"] = full_ds.join(beacon_key_lookups, on="beacon_mle_id").xumi_id
    full_ds["target_xumi_id"] = full_ds.join(target_key_lookups, on="target_mle_id").xumi_id
    full_ds["target_seg"] = full_ds.join(tgts_with_segs, on="target_xumi_id" )["seg"]
    full_ds["beacon_seg"] = full_ds.join(beacons_with_segs, on="beacon_xumi_id" )["seg"]

    out= full_ds.set_index("target_xumi_id").join(targets.reset_index()[["xumi_line_number","xumi_id"]].set_index("xumi_id").xumi_line_number.rename("xumi_target_line_number")).reset_index()
    out= out.set_index("beacon_xumi_id").join(beacons.reset_index()[["xumi_line_number","xumi_id"]].set_index("xumi_id").xumi_line_number.rename("xumi_beacon_line_number")).reset_index()
    target_uei_counts = out.groupby("xumi_target_line_number").n_uei.sum().sort_index()
    beacon_uei_counts = out.groupby("xumi_beacon_line_number").n_uei.sum().sort_index()


    from scipy.cluster.hierarchy import linkage
    from scipy.spatial.distance import squareform

    n_cells = 0
    n_umis = 0
    umi_data["seg"]= None
    umi_data["transient_umi"]  = umi_data.filter(regex="^(?!seg)").apply(lambda x: Umi(**x.to_dict()),axis=1)
    cnt = -1
    cells_gb = umi_data.groupby("seg20")
    for k,umis_grp in cells_gb:
        cnt+=1
        n_umis += len(umis_grp)
        if cnt % 20 == 0: print(cnt, n_cells, n_umis)
            
        if len(umis_grp) == 1:
            nc = Segment(dsid=dskey)
            umis_grp.iloc[0].transient_umi.segment = nc
            session.add(nc)
            continue

        allcounts = pd.concat([target_uei_counts,beacon_uei_counts])
        udf_withcounts = umis_grp.join(allcounts.rename("uei_count"))
        udf_withcounts.index.name="xumi_idx"
        distance_subset = udf_withcounts[["uei_count","x","y"]].reset_index()
        distance_subset["key"] = 1
        distance_grid = pd.merge(distance_subset,distance_subset, on ="key")

        e = distance_grid
        overlap =(2*(e.uei_count_x*e.uei_count_y)**-.5) / (1/e.uei_count_x+1/e.uei_count_y) * np.e**(-1*((e.x_x - e.x_y)**2 + (e.y_x- e.y_y)**2)/(1/e.uei_count_x+1/e.uei_count_y))
        overlap_2d = pd.concat([e,overlap.rename("overlap_factor")],axis=1)[["xumi_idx_x","xumi_idx_y","overlap_factor"]].set_index(["xumi_idx_x","xumi_idx_y"])["overlap_factor"].unstack()
        threshold = .5
        distances_2d = 1 - (overlap_2d > threshold)
        distances_square = squareform(distances_2d)
        clustering = linkage(distances_square)
        cluster_memberships = scipy.cluster.hierarchy.cut_tree(clustering,height=.2)
        memberships = pd.concat([umis_grp.reset_index(),pd.Series(cluster_memberships[:,0]).rename("linkage_cluster")],axis =1).set_index("idx")
    
        for k2,g2 in memberships.groupby("linkage_cluster"):
            nc = Segment(dsid=dskey)
            for u in umi_data.loc[g2.index].transient_umi:
                u.segment = nc
                session.add(u)
            session.add(nc)
            n_cells+=1

    session.commit()


    umis = session.query(Umi).all()
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


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--init', dest='init', action='store_const',
                        const=True, default=False,
                        help='recreates tables')

    parser.add_argument('--load', dest='load',default=None,
                        help='reload all database content')
    args = parser.parse_args()
    if args.init:
        print ("dropping and remaking all database tables")
        #meta.drop_all(session.connection())
        print ("creating all tables")
        meta.create_all(session.connection())

    if args.load:
        print("entering a dataset all datasets")
        init_dataset_database(None, {"dataset":args.load})

if __name__ =="__main__":
    main()