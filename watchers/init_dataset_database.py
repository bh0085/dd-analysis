import os
import pandas as pd
from dataset_models import db, meta, session, NcbiGene,Segment, EnsemblGene, UmiGeneId, UmiGoTerm, Umi, Dataset, GoTerm
from _config import DATASETS_ROOT

def init_dataset_database(folder, dataset):

    d = dataset["dataset"]

    dskey = int(d[:8])
    print (f"entering data for dataset {d}")
    print("deleting if exists")
    matches = session.query(Dataset).filter(Dataset.id == dskey).first()
    print(matches)
    if matches:
        print(matches)
        session.delete(matches)
        session.commit()

    print("done")
    dataset_folder = os.path.join(DATASETS_ROOT,d)

    print("reading csv data)")
    database_files_folder = os.path.join(dataset_folder, "database_init_files")
    umi_data = pd.read_csv(os.path.join(database_files_folder,"database_umis.csv"),index_col="idx")
    geneid_data = pd.read_csv(os.path.join(database_files_folder,"database_geneids.csv"),index_col="ignored_index")
    umi_go_data = pd.read_csv(os.path.join(database_files_folder,"database_umi2go.csv"),index_col="ignored_index")
    dataset_data= pd.DataFrame(pd.Series({"id":int(dskey),"name":"dataset1"})).T.set_index("id")

    print (f"{len(dataset_data)} datasets, {len(umi_data)} umis, {len(umi_go_data)} go terms")
    print("entering umi & dataset info")
    dataset_data.to_sql("dataset",db, if_exists='append')

    segment_data = pd.DataFrame(pd.Series(umi_data.seg20.unique()).rename("og_segid")).assign(dsid=dskey)
    segment_data.to_sql("segment", db, if_exists="append", index = False)
    segs = session.query(Segment).all() 
    seg_map = dict([(s.og_segid, s.id) for s in segs])



    umi_data["seg"] = umi_data.seg20.apply(lambda x: seg_map[x])
    umi_data.to_sql("umi",db, if_exists='append')
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

    goterm_data = umi_go_data[["go_name","go_id"]].drop_duplicates(["go_id"])
    existing_goterms = set([e[0] for e in session.query(GoTerm.go_id).all()])
    goterm_data = goterm_data.loc[~goterm_data.go_id.isin(existing_goterms)]

    #duplicates removed in earlier step
    ncbi_genedata.to_sql("ncbigene",db, if_exists='append', index = False)
    ens_genedata.to_sql("ensemblgene",db, if_exists='append', index = False)
    goterm_data.to_sql("goterm",db, if_exists='append', index = False)


    #transform UMI x GENE data to use newly assigned ids
    geneid_data["umi_id"] = geneid_data["umi_idx"].apply(lambda x:umi_map[x])
    geneid_data_subs= geneid_data[["umi_id","ensembl_gene","ncbi_gene"]]\
            .rename({"ensembl_gene":"ensembl_geneid","ncbi_gene":"ncbi_geneid"},axis ="columns").drop_duplicates()

 
    print("entering additional gene information")
    #transform UMIX x GO ids to newly assigned ids
    umi_go_data["umi_id"] = umi_go_data["umi_idx"].apply(lambda x: umi_map[x])
    umi_go_data[["umi_id","go_id"]].to_sql("umigoterm",db, if_exists='append',index = False)
    #segment_go_data.to_sql("segmentgoterm",db, if_exists='append',index = False)


    geneid_data_subs.to_sql("umigeneid",db, if_exists='append', index = False)
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
        #meta.drop_all(db)
        print ("creating all tables")
        meta.create_all(db)

    if args.load:
        print("entering a dataset all datasets")
        init_dataset_database(None, {"dataset":args.load})

if __name__ =="__main__":
    main()