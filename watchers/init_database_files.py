import pandas as pd
import os
from _config import DATA_DIR, DATASETS_ROOT


def init_database_files(inp_folder, dataset):
    nm = dataset["dataset"]
    segfn = os.path.join(inp_folder, "xumi_segment_base_"+nm)
    featfn = os.path.join(inp_folder, "xumi_feat_"+nm)
    basefn = os.path.join(inp_folder, "xumi_base_"+nm)

    out_folder = os.path.join(
        f"/data/dd-analysis/datasets/{dataset['dataset'] }/database_init_files/")
    if not os.path.isdir(out_folder):
        os.makedirs(out_folder)

    coord_data_pd = pd.read_csv(basefn, names=["unused_id", "x", "y"])
    annotation_data_pd = pd.read_csv(
        featfn, names=["unused_id", "molecule_type", "ar", "tr", "sequence"])
    seg_data_pd = pd.read_csv(
        segfn, names=["ignored_idx", "seg20", "unk2", "unk3", "unk4", "unk5", ])

    #umis2go_path = os.path.join(DATASETS_ROOT, nm, "goterms/umis2go.csv")
    genes2go_path = os.path.join(DATASETS_ROOT, nm, "goterms/genes2go.csv")

    umis2geneids_path = os.path.join(
        DATASETS_ROOT, nm, "genesymbols/umi_symbols.csv")
    genes2go = pd.read_csv(genes2go_path).dropna().rename(
        {"ncbi_gene": "ncbi_gene", "symbol":"symbol", "GO_NAME": "go_name", "GO_ID": "go_id"}, axis=1)


    # umis2go = pd.read_csv(umis2go_path).dropna().rename(
    #     {"umi": "umi_idx", "GO_NAME": "go_name", "GO_ID": "go_id"}, axis=1)        
    umis2geneids = pd.read_csv(umis2geneids_path).dropna().rename(
        {"umi": "umi_idx", "GO_NAME": "go_name", "GO_ID": "go_id"}, axis=1)


    for df in [coord_data_pd, annotation_data_pd, seg_data_pd]:
            df.index = coord_data_pd.index.rename("idx")
            df["dsid"] = int(nm[:8])

    for df in [ umis2geneids]:
            df.index = df.index.rename("ignored_index")
            df["dsid"] = int(nm[:8])
            
    #produce a small gzip output file having x,y,t for each point
    cfpath = os.path.join(out_folder, "database_coords.csv")
    afpath = os.path.join(out_folder, "database_annotations.csv")
    sfpath = os.path.join(out_folder, "database_segments.csv")

    coord_data_pd.to_csv(cfpath)
    annotation_data_pd.to_csv(afpath)
    seg_data_pd.to_csv(sfpath)


    coord_data_pd.to_csv(cfpath+".gz", compression='gzip')
    annotation_data_pd.to_csv(afpath+".gz", compression='gzip')
    seg_data_pd.to_csv(sfpath+".gz", compression='gzip')


    umidata = pd.concat([coord_data_pd[["x", "y", "dsid"]], annotation_data_pd[[
                        "molecule_type", "sequence"]], seg_data_pd[['seg20']]], axis=1)
    umidata.to_csv(os.path.join(out_folder, "database_umis.csv"))
    umis2geneids.to_csv(os.path.join(out_folder, "database_geneids.csv"))
    #umis2go.to_csv(os.path.join(out_folder, "database_umi2go.csv"))
    genes2go.to_csv(os.path.join(out_folder, "database_gene2go.csv"))



    return 0
