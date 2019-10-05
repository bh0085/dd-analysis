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
        featfn, names=["unused_id", "molecule_type", "ar", "total_reads", "sequence"])
    seg_data_pd = pd.read_csv(
        segfn, names=["ignored_idx", "seg20", "unk2", "unk3", "unk4", "unk5", ])

    for df in [coord_data_pd, annotation_data_pd, seg_data_pd]:
            df.index = coord_data_pd.index.rename("idx")
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
                        "molecule_type", "sequence","total_reads"]], seg_data_pd[['seg20']]], axis=1)
    umidata.to_csv(os.path.join(out_folder, "database_umis.csv"))



    return 0
