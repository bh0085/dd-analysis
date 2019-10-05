import pandas as pd
from dataset_models import Umi, Dataset, GeneGoTerm, UmiGeneId, GoTerm, NcbiGene, Segment

from db import session
rsq = pd.read_sql_query
sq = session.query

from sqlalchemy import desc, asc, func, distinct
import pandas as pd
import numpy as np
import json, os
from scipy import stats

getDatasetId =  lambda dsname: int(dsname[:8])

def init_segmentations(tmpfolder, dataset):

    print("initiaizing segmentations")
    nm = dataset["dataset"]
    out_folder = os.path.join(
        f"/data/dd-analysis/datasets/{nm}/segmentations/")
    if not os.path.isdir(out_folder):
        os.makedirs(out_folder)


    umis= rsq(sq(Umi).filter(Umi.dsid==getDatasetId(nm)).statement,session.connection()).rename({"id":"umi_id"},axis="columns").set_index("umi_id")
    # initialize segment information
    
    segment_umis = rsq( sq(Umi.dsid,Umi.id.label("umi_id"),Umi.x,Umi.y,Segment.id.label("seg_id")).filter(Umi.dsid==getDatasetId(nm)).join(Segment).statement,session.connection())
    
    seg_counts = segment_umis.seg_id.value_counts()

    segs = segment_umis.join(segment_umis.seg_id.value_counts().rename("seg_umi_count"), on = "seg_id").loc[lambda x: x.seg_umi_count>20]
    segs_by_id = segs.set_index("seg_id")

    print("computing eigensystems")
    #compute normalized distances to cell centers using eigenvectors of the covariance matrix
    esys = segs_by_id.groupby("seg_id").apply(lambda g: np.linalg.eig(np.cov(*g[["x","y"]].values.T)))
    emeans = segs_by_id[["x","y"]].groupby("seg_id").mean()

    #USEFUL CODE... not used
    # proj_dist = segs_by_id.reset_index().set_index("umi_id").apply(
    #     lambda u:(lambda e: (np.dot(u[["x","y"]] - emeans.loc[u.seg_id] ,e[1][:,0])/(e[0][0]**.5))**2 + \
    #                         (np.dot(u[["x","y"]] - emeans.loc[u.seg_id] ,e[1][:,1])/(e[0][1]**.5))**2)(esys.loc[u.seg_id]),axis=1)\
    #     .rename("transformed_dist")


    # segs_with_dists = segs_by_id.join(proj_dist.reset_index().set_index("umi_id"),on="umi_id")
    # segs_with_dists = segs_with_dists.join(umis[["total_reads"]],on="umi_id")
    s_mean_xs =  segment_umis.groupby("seg_id").x.mean()
    s_mean_ys =  segment_umis.groupby("seg_id").y.mean()
    
    dbsegs = sq(Segment).filter(Segment.dsid == getDatasetId(nm)).all()
    for s in dbsegs:
    
        s.n_umis = int(seg_counts.loc[int(s.id)])
        s.meanx = s_mean_xs.get(s.id)
        s.meany = s_mean_ys.get(s.id)
    
    
        if ( esys.index.contains(s.id)):
            s.evec0x = esys.loc[s.id][1][0][0]
            s.evec0y = esys.loc[s.id][1][1][0]
            s.evec1x = esys.loc[s.id][1][0][1]
            s.evec1y = esys.loc[s.id][1][1][1]
            s.eval0 =  esys.loc[s.id][0][0]
            s.eval1 =  esys.loc[s.id][0][1]
            
        session.add(s)

    session.commit()

    segs_with_dists = segs_by_id.join(umis[["total_reads"]],on="umi_id")

    combined_series = pd.Series()
    print("computing segment hashes")
    i = 0
    for k, g in segs_with_dists.groupby(level=0):
        gsubs = g.loc[g.total_reads>0]
        xmin= gsubs.x.mean()- (np.max([gsubs.x.std(),.5]))
        xmax= gsubs.x.mean()+ (np.max([gsubs.x.std(),.5]))
        ymin= gsubs.y.mean()- (np.max([gsubs.y.std(),.5]))
        ymax= gsubs.y.mean()+ (np.max([gsubs.y.std(),.5]))

        #X, Y = np.mgrid[xmin:xmax:500j, ymin:ymax:500j]

        pts = 2
        res = 10**(-1*pts)
        #print(res)
        X, Y = np.round(np.mgrid[np.round(xmin,pts):np.round(xmax,pts):res, np.round(ymin,pts):np.round(ymax,pts):res],pts)
        positions = np.vstack([X.ravel(), Y.ravel()])

        values = np.vstack([gsubs.x,gsubs.y])
        kernel = stats.gaussian_kde(values,weights=np.log(gsubs.total_reads.astype(float).values))#, bw_method=1,)
        Z = np.reshape(kernel.evaluate(positions).T, X.shape)

        df = pd.DataFrame(Z)#/ np.sum(Z)
        df.columns =pd.Series(Y[0,:]).rename("y")
        df.index =pd.Series(X[:,0]).rename("x")

        series = pd.DataFrame(df.stack()).assign(g=k).set_index("g",append=True).iloc[:,0]
        combined_series = pd.concat([series,combined_series])
        
        i+=1
        if i % 10 == 0:
            print(i)

        
    combined_series=combined_series.rename("kde_vals")
    val_sorted  = combined_series.sort_values(0,ascending=False).sort_index(level=[0,1], sort_remaining=False)

    print("computing pixel segmentations")
    #threshold kde distributions to create "passing" pixels considered to be hits for each cell segment
    passing_pixels = val_sorted.groupby("g").apply(lambda x: x.loc[x> 1]).reset_index(level=0,drop=True)

    #argmax thresholded kde distributions to create bitmasks indicating which cell will have precendence in each pixel
    best_seg = val_sorted.reset_index(level = "g").loc[lambda x: ~x.index.duplicated()].g
    best_val = val_sorted.reset_index(level = "g").loc[lambda x: ~x.index.duplicated()].kde_vals
    winning_pixels = best_seg.loc[best_val > 1].rename("segment")

    passing_pixels.index.levels[2].name = "segment"
    winning_pixels.to_frame().to_csv(os.path.join(out_folder,"winning_pixels.csv"))
    passing_pixels.to_frame().to_csv(os.path.join(out_folder,"passing_pixels.csv"))

    print("computing nested segmentations")
    passing_segments_df_2 = passing_pixels.reset_index(level=2).sort_index(level=[0,1])
    lists = passing_segments_df_2.groupby(["x","y"]).apply(lambda g: [int(e) for e in g.segment.values])
    nests = lists.to_frame().reset_index("x").groupby(by="x").apply(lambda g:g[0].to_dict()).to_dict()

    with open(os.path.join(out_folder,"passing_pixels_nested.json"),"w") as fopen:
        fopen.write( json.dumps(nests))

    #passing_pixels.to_csv(os.path.join(out_folder,"passing_pixels.csv"))

    return 0

def get_winning_pixels(nm):
    dsid = getDatasetId(nm)
    winning_path =  os.path.join( f"/data/dd-analysis/datasets/{nm}/segmentations/", "winning_pixels.csv")
    return pd.read_csv(winning_path, index_col=["x","y"])

def get_passing_pixels(nm):
    dsid = getDatasetId(nm)
    passing_path =  os.path.join( f"/data/dd-analysis/datasets/{nm}/segmentations/", "passing_pixels.csv")
    return pd.read_csv(passing_path, index_col = ["x","y","segment"])

def get_passing_pixels_nested_json(nm):

    with open(os.path.join(out_folder,"passing_pixels_nested.json")) as fopen:
        return fopen.read()
 