
from sqlalchemy import text
from dataset_models import Segment, Umi, getDatasetId
from db import session
import geoalchemy2
sq = session.query
#from https://gist.github.com/dwyerk/10561690
from shapely.ops import cascaded_union, polygonize
from scipy.spatial import Delaunay
from  scipy import stats
import numpy as np
import math
from shapely import geometry
import gdal, osr
from geoalchemy2.shape import to_shape, from_shape
import pandas as pd

## ALERT! UNTESTED CODE!

def init_postgis(tmpdir, dataset, key= None):
    print("INITIAILIZING POSTGIS SQL")
    nm = dataset["dataset"]
    dsid = getDatasetId(nm)
    dsname = dsid

    # session.execute(f"UPDATE segment SET hull=null WHERE segment.dsid= {dsid}")
    # session.execute(f"UPDATE segment SET kde_density=null WHERE segment.dsid= {dsid}")

    # cells = sq(Segment).filter(Segment.dsid==dsname).filter(Segment.n_umis>20)
    # valid_cells = pd.Series(cells,index =[ c.id for c in cells])
    # def make_convex_hull_safe(x):
    #     if len(x.umis)<3:
    #         return None
    #     try:
    #         return sp_spatial.ConvexHull([[u.x,u.y]  for u in x.umis] )
    #     except Exception as e:
    #         return None
    # hulls = valid_cells.apply(make_convex_hull_safe)
    # cells_and_hulls = pd.concat([valid_cells.rename("cell"),hulls.rename("hull")],axis=1)

    # for k,r in cells_and_hulls.iterrows():

    #     hull=r.hull
    #     points = hull.points
    #     print(points)
    #     indices = hull.simplices
    #     poly_string = "POLYGON(("+", ".join([f"{p[0]} {p[1]}" for p in hull.points[np.concatenate([hull.vertices,[hull.vertices[0]]])]])+"))"
    #     r.cell.hull = poly_string
    #     session.add(r.cell)
    

    raw_sql = text("""
UPDATE umi 
SET xumi_xy=ST_SetSRID(ST_MakePoint(x, y),4326) 
FROM segment
WHERE segment.id = umi.seg
AND umi.dsid=:name;


UPDATE segment SET hull = ch.hull
FROM(
SELECT umi.seg as seg, ST_ConvexHull(ST_Collect(xumi_xy))  as hull
FROM umi JOIN segment ON segment.id= umi.seg
WHERE segment.n_umis>20 
AND segment.dsid=:name
GROUP BY umi.seg 
) as ch
WHERE ch.seg = segment.id
AND segment.dsid=:name;
""").params(name=dsid)
    session.execute(raw_sql)




    def array2raster(newRasterfn,rasterOrigin,pixelWidth,pixelHeight,array):

        cols = array.shape[1]
        rows = array.shape[0]
        originX = rasterOrigin[0]
        originY = rasterOrigin[1]

        driver = gdal.GetDriverByName('GTiff')
        outRaster = driver.Create(newRasterfn, cols, rows, 1, gdal.GDT_Byte)
        outRaster.SetGeoTransform((originX, pixelWidth, 0, originY, 0, pixelHeight))
        outband = outRaster.GetRasterBand(1)
        outband.WriteArray(array)
        outRasterSRS = osr.SpatialReference()
        outRasterSRS.ImportFromEPSG(4326)
        outRaster.SetProjection(outRasterSRS.ExportToWkt())
        outband.FlushCache()
        
    cells = sq(Segment).filter(Segment.dsid==dsname).filter(Segment.n_umis>20)     
    for i,c in enumerate(cells):
        #f c.id != 300228: continue
        #c0 = c
        print("SETTING")
        coords = np.array([(u.x,u.y) for u in c.umis]).T
        points_kde =  stats.gaussian_kde(coords)
        sf = 20
        r = max(c.eval0, c.eval1)*sf
        
        n = 50
        
        h = c.hull
        if type(h) == type("STR"):
            h = WKBElement(h)

    
        pts = to_shape(h).exterior.xy
        pts_df = pd.DataFrame(pts).T.rename({0:"x",1:"y"},axis="columns")    
        

        xrange=[pts_df.x.min(), pts_df.x.max()]
        yrange=[pts_df.y.min(), pts_df.y.max()]
        
        X,Y = np.meshgrid(np.linspace(xrange[0],xrange[1],n),np.linspace(yrange[0],yrange[1],n))
        grid =np.vstack([X.ravel(), Y.ravel()]).T
        vals = points_kde.evaluate(grid.T)
        vsquare = vals.reshape(n,n)

        array =( vsquare / np.max(vsquare) * 255).astype(int)
        reversed_arr = array[::-1] # reverse array so the tif looks like the array


        rasterOrigin = (xrange[0], yrange[0])
        pixelWidth = (xrange[1] - xrange[0]) / n
        pixelHeight = (yrange[1] - yrange[0]) / n
        
        newRasterfn = 'test.tif'
        array2raster(newRasterfn,rasterOrigin,pixelWidth,pixelHeight,array) # convert array to raster

        hexbits = open(newRasterfn,"rb").read().hex()
        sub = r"\x" + f"{hexbits}"
        query = f"""UPDATE segment SET kde_density=ST_FromGDALRaster('{sub}') WHERE id={c.id}"""
        session.execute(query)
    


    session.commit()

    raw_sql = text("""

UPDATE umi
SET kde_val= ST_NearestValue(kde_density, ST_SetSRID(umi.xumi_xy,4326 ))
FROM segment 
WHERE segment.id = umi.seg
AND umi.dsid=:name;

UPDATE segment SET hull1 = ch.hull
FROM(
SELECT umi.seg as seg, ST_ConvexHull(ST_Collect(xumi_xy))  as hull
FROM umi JOIN segment ON segment.id= umi.seg
WHERE segment.kde_density is not null 
AND umi.kde_val >= 1
AND segment.dsid=:name
GROUP BY umi.seg 
) as ch
WHERE ch.seg = segment.id
AND segment.dsid=:name;

UPDATE segment SET hull128 = ch.hull
FROM(
SELECT umi.seg as seg, ST_ConvexHull(ST_Collect(xumi_xy))  as hull
FROM umi JOIN segment ON segment.id= umi.seg
WHERE segment.kde_density is not null 
AND umi.kde_val >= 128
AND segment.dsid=:name
GROUP BY umi.seg 
) as ch
WHERE ch.seg = segment.id
AND segment.dsid=:name;

UPDATE segment SET hull12 = ch.hull
FROM(
SELECT umi.seg as seg, ST_ConvexHull(ST_Collect(xumi_xy))  as hull
FROM umi JOIN segment ON segment.id= umi.seg
WHERE segment.kde_density is not null 
AND umi.kde_val >= 12
AND segment.dsid=:name
GROUP BY umi.seg 
) as ch
WHERE ch.seg = segment.id
AND segment.dsid=:name;

UPDATE segment SET center = ch.centroid
FROM(
SELECT umi.seg as seg, ST_Centroid(ST_Collect(xumi_xy))  as centroid
FROM umi JOIN segment ON segment.id= umi.seg
WHERE segment.kde_density is not null 
AND segment.dsid=:name
GROUP BY umi.seg 
) as ch
WHERE ch.seg = segment.id
AND segment.dsid=:name;

UPDATE segment SET area12 = ST_Area(hull12);


update segment set rval=(5915587277.0*id)%255/256 WHERE segment.dsid=:name;
update segment set gval=(5915587277.0*id)/256%255/256 WHERE segment.dsid=:name;
update segment set bval=(5915587277.0*id)/256/256%255/256 WHERE segment.dsid=:name;



""").params(name=dsid)
    session.execute(raw_sql)

    raw_sql2 = text("""

UPDATE dataset SET raster_2k_red = ST_MapAlgebra(
    ST_AddBand(
        ST_MakeEmptyRaster(2000,2000, -20, -20, .02, .02, 0, 0, 4326),
        1, '8BUI'::text, 5, 0), 
    images.union_img, '[rast1]*0+[rast2]', null, 'UNION')
FROM (
    SELECT rs.dsid as dsid, ST_Union(resampled, 'SUM' ) as union_img
        FROM  (
            SELECT  ST_MapAlgebra(
                ST_Resample(kde_density, ST_MakeEmptyRaster(2000,2000, -20, -20, .02, .02, 0, 0, 4326),'Bilinear'),
                1,null,FORMAT('[rast] * %s ',segment.rval)
            )  AS resampled,
            dsid
        FROM segment 
        WHERE segment.kde_density IS NOT null
        AND segment.dsid=:name
            )  AS rs
        GROUP BY rs.dsid
) AS images
WHERE images.dsid = dataset.id
AND dataset.id=:name;



UPDATE dataset SET raster_2k_green = ST_MapAlgebra(
    ST_AddBand(
        ST_MakeEmptyRaster(2000,2000, -20, -20, .02, .02, 0, 0, 4326),
        1, '8BUI'::text, 5, 0), 
    images.union_img, '[rast1]*0+[rast2]', null, 'UNION')
FROM (
    SELECT rs.dsid as dsid, ST_Union(resampled, 'SUM'  ) as union_img
        FROM  (
            SELECT  ST_MapAlgebra(
ST_Resample(kde_density, ST_MakeEmptyRaster(2000,2000, -20, -20, .02, .02, 0, 0, 4326),'Bilinear'),
                1,null,FORMAT('[rast] * %s ',segment.gval)
            )  AS resampled,
            dsid
        FROM segment 
        WHERE segment.kde_density IS NOT null
        AND segment.dsid=:name
            )  AS rs
        GROUP BY rs.dsid
) AS images
WHERE images.dsid = dataset.id
AND dataset.id=:name;





UPDATE dataset SET raster_2k_blue = ST_MapAlgebra(
    ST_AddBand(
        ST_MakeEmptyRaster(2000,2000, -20, -20, .02, .02, 0, 0, 4326),
        1, '8BUI'::text, 5, 0), 
    images.union_img, '[rast1]*0+[rast2]', null, 'UNION')
FROM (
    SELECT rs.dsid as dsid, ST_Union(resampled, 'SUM'  ) as union_img
        FROM  (
            SELECT  ST_MapAlgebra(
ST_Resample(kde_density, ST_MakeEmptyRaster(2000,2000, -20, -20, .02, .02, 0, 0, 4326),'Bilinear'),
                1,null,FORMAT('[rast] * %s ',segment.bval)
            )  AS resampled,
            dsid
        FROM segment 
        WHERE segment.kde_density IS NOT null
        AND segment.dsid=:name
            )  AS rs
        GROUP BY rs.dsid
) AS images
WHERE images.dsid = dataset.id
AND dataset.id=:name;





UPDATE dataset SET raster_2k_all = 
       ST_AddBand( ST_AddBand( 
            raster_2k_red,raster_2k_green), raster_2k_blue)
            WHERE dataset.id=:name;

""").params(name=dsid)
    session.execute(raw_sql2)

    raw_sql3 = text("""

UPDATE segment 
SET points = sq.new_geo
FROM (
    SELECT segment.id as seg_id, ST_Collect(umi.xumi_xy) as new_geo
    FROM segment JOIN umi ON umi.seg = segment.id
    WHERE segment.dsid=:name
    GROUP BY segment.id
)  AS sq
WHERE segment.id=sq.seg_id;

UPDATE segment 
SET points_xym_total_reads = sq.new_geo
FROM (
    SELECT segment.id AS seg_id, ST_Collect(ST_MakePointM(ST_X(umi.xumi_xy),ST_Y(umi.xumi_xy),umi.total_reads)) as new_geo
    FROM segment JOIN umi ON umi.seg = segment.id
    WHERE segment.dsid=:name
    GROUP BY segment.id
) AS sq
WHERE segment.id = sq.seg_id;

UPDATE segment 
SET points_xym_kde = sq.new_geo
FROM (
    SELECT segment.id AS seg_id, ST_Collect(ST_MakePointM(ST_X(umi.xumi_xy),ST_Y(umi.xumi_xy),umi.kde_val)) as new_geo
    FROM segment JOIN umi ON umi.seg = segment.id
    WHERE segment.dsid=:name
    GROUP BY segment.id
) AS sq
WHERE segment.id = sq.seg_id;

""").params(name=dsid)
    session.execute(raw_sql3)

    raw_sql4 = text("""

UPDATE segment 
SET points12 = sq.new_geo
FROM (
    SELECT segment.id as seg_id, ST_Collect(umi.xumi_xy) as new_geo
    FROM segment JOIN umi ON umi.seg = segment.id
    WHERE umi.kde_val > 12
    AND segment.dsid=:name
    GROUP BY segment.id
)  AS sq
WHERE segment.id=sq.seg_id;



UPDATE segment 
SET points128 = sq.new_geo
FROM (
    SELECT segment.id as seg_id, ST_Collect(umi.xumi_xy) as new_geo
    FROM segment JOIN umi ON umi.seg = segment.id
    WHERE umi.kde_val > 128
    AND segment.dsid=:name
    GROUP BY segment.id
)  AS sq
WHERE segment.id=sq.seg_id;


""").params(name=dsid)
    session.execute(raw_sql4)

    session.commit()


    def alpha_shape(points, alpha):
        """
        Compute the alpha shape (concave hull) of a set
        of points.
        @param points: Iterable container of points.
        @param alpha: alpha value to influence the
            gooeyness of the border. Smaller numbers
            don't fall inward as much as larger numbers.
            Too large, and you lose everything!
        """
        if len(points) < 4:
            # When you have a triangle, there is no sense
            # in computing an alpha shape.
            return geometry.MultiPoint(list(points)).convex_hull

        coords = np.array([point.coords[0] for point in points])
        tri = Delaunay(coords)
        triangles = coords[tri.vertices]
        a = ((triangles[:,0,0] - triangles[:,1,0]) ** 2 + (triangles[:,0,1] - triangles[:,1,1]) ** 2) ** 0.5
        b = ((triangles[:,1,0] - triangles[:,2,0]) ** 2 + (triangles[:,1,1] - triangles[:,2,1]) ** 2) ** 0.5
        c = ((triangles[:,2,0] - triangles[:,0,0]) ** 2 + (triangles[:,2,1] - triangles[:,0,1]) ** 2) ** 0.5
        s = ( a + b + c ) / 2.0
        areas = (s*(s-a)*(s-b)*(s-c)) ** 0.5
        circums = a * b * c / (4.0 * areas)
        filtered = triangles[circums < (1.0 / alpha)]
        edge1 = filtered[:,(0,1)]
        edge2 = filtered[:,(1,2)]
        edge3 = filtered[:,(2,0)]
        edge_points = np.unique(np.concatenate((edge1,edge2,edge3)), axis = 0).tolist()
        m = geometry.MultiLineString(edge_points)
        triangles = list(polygonize(m))
        return cascaded_union(triangles), edge_points



    for i,c in enumerate(sq(Segment).filter(Segment.dsid== dsid).filter(Segment.n_umis>20).all()):

        shp = to_shape(c.points)
        alpha = .1/np.mean(np.var(np.array([p.coords[0] for p in shp]).T,1))**.5
        if i %20 == 0:
            print(i,alpha)
            
        concave_hull, edge_points = alpha_shape(shp, alpha=alpha)
        if concave_hull.geom_type == 'MultiPolygon':
            concave_hull = max(concave_hull, key=lambda a: a.area)

        wkb_element = from_shape(concave_hull)
        c.hull = wkb_element

        
        shp = to_shape(c.points12)
        alpha = .5/np.mean(np.var(np.array([p.coords[0] for p in shp]).T,1))**.5
        concave_hull, edge_points = alpha_shape(shp, alpha=alpha)
        if concave_hull.geom_type == 'MultiPolygon':
            concave_hull = max(concave_hull, key=lambda a: a.area)

        wkb_element = from_shape(concave_hull)
        c.hull12 = wkb_element
            
        shp = to_shape(c.points128)
        alpha = .5/np.mean(np.var(np.array([p.coords[0] for p in shp]).T,1))**.5
        concave_hull, edge_points = alpha_shape(shp, alpha=alpha)
        if concave_hull.geom_type == 'MultiPolygon':
            concave_hull = max(concave_hull, key=lambda a: a.area)

        wkb_element = from_shape(concave_hull)
        c.hull128 = wkb_element
        
        session.add(c)
    #pTODO] PUT BACK IN COMMITS WHEN THIS CODE IS READY TO RUN!
    #session.commit()










    return 0


    