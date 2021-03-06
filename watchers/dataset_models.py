
from sqlalchemy import create_engine , cast, Index, func
from sqlalchemy import Column, String, Integer, Numeric, Float, ForeignKey, Boolean, ForeignKeyConstraint, BigInteger
from geoalchemy2 import Geometry, Raster
import pandas as pd

from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship
from sqlalchemy.ext.compiler import compiles
import numpy as np

from geoalchemy2.functions import ST_Crosses, ST_AsGeoJSON

from sqlalchemy import types
from sqlalchemy.dialects import postgresql

#from sqlalchemy.ext.declarative import declarative_base  
#from sqlalchemy_repr import RepresentableBase

from flask_sqlalchemy import SQLAlchemy
db2 = SQLAlchemy()
from tifffile import imread 
import io

def create_tsvector(*args):
    exp = args[0]
    for e in args[1:]:
        exp += ' ' + e
    return func.to_tsvector('english', exp)

# db_string = "postgres://ben_coolship_io:password@localhost/dd"
# db = create_engine(db_string)  
# Session = sessionmaker(db)  
# session = Session()


class Dataset(db2.Model):
    __tablename__ = 'dataset'
    id = Column(Integer, primary_key = True)
    name = Column(String)
    umis = relationship("Umi", cascade ="all,delete",backref="dataset")
    segments = relationship("Segment", cascade = "all,delete", backref="dataset")

    raster_2k_all = Column(Raster)
    raster_2k_red = Column(Raster)
    raster_2k_blue = Column(Raster)
    raster_2k_green = Column(Raster)

    raster_400_all = Column(Raster)
    raster_400_red = Column(Raster)
    raster_400_blue = Column(Raster)
    raster_400_green = Column(Raster)
    
    
class UmiGeneId(db2.Model):
    __tablename__ = "umigeneid"
    id = Column(Integer, primary_key = True, autoincrement = True)
    #dsid = Column(Integer,index = True)
    umi_id = Column(Integer, ForeignKey("umi.id", ondelete="CASCADE"), index = True )

    ncbi_geneid = Column(Integer, ForeignKey("ncbigene.geneid", ondelete="CASCADE"),index = True) 
    ensembl_geneid = Column(String, ForeignKey("ensemblgene.geneid", ondelete="CASCADE"),index = True) 

class GeneGoTerm(db2.Model):
    __tablename__= "genegoterm"
    id = Column(Integer, primary_key = True, autoincrement=True)
    ncbi_gene= Column(Integer, ForeignKey("ncbigene.geneid", ondelete="CASCADE"))
    go_id = Column(String, ForeignKey("goterm.go_id", ondelete="CASCADE"))

class EnsemblGene(db2.Model):
    __tablename__= "ensemblgene"
    geneid = Column(String,primary_key = True)

class NcbiGene(db2.Model):
    __tablename__= "ncbigene"
    geneid = Column(Integer,primary_key=True)
    symbol = Column(String,index = True)
    desc = Column(String, index = True)

    __ts_vector__ = create_tsvector(
        cast(func.coalesce(desc, ''), postgresql.TEXT)
    )

    __table_args__ = (
        Index(
            'idx_desc_tsv',
            __ts_vector__,
            postgresql_using='gin'
        ),

        Index('idx_desc_tgm', "desc",
              postgresql_ops={"desc": "gin_trgm_ops"},
              postgresql_using='gin'),
    )

class GoTerm(db2.Model):
    __tablename__= "goterm"
    go_id = Column(String, primary_key=True)
    go_name = Column(String, index = True)

    
class Umi(db2.Model):  
    __tablename__ = 'umi'

    id = Column(Integer, primary_key = True, autoincrement = True)
    dsid = Column(Integer, ForeignKey("dataset.id", ondelete="CASCADE"), index = True)
    idx = Column(Integer, index = True)

    x = Column(Float)
    y = Column(Float)

    umap_x = Column(Float)
    umap_y = Column(Float)
    umap_z = Column(Float)

    is_aligned_to_intron = Column(Boolean)
    is_aligned_to_exon= Column(Boolean)

    seg = Column(Integer, ForeignKey("segment.id", ondelete="CASCADE"),index = True)    
    molecule_type = Column(Integer)
    sequence = Column(String)
    total_reads = Column(Integer)

    xumi_xy = Column(Geometry('POINT'))
    umap_xyz = Column(Geometry('POINTZ'))
    kde_val = Column(Float)

    __table_args__ = (
    Index('idx_seq_tgm', "sequence",
              postgresql_ops={"sequence": "gin_trgm_ops"},
              postgresql_using='gin'),
              )

    genes = relationship("UmiGeneId", cascade="all,delete" ,backref="umi")

    def as_dict(self):
        return {
            "x": self.x,
            "y": self.y,
            "seg": self.seg,
            "dsid":self.dsid,
            "sequence": self.sequence,
            "total_reads":self.total_reads,
            "molecule_type":self.molecule_type,
            "is_aligned_to_intron":self.is_aligned_to_intron,
            "is_aligned_to_exon": self.is_aligned_to_exon,
            "id":self.id,
        }
import json

    
from geoalchemy2 import WKBElement, Raster
from sqlalchemy.orm import  column_property
from sqlalchemy import select , text

getDatasetId =  lambda dsname: int(dsname[:8])
color_factor = 5915587277



class Segment(db2.Model):
    __tablename__ = "segment"
    id = Column(Integer, primary_key = True, autoincrement = True)
    dsid = Column(Integer, ForeignKey("dataset.id", ondelete="CASCADE"), index = True)
    og_segid = Column(Integer)

    eval0 = Column(Float)
    eval1 = Column(Float)

    evec0x = Column(Float)
    evec0y = Column(Float)
    evec1x = Column(Float)
    evec1y = Column(Float)

    meanx = Column(Float, index = True)
    meany = Column(Float, index = True)
    n_umis = Column(Integer)

    hull = Column(Geometry('POLYGON'))
    hull1 = Column(Geometry('POLYGON'))
    hull12 = Column(Geometry('POLYGON'))
    hull128 = Column(Geometry('POLYGON'))

    rval = Column(Float)
    gval = Column(Float)
    bval = Column(Float)

    area12 = Column(Float)
    center = Column(Geometry('POINT'))
    kde_density = Column(Raster)

    points = Column(Geometry('MULTIPOINT'))
    points12 = Column(Geometry('MULTIPOINT'))
    points128 = Column(Geometry('MULTIPOINT'))
    points_xym_total_reads = Column(Geometry('MULTIPOINTM'))
    points_xym_kde = Column(Geometry('MULTIPOINTM'))

    umi_count = column_property(
        select([func.count(Umi.id)]).\
            where(Umi.seg==id).\
            correlate_except(Umi)
    )

    def as_frontend_dict(self):
        #return({"x":"hi"})

        #for k,v in out.items():
        #    if type(v) = type(np.array([])):
        #        out[k] = list(v)
        return {k:getattr(self,k) 
        for k in ["meanx","meany","id", "rgb", "xy", "area12",
        #"kde_array_5"#, "umis_top_100_total_reads",
        ]} #      "kde_array_100", "hull_xy",
        #for k in ["meanx","meany","id", "rgb", "xy"]}
                         # ]})


    def as_custom_frontend_dict(self,attrs):
        out = {k:getattr(self,k) for k in attrs}
        return out 

    def as_detailed_frontend_dict(self):
        #return({"x":"hi"})

        out = {k:getattr(self,k) for k in ["meanx","meany","id", "rgb", "xy","points12_xy"]}#,"hull12", "kde_array_100"]}
        # for k,v in out.items():
        #     if type(v) == type(np.array([])):
        #         out[k] = v.tolist()
        return out
        
    
    @property
    def intersecting_cells(self):
        from db import session
        return session.query(Segment).filter(Segment.hull.ST_Intersects(self.hull)).all()


    @property
    def rgb(self):
        return [int(256 * self.rval), int(256 * self.gval),int(256*self.bval)]

    @property
    def xy(self):
        return [self.meanx, self.meany]
    
    
    @property
    def kde_obj(self):
        return Raster(self.kde_density) if type(self.kde_density)==type("STR") else self.kde_density

    @property
    def hull_obj(self):
        return WKBElement(self.hull) if type(self.hull)==type("STR") else self.hull

    @property
    def hull12_obj(self):
        return WKBElement(self.hull12) if type(self.hull12)==type("STR") else self.hull12

    @property
    def hull128_obj(self):
        return WKBElement(self.hull128) if type(self.hull128)==type("STR") else self.hull128


    kde_tiff_100 = column_property( 
        select([text("ST_AsTIFF(ST_Resize(kde_density,100,100))")]))

    kde_tiff_20 = column_property(
        select([text("ST_AsTIFF(ST_Resize(kde_density,20,20))")]))
    
    kde_tiff_5 = column_property(
        select([text("ST_AsTIFF(ST_Resize(kde_density,5,5))")]))
    
    
    umis_top_100_total_reads = column_property(
    select([text(""" st_asgeojson(ST_Collect(pts.pt),3)
FROM(
SELECT    (g.gdump).geom as pt
     FROM
    (
        SELECT ST_DumpPoints(segment.points_xym_kde ) AS gdump
        FROM segment WHERE  id=id
    ) AS g
    ORDER BY ST_M((g.gdump).geom) DESC
    LIMIT 10
) AS pts
""")]))

    
    
    umis_top_100_kde = column_property(
    select([text(""" st_asgeojson(ST_Collect(pts.pt),3)
FROM(
SELECT    (g.gdump).geom as pt
     FROM
    (
        SELECT ST_DumpPoints(segment.points_xym_total_reads ) AS gdump
        FROM segment WHERE  id=id
    ) AS g
    ORDER BY ST_M((g.gdump).geom) DESC
    LIMIT 20
) AS pts
""")]))



    @property
    def hull_shape(self):
        return to_shape(self.hull_obj)

    @property
    def hull_xy(self):
        return  [[float(x),float( y)] for x, y in zip(*self.hull_shape.exterior.xy)]
            
    @property
    def hull12_xy(self):
        pts = to_shape(self.hull12_obj).exterior.xy
        pts_df = pd.DataFrame(pts).T.rename({0:"x",1:"y"},axis="columns")    
        return pts_df.values.astype(float).tolist()

    @property
    def hull128_xy(self):
        pts = to_shape(self.hull128_obj).exterior.xy
        pts_df = pd.DataFrame(pts).T.rename({0:"x",1:"y"},axis="columns")    
        return pts_df.values.astype(float).tolist()

    # @property
    # def points_xy(self):
    #     #return (lambda x: np.reshape(np.array(x["data"]),x["shape"]))(to_shape(self.points).array_interface()).astype(float).tolist()
        # return json.loads(sq(ST_AsGeoJSON(Segment.points)).filter(Segment.id.in_(ids)).one()[0])['coordinates']
    
    points_xy_geojson = column_property(
        select([text("ST_AsGeoJSON(points, 2)")])
    )

    points12_xy_geojson = column_property(
        select([text("ST_AsGeoJSON(points12, 2)")])
    )

    points128_xy_geojson = column_property(
        select([text("ST_AsGeoJSON(points128, 2)")])
    )

    @property
    def points_xy(self):
        return json.loads(self.points_xy_geojson)["coordinates"]

    @property
    def points12_xy(self):
        return json.loads(self.points12_xy_geojson)["coordinates"]

    @property
    def points128_xy(self):
        return json.loads(self.points128_xy_geojson)["coordinates"]


    @property
    def points12_xy(self):
        return (lambda x: np.reshape(np.array(x["data"]),x["shape"]))(to_shape(self.points12).array_interface()).astype(float).tolist()


    @property
    def points128_xy(self):
        return (lambda x: np.reshape(np.array(x["data"]),x["shape"]))(to_shape(self.points128).array_interface()).astype(float).tolist()


    @property
    def kde_array_100(self):
        return   imread(io.BytesIO(self.kde_tiff_100.tobytes())).astype(int).tolist()


    @property
    def kde_array_20(self):
        return   imread(io.BytesIO(self.kde_tiff_20.tobytes())).astype(int).tolist()

    @property
    def kde_array_5(self):
        return   imread(io.BytesIO(self.kde_tiff_5.tobytes())).astype(int).tolist()

    umis = relationship("Umi", cascade="all,delete" ,backref="segment")

from geoalchemy2.shape import to_shape

class GeoEdge(db2.Model):
    __tablename__ = "geoedge"
    id = Column(BigInteger, primary_key = True)
    target_id = Column(Integer, ForeignKey("umi.id", ondelete="CASCADE"), index = True)
    beacon_id = Column(Integer, ForeignKey("umi.id", ondelete="CASCADE"), index = True)
    n_uei = Column(Integer)
    

