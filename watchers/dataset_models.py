
from sqlalchemy import create_engine , cast, Index, func
from sqlalchemy import Column, String, Integer, Float, ForeignKey, Boolean, ForeignKeyConstraint, BigInteger
from geoalchemy2 import Geometry, Raster

from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship
from sqlalchemy.ext.compiler import compiles

from sqlalchemy import types
from sqlalchemy.dialects import postgresql

#from sqlalchemy.ext.declarative import declarative_base  
#from sqlalchemy_repr import RepresentableBase

from flask_sqlalchemy import SQLAlchemy
db2 = SQLAlchemy()

def create_tsvector(*args):
    exp = args[0]
    for e in args[1:]:
        exp += ' ' + e
    return func.to_tsvector('english', exp)

#db_string = "postgres://ben_coolship_io:password@localhost/dd"
#db = create_engine(db_string)  
#Session = sessionmaker(db)  
#session = Session()


class Dataset(db2.Model):
    __tablename__ = 'dataset'
    id = Column(Integer, primary_key = True)
    name = Column(String)
    umis = relationship("Umi", cascade ="all,delete",backref="dataset")
    segments = relationship("Segment", cascade = "all,delete", backref="dataset")

    
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

    umis = relationship("Umi", cascade="all,delete" ,backref="segment")

    
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
                #one of 2 primary keys, assigned from dataset
    idx = Column(Integer, index = True)
                #second primary key, assigned according to the row in the original file
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

    hull = Column(Geometry('POLYGON'))
    center = Column(Geometry('POINT'))
    kde_density = Column(Raster)

    __table_args__ = (
    Index('idx_seq_tgm', "sequence",
              postgresql_ops={"sequence": "gin_trgm_ops"},
              postgresql_using='gin'),
              )


class GeoEdge(db2.Model):
    __tablename__ = "geoedge"
    id = Column(BigInteger, primary_key = True)
    target_id = Column(Integer, ForeignKey("umi.id", ondelete="CASCADE"), index = True)
    beacon_id = Column(Integer, ForeignKey("umi.id", ondelete="CASCADE"), index = True)
    n_uei = Column(Integer)
    

