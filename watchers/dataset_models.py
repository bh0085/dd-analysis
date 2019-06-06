
from sqlalchemy import create_engine  
from sqlalchemy import Column, String, Integer, Float, ForeignKey, ForeignKeyConstraint
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship

db_string = "postgres://ben_coolship_io:password@localhost/dd"
db = create_engine(db_string)  
base = declarative_base(bind = db)
Session = sessionmaker(db)  
session = Session()


class Dataset(base):
    __tablename__ = 'dataset'
    id = Column(Integer, primary_key = True)
    name = Column(String)
    umis = relationship("Umi", cascade ="all,delete",backref="dataset")
    
class Umi(base):  
    __tablename__ = 'umi'
    dsid = Column(Integer, ForeignKey("dataset.id", ondelete="CASCADE"), index = True, primary_key = True,)
                #one of 2 primary keys, assigned from dataset
    idx = Column(Integer, index = True, primary_key = True,) 
                #second primary key, assigned according to the row in the original file
    x = Column(Float)
    y = Column(Float)
    seg20 = Column(Integer)
    seg = Column(Integer, ForeignKey("segment.id", ondelete="CASCADE"),index = True)
    molecule_type = Column(Integer)
    sequence = Column(String)
    goterms = relationship("UmiGoTerm", cascade="all,delete", backref="umi")
    geneids = relationship("UmiGeneId", cascade="all,delete", backref="umi")
    
class Segment(base):
    __tablename__ = "segment"
    id = Column(Integer, primary_key = True, autoincrement = True)
    dsid = Column(Integer, ForeignKey("dataset.id", ondelete="CASCADE"), index = True)
    og_segid = Column(Integer)
    umis = relationship("Umi", cascade="all,delete" ,backref="segment")

class UmiGoTerm(base):
    __tablename__ ="umigoterm"
    id = Column(Integer, primary_key = True, autoincrement = True)
    dsid = Column(Integer)
    umi_idx = Column(Integer)
    __table_args__ = (ForeignKeyConstraint([dsid, umi_idx],
                                           [Umi.dsid, Umi.idx], ondelete="CASCADE"),
                      {})    
    
    
    go_name = Column(String, nullable = False)
    go_id = Column(String, nullable = False)
    
class UmiGeneId(base):
    __tablename__ = "umigeneid"
    id = Column(Integer, primary_key = True, autoincrement = True)
    dsid = Column(Integer,index = True)
    umi_idx = Column(Integer, index = True)
    ncbi_geneid = Column(String, ForeignKey("ncbigene.geneid", ondelete="CASCADE"),index = True) 
    ensembl_geneid = Column(String, ForeignKey("ensemblgene.geneid", ondelete="CASCADE"),index = True) 

        
    __table_args__ = (ForeignKeyConstraint([dsid, umi_idx],
                                           [Umi.dsid, Umi.idx], ondelete="CASCADE"),
                      {})
class EnsemblGene(base):
    __tablename__= "ensemblgene"
    geneid = Column(String,primary_key = True)
    umis = relationship("UmiGeneId", cascade ="all,delete",backref="ens_genes")

class NcbiGene(base):
    __tablename__= "ncbigene"
    geneid = Column(String,primary_key=True)
    symbol = Column(String,index = True)
    desc = Column(String)
    umis = relationship("UmiGeneId", cascade ="all,delete",backref="ncbi_genes")

meta = base.metadata