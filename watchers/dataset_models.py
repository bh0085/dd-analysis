
from sqlalchemy import create_engine , cast, Index, func
from sqlalchemy import Column, String, Integer, Float, ForeignKey, ForeignKeyConstraint
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship
from sqlalchemy.ext.compiler import compiles
from sqlalchemy_repr import RepresentableBase
from sqlalchemy import types
from sqlalchemy.dialects import postgresql

def create_tsvector(*args):
    exp = args[0]
    for e in args[1:]:
        exp += ' ' + e
    return func.to_tsvector('english', exp)

db_string = "postgres://ben_coolship_io:password@localhost/dd"
db = create_engine(db_string)  
base = declarative_base(bind = db,cls=RepresentableBase)
Session = sessionmaker(db)  
session = Session()


class Dataset(base):
    __tablename__ = 'dataset'
    id = Column(Integer, primary_key = True)
    name = Column(String)
    umis = relationship("Umi", cascade ="all,delete",backref="dataset")
    segments = relationship("Segment", cascade = "all,delete", backref="dataset")

    
class Segment(base):
    __tablename__ = "segment"
    id = Column(Integer, primary_key = True, autoincrement = True)
    dsid = Column(Integer, ForeignKey("dataset.id", ondelete="CASCADE"), index = True)
    og_segid = Column(Integer)
    umis = relationship("Umi", cascade="all,delete" ,backref="segment")
    
class UmiGeneId(base):
    __tablename__ = "umigeneid"
    id = Column(Integer, primary_key = True, autoincrement = True)
    #dsid = Column(Integer,index = True)
    umi_id = Column(Integer, ForeignKey("umi.id"), index = True )
    ncbi_geneid = Column(Integer, ForeignKey("ncbigene.geneid", ondelete="CASCADE"),index = True) 
    ensembl_geneid = Column(String, ForeignKey("ensemblgene.geneid", ondelete="CASCADE"),index = True) 

class GeneGoTerm(base):
    __tablename__= "genegoterm"
    id = Column(Integer, primary_key = True, autoincrement=True)
    ncbi_gene= Column(Integer, ForeignKey("ncbigene.geneid"))
    go_id = Column(String, ForeignKey("goterm.go_id"))

class EnsemblGene(base):
    __tablename__= "ensemblgene"
    geneid = Column(String,primary_key = True)

class NcbiGene(base):
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

class GoTerm(base):
    __tablename__= "goterm"
    go_id = Column(String, primary_key=True)
    go_name = Column(String, index = True)

    
class Umi(base):  
    __tablename__ = 'umi'

    id = Column(Integer, primary_key = True, autoincrement = True)
    dsid = Column(Integer, ForeignKey("dataset.id", ondelete="CASCADE"), index = True)
                #one of 2 primary keys, assigned from dataset
    idx = Column(Integer, index = True)
                #second primary key, assigned according to the row in the original file
    x = Column(Float)
    y = Column(Float)
    seg20 = Column(Integer)
    seg = Column(Integer, ForeignKey("segment.id", ondelete="CASCADE"),index = True)
    molecule_type = Column(Integer)
    sequence = Column(String)

    __table_args__ = (
    Index('idx_seq_tgm', "sequence",
              postgresql_ops={"sequence": "gin_trgm_ops"},
              postgresql_using='gin'),
              )
              

    # goterms = relationship("GoTerm", 
    # cascade="all,delete", 
    # secondary="umigoterm",
    # backref= "umis")
    # genes = relationship("NcbiGene", 
    #     cascade="all,delete",
    #     secondary="umigeneid",
    #     backref = "umis")


meta = base.metadata