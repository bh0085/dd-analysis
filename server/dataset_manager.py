import os
import pandas as pd
from sqlalchemy import create_engine  
from sqlalchemy import Column, String, Integer, Float, ForeignKey, ForeignKeyConstraint
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker

db_string = "postgres://ben_coolship_io:password@localhost/dd"
db = create_engine(db_string)  
base = declarative_base()
Session = sessionmaker(db)  
session = Session()

class Dataset(base):
    __tablename__ = 'dataset'
    id = Column(Integer, primary_key = True)
    
class Umi(base):  
    __tablename__ = 'umi'
    ds = Column(Integer, ForeignKey(Dataset.id), index = True, primary_key = True)
    idx = Column(Integer, index = True, primary_key = True)
    x = Column(Float)
    y = Column(Float)

class UmiGoTerm(base):
    __tablename__ ="umigoterm"
    id = Column(Integer, primary_key = True)
    umi_ds = Column(Integer)
    umi_idx = Column(Integer)
    __table_args__ = (ForeignKeyConstraint([umi_ds, umi_idx],
                                           [Umi.ds, Umi.idx]),
                      {})    
    
    go_name = Column(String, nullable = False)
    go_id = Column(String, nullable = False)
    
class UmiGeneId(base):
    __tablename__ = "umigeneid"
    id = Column(Integer, primary_key = True)
    umi_ds = Column(Integer)
    umi_idx = Column(Integer)
    __table_args__ = (ForeignKeyConstraint([umi_ds, umi_idx],
                                           [Umi.ds, Umi.idx]),
                      {})
    
    gene_id = Column(String, nullable = False)
    
meta = base.metadata

def enter_all_datasets():
    datasets = os.listdir(DATASETS_ROOT)
    datasets = ["6989106579746251"]
    for d in datasets:
        enter_dataset(d)
def enter_data(d):
    DATASETS_ROOT = "/data/dd-analysis/datasets"
    
    datasets = 
    dataframes = {}
    for d in datasets:
        dfs = {}
        print("loading data for {}".format(d))

        print("TODO : PRE FILTER NAs AND GET INDEXING WORKING PROPERLY")
        umis2go_path = os.path.join(DATASETS_ROOT, d,"goterms/umis2go.csv")
        umis2geneids_path = os.path.join(DATASETS_ROOT, d,"genesymbols/umi_symbols.csv")
        umis2seg_path = os.path.join(DATASETS_ROOT, d,"segments/umi2seg.csv")

        

        dfs["umis2go"] = pd.read_csv(umis2go_path).dropna()
        dfs["umis2geneids"] = pd.read_csv(umis2geneids_path).dropna()#.set_index("umi")
        dfs["umi2seg"] = pd.read_csv(umis2seg_path).dropna()


        #dfs["go2umis"] = pd.read_csv(os.path.join(DATASETS_ROOT, d,"goterms/umis2go.csv")).dropna()#.set_index("GO_NAME")
        #dfs["cells2go"] = pd.read_csv(os.path.join(DATASETS_ROOT, d,"goterms/segments2go.csv")).dropna()#.set_index("segment")
        #dfs["go2cells"] = pd.read_csv(os.path.join(DATASETS_ROOT, d,"goterms/segments2go.csv")).dropna()#.set_index("GO_NAME")
        #dfs["cells2geneids"] = pd.read_csv(os.path.join(DATASETS_ROOT, d,"genesymbols/segment_symbols.csv")).dropna()#.set_index("segment")
        
        dataframes[d] = dfs

def main():

    import argparse

    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--reset', dest='reset', action='store_const',
                        const=True, default=False,
                        help='drop database tables and all content')

    
    parser.add_argument('--load-all', dest='reset', action='store_const',
                        const=True, default=False,
                        help='drop database tables and all content')

    args = parser.parse_args()

    if args.reset:
        meta.drop_all(db)
        meta.create_all(db)


if __name__ =="__main__":
    main()