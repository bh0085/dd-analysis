import pysam
import os
import pandas as pd
from Bio import SeqIO
import pandas as pd
import re
import numpy as np
from goatools.go_enrichment import GOEnrichmentStudy
from goatools import obo_parser

import wget

DATA_DIR = "/data/dd-analysis"

#LOAD DATABASE ANNOTATIONS
refseq_genes = pd.read_csv("/data/genomes/annotations/refseq_genes_export.csv", delimiter="\t")
entrez = pd.read_csv("/data/genomes/annotations/Homo_sapiens.GRCh38.95.entrez.tsv",delimiter = "\t")
refseq_xref = pd.read_csv("/data/genomes/annotations/Homo_sapiens.GRCh38.95.refseq.tsv",delimiter = "\t")


#LOAD GENE ONTOLOGIES FOR HG38
from Bio.UniProt import GOA
fopen = open("/data/genomes/annotations/goa_human.gaf")
itr = GOA.gafiterator(fopen)
records = list(itr)
ontologies = pd.DataFrame.from_dict(records)    

def init_go_terms(tmpfolder, dataset):
    dsname = dataset["dataset"]
    
    #READ TRANSCRIPT ALIGNMENTS
    samfile = pysam.AlignmentFile("/data/dd-analysis/datasets/{}/tophat/accepted_hits.bam".format(dsname), "rb")
    all_alignments = [a for a in  samfile]
    names =[e.reference_name for e in all_alignments]
    query_names = [e.query_name for e in all_alignments]
    
    #LOAD TRANSCRIPTOME FASTA FILE
    #this is the file that the reads above were aligned to and contains add'l annotations
    with open("/data/transcriptomes/Homo_sapiens.GRCh38.cdna.all.fa") as f:
        farecords = SeqIO.parse(f, "fasta")
        recs = [r for r in farecords]
    for r in recs:
        r.id = r.id[:r.id.index(".")]
    

        
    gsre = re.compile("gene_symbol:(\S+)")
    dre = re.compile("description:(.*)")
    chromre = re.compile("chromosome:(\S+)")
    genere = re.compile("gene:(\S+)")

    transcript_gene_symbols = dict([(r.id, gsre.search(r.description).groups()[0]) for r in recs if "gene_symbol:" in r.description])
    transcript_descriptions = dict([(r.id, dre.search(r.description).groups()[0]) for r in recs if "description:" in r.description])
    transcript_chromosome = dict([(r.id, chromre.search(r.description).groups()[0]) for r in recs if "chromosome:" in r.description])
    transcript_geneid_withversion = dict([(r.id, genere.search(r.description).groups()[0]) for r in recs if "description:" in r.description])
    transcript_geneid = dict([(k,v[:v.index(".")]) for k, v in transcript_geneid_withversion.items()])
    entrez_by_gene = entrez.drop_duplicates("gene_stable_id").set_index(entrez.drop_duplicates("gene_stable_id").gene_stable_id)
    mapped_genes = set(g for g in transcript_geneid.values() if entrez_by_gene.index.contains(g))
    transcript_xref = dict([(k ,entrez_by_gene.loc[g].xref if g in mapped_genes else None) for k,g in transcript_geneid.items() ])

    
    #many-to-one mapping between UMI_IDS and transcripts
    umi2tx = pd.DataFrame.from_dict([{"umi_id":qn,"dataset":qn.split("_")[0],"umi":qn.split("_")[1],"transcript":names[i][:names[i].index(".")]} 
                                    for i,qn in enumerate(query_names)])
    
    #mappping from transcript gene symbols (recorded in the ensembl cdna file to 
    #refseq genes
    
    refseq_symbol_names_jointable = pd.concat([refseq_genes.name2,refseq_genes.name],axis = 1)
    refseq_symbols_idx = pd.Index(refseq_genes.name2.drop_duplicates().str.upper())
    refseq_symbol_ids = pd.Series(refseq_genes.name2.drop_duplicates().index, index=refseq_symbols_idx)
    
    tx_symbols = pd.Series(transcript_gene_symbols)
    refseq_matches = tx_symbols.apply(lambda x: refseq_symbol_ids[x] if x in refseq_symbols_idx else None).dropna()
    #roughly 85% of transcripts can be matched refseq IDs
    transcript_refseqs = refseq_matches.apply(lambda x: refseq_genes.name.loc[x])

    TX_INFO = pd.concat([pd.Series(transcript_descriptions).rename("desc"),
                     pd.Series(transcript_gene_symbols).rename("symbol"),
                    pd.Series(transcript_geneid).rename("ensembl_gene"),
                     pd.Series(transcript_xref).rename("ncbi_gene")
                    ],axis = 1, sort=True)
    
    all_umis = umi2tx.umi_id.unique()
    
    
    #GOTTA REMEMBER TO FILTER OUT ALL SHITTY UMIS!
    nosegfeat_fn = f"/data/tmp/watch_sequences/{dsname}/xumi_feat_{dsname}"
    nosegfeat_df = pd.read_csv(nosegfeat_fn,names=["umi","check","blank1","blank2","sequence"])
    nosegfeat_df.iloc[:,1].unique()
    good_umis_idx = nosegfeat_df.loc[nosegfeat_df.iloc[:,1]==0].index
    
    segfn = f"/data/tmp/watch_sequences/{dsname}/xumi_segment_base_{dsname}"
    segdf = pd.read_csv(segfn,names=["umi","seg","1","2","3"]).loc[good_umis_idx]

    go_obo_url = 'http://purl.obolibrary.org/obo/go/go-basic.obo'
    data_folder = '/data/go'

    # Check if we have the ./data directory already
    if(not os.path.isfile(data_folder)):
        # Emulate mkdir -p (no error if folder exists)
        try:
            os.mkdir(data_folder)
        except OSError as e:
            if(e.errno != 17):
                raise e
    else:
        raise Exception('Data path (' + data_folder + ') exists as a file. '
                       'Please rename, remove or change the desired location of the data path.')
    
    # Check if the file exists already
    if(not os.path.isfile(data_folder+'/go-basic.obo')):
        go_obo = wget.download(go_obo_url, data_folder+'/go-basic.obo')
    else:
        go_obo = data_folder+'/go-basic.obo'

    # Import the OBO parser from GOATools
    go = obo_parser.GODag(go_obo)


    a0 = ontologies.sort_values("DB_Object_Symbol")
    
    #for each cell in the selected segmentation, list all transcripts (with counts)
    #that appear
    
    segments = segdf.seg.unique()
    #segment1_symbols = {}
    #segment1_goterms = {}

    segment_symbols = pd.DataFrame()
    segment_goterms = pd.DataFrame()
    segment_gonames = pd.DataFrame()

    umi2txall = umi2tx.join( TX_INFO,on = "transcript")
    ontologies_by_symbol = ontologies.set_index(ontologies.DB_Object_Symbol)
    ontologies_by_symbol["GO_NAME"] = ontologies_by_symbol.GO_ID.apply(lambda x: go[x].name).rename("GO_NAME")
    umi2go=umi2txall.drop_duplicates(["umi","symbol"]).join(ontologies_by_symbol, on="symbol" )
    
    cnt = -1
    for s in segments:
        cnt+=1
        umis = [str(e) for e in segdf.loc[segdf.seg ==s].umi]

        ss2 = umi2tx.loc[umi2tx.umi.isin(umis)].join(TX_INFO,on="transcript")[["umi","ensembl_gene","ncbi_gene","symbol"]].drop_duplicates()

        go_ids = []
        for k, g in ss2.groupby("umi"):
            for sym in g. symbol:
                els = a0.index[a0.DB_Object_Symbol.searchsorted(sym,"left"):a0.DB_Object_Symbol.searchsorted(sym,"right")]
                go_ids+= list(set(list(a0.loc[els].GO_ID.values)))


        gonames = pd.DataFrame([{"segment":s,
                                "go_name":gn,
                                "go_id":gid}
                               for gn,gid in [[go[gid].name,gid] for gid in go_ids]])

        segment_symbols = segment_symbols.append(ss2)

        if len(gonames) == 0: continue
        segment_counts = gonames.drop_duplicates().join(gonames.go_id.value_counts().rename("count"),on="go_id")
        segment_gonames = segment_gonames.append(segment_counts)

        if cnt %50 == 0: print (cnt)
        #if cnt>100:break
        
    OUTDIR_GO=os.path.join( f"/data/dd-analysis/datasets/{dataset['dataset'] }/goterms/")    
    if not os.path.isdir(OUTDIR_GO): os.makedirs(OUTDIR_GO)

    
    segment_gonames.to_csv(os.path.join(OUTDIR_GO, "segments2go.csv"),index=False)
    
    umis2go_out =umis2go_out = pd.concat([umi2go.umi,umi2go.GO_NAME,umi2go.GO_ID],axis=1).drop_duplicates()
    umis2go_out.to_csv(os.path.join(OUTDIR_GO, "umis2go.csv"),index=False)
   

        
    OUTDIR_GS=os.path.join( f"/data/dd-analysis/datasets/{dataset['dataset'] }/genesymbols/")    
    if not os.path.isdir(OUTDIR_GS): os.makedirs(OUTDIR_GS)
    segment_symbols.to_csv(os.path.join(OUTDIR_GS, "segment_symbols.csv"),index = False)
    umi_symbols = umi2txall[["umi","symbol","desc","ensembl_gene","ncbi_gene"]]     
    umi_symbols.to_csv(os.path.join(OUTDIR_GS,"umi_symbols.csv"),index= False)
    
    OUTDIR_GIDS=os.path.join( f"/data/dd-analysis/datasets/{dataset['dataset'] }/geneids/")    
    if not os.path.isdir(OUTDIR_GIDS): os.makedirs(OUTDIR_GIDS)
    umi_genes = pd.concat([umi2txall.umi, umi2txall.ensembl_gene, umi2txall.ncbi_gene],axis = 1)
    umi_genes.to_csv(os.path.join(OUTDIR_GIDS,"umi_genes.csv"),index = False)
    
    OUTDIR_SEGMENTS=os.path.join(f"/data/dd-analysis/datasets/{dataset['dataset'] }/segments/")
    if not os.path.isdir(OUTDIR_SEGMENTS):os.makedirs(OUTDIR_SEGMENTS)
    umi_to_seg_df = segdf[["umi","seg"]]
    umi_to_seg_df.to_csv(os.path.join(OUTDIR_SEGMENTS,"umi2seg.csv"),index = False)

    return 0
                         

