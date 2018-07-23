import datetime
import networkx as nx
import matplotlib.pyplot as plt
import astropy.stats as astats
from astropy.stats import median_absolute_deviation as MAD
import os
import matplotlib
import numpy as np
import os.path
import json

Name = 'KIM'

Name = 'KIM-Minify2-CLF-DB'

logDates = {}
dbh = DBHelper()

logs,logcolnames = dbh.GET_ALL(table='LOGS')
filerows,fidCols =  dbh.GET_ALL(table='FILES')
fileIDS = {}
for row in filerows:
    fileIDS[row[1]] = row[0]

filerows = None
logsd = {}

fidCBL = {}

fidcmpBlamer = {}

fidcmp = {}

for log in logs:
    key = log[logcolnames.index('locid')]
    logsd[key]=log
    logDates[key]=log[logcolnames.index('timestamp')]    
    
    
    
logs = None

for key in logsd.keys():
    lg =logsd[key]
    logDates[key]=lg[logcolnames.index('timestamp')]

bugDates = {}
        
keys = ['bug','reportTimestamp','status','product','component']
vbugs,vbugscolnames = dbh.GET_ALL(table='VALIDBUGS')

for line in vbugs:
            
    b = str(line[0])
            
    bugDates[b] = line[1]

f = open('q1.csv')
comps = f.readlines()[1:]
f.close()
for i in range(len(comps)):
    comps[i] = comps[i].split(',')[0].strip()

ChgCompBlamed = {}
ChgCompBlamer = {}

for cmpName in comps:
    allfiles = set()                
    BlameFol = 'Blames'+Name+'/'
            
    blamedMultiParents=[]
    blamedHasMergeKeyword=[]
    
    
	def ExtractBlames(lines,ruBlamer,ruBlamed,Blamer,Blamed,ruFlBlmr, ruFileBlamed, FileBlamer, FileBlamed):
        for i,line in enumerate(lines):
        
            line = line.split(Helpers.bcSep)
            file = line[2].split(':')
            fid, file = int(file[0]),file[1]
            if not  Helpers.isExtValidCpp(file):
                continue

            allfiles.add(file)

            commitId = line[0]
            bugId = line[1]
            blamerAuth = logAuthors[int(commitId)]
            cmb = commitId+'--'+bugId
            fb = str(fid)+'--'+bugId
		
            parentNode = line[3].split('-')
            subjNode = line[4].split('-')
            blmdRv = parentNode[0].split(':')[1]
            BlameLines = parentNode[1]+'-'+parentNode[2]+','+subjNode[1]+'-'+subjNode[2]
            
            #RUBlamer
            if not cmb in ruBlamer.keys():
                ruBlamer[cmb] = {}

            if blmdRv not in ruBlamer[cmb].keys():
                ruBlamer[cmb][blmdRv] = []
            ruBlamer[cmb][blmdRv].append(BlameLines)

            #ruFlBlmr
            if not fid in ruFlBlmr.keys():
                ruFlBlmr[fid] = {}

            if not commitId in ruFlBlmr[fid].keys():
                ruFlBlmr[fid][commitId] = {}

            if not bugId in ruFlBlmr[fid][commitId].keys():
                ruFlBlmr[fid][commitId][bugId] = {}

            if blmdRv not in ruFlBlmr[fid][commitId][bugId].keys():
                ruFlBlmr[fid][commitId][bugId][blmdRv] = []
            ruFlBlmr[fid][commitId][bugId][blmdRv].append(BlameLines)

            #RUFileBlamed
            if not fid in ruFileBlamed.keys():
                ruFileBlamed[fid] = {}

            if not blmdRv in ruFileBlamed[fid].keys():
                ruFileBlamed[fid][blmdRv] = set()

            if not cmb in ruFileBlamed[fid][blmdRv]:
                ruFileBlamed[fid][blmdRv].add(cmb)


    rBlamed = {}
    rBlamer = {}
    uBlamed = {}
    uBlamer = {}
    rFileBlamed = {}
    rFileBlamer = {}
    uFileBlamed = {}
    uFileBlamer = {}


    Blamed = {}
    Blamer = {}
    FileBlamed = {}
    FileBlamer = {}

    FutureBugs = {}
    BlamedForBug = {}
    for file in os.listdir(BlameFol):
        if file.startswith('RemovedBlames-'):
            f= open(BlameFol+file,encoding='utf8')
            lines = [line for line in f.read().split(Helpers.bcSep+'\n\n\n') if len(line)>3]
            f.close()   
            ExtractBlames(lines,rBlamer,rBlamed,Blamer,Blamed,rFileBlamer, rFileBlamed, FileBlamer, FileBlamed)
        elif file.startswith ('UpdatedBlames-'):
            f= open(BlameFol+file,encoding='utf8')
            lines =[line for line in  f.read().split(Helpers.bcSep+'\n\n\n') if len(line)>3]
            f.close()
            ExtractBlames(lines,uBlamer,uBlamed,Blamer,Blamed,uFileBlamer, uFileBlamed, FileBlamer, FileBlamed)

        
    allchanges = set()

    for cmb in Blamer.keys():
        chg = cmb.split('--')[0]
        vals = set(Blamer[cmb].keys())
        vals.add(chg)
        for chg in vals:
            if not chg in allchanges:
                allchanges.add(chg)

    G=nx.DiGraph()
    for nd in allchanges:
        G.add_node(nd)

    for cmb in Blamer.keys():
        fromNode = cmb.split('--')[0]
        vals = set(Blamer[cmb].keys())
        for toNode in vals:
            G.add_edge(fromNode,toNode)

    pr = nx.pagerank(G,alpha=0.1)
    prlst = [(pr[key],key) for key in pr.keys()]
    prlst = sorted(prlst,key=lambda tup: tup[0],reverse=True)
    
    prTop = {p[1] for p in prlst[:int(len(prlst)/10)]}
    
    BlamedCounts = []
    for key in Blamed.keys():
        BlamedCounts.append(len(Blamed[key]))   
    
    BlamerCounts = []
    for key in Blamer.keys():
        BlamerCounts.append(len(Blamer[key]))
    
    
    BlamedList = [(len(Blamed[key]),key) for key in Blamed.keys()]
    BlamedList = sorted(BlamedList,key=lambda tup: tup[0],reverse=True)

    FileBlamedList = {fid:[(len(FileBlamed[fid][key]),key) for key in FileBlamed[fid].keys()] for fid in FileBlamed.keys()}

    
    bl25 = {p[1] for p in BlamedList[:int(len(BlamedList)/4)]}
    cnt = 0
    for b in bl25:
        if b in prTop:
            cnt+=1
    
    BlamerList = [(len(Blamer[key].keys()),key) for key in Blamer.keys()]
    BlamerList = sorted(BlamerList,key=lambda tup: tup[0],reverse=True)

    FileBlamerList = {fid:[(np.sum([len(FileBlamer[fid][key][b].keys()) for  b in FileBlamer[fid][key]]),key) for key in FileBlamer[fid].keys()] for fid in FileBlamer.keys()}

    bl25 = {p[1].split('--')[0] for p in BlamerList[:int(len(BlamerList)/4)]}
    cnt = 0
    for b in bl25:
        if b in prTop:
            cnt+=1

    BLIST = [x[0] for x in BlamerList]

    CntFtBugs = [(len(set([cmb.split('--')[1] for cmb in Blamed[key]])),key) for key in Blamed.keys()]
    CntFtBugs = sorted(CntFtBugs,key=lambda tup: tup[0],reverse=True)
    
    bl25 = {p[1] for p in CntFtBugs[:int(len(CntFtBugs)/4)]}
    cnt = 0
    for b in bl25:
        if b in prTop:
            cnt+=1
    
    BLIST = [x[0] for x in CntFtBugs]
   
    for key in Blamed.keys():
        FutureBugs[key] = list({cmb.split('--')[1] for cmb in Blamed[key]})

        for cmb in  Blamed[key]:
            b = cmb.split('--')[1]
            if not b in BlamedForBug.keys():
                BlamedForBug[b] = set()
            BlamedForBug[b].add(cmb.split('--')[0])
    timespans = []
    timespansNoz = []
    ids = []

    for key in FutureBugs.keys():
        bds = sorted([bugDates[b] for b in FutureBugs[key]])

        if len(bds)>1:
            timespans.append((datetime.datetime.fromtimestamp(bds[-1])-datetime.datetime.fromtimestamp(bds[0])).total_seconds()/86400)
            timespansNoz.append(timespans[-1])
            ids.append(key)
        else:
            timespans.append(0)

    mad = MAD(timespansNoz)
    umad = mad+np.median(timespansNoz)
    lmad = np.median(timespansNoz)-mad
    
    timespans = []
    timespansNoz = []
    for key in BlamedForBug.keys():
        bds = sorted([logDates[int(l)] for l in BlamedForBug[key]])

        if len(bds)>1:
            timespans.append((datetime.datetime.fromtimestamp(bds[-1])-datetime.datetime.fromtimestamp(bds[0])).total_seconds()/86400)
            timespansNoz.append(timespans[-1])
        else:
            timespans.append(0)


    
    mad = MAD(timespansNoz)
    umad = mad+np.median(timespansNoz)
    lmad = np.median(timespansNoz)-mad

    allChangesets = set()

    fids = set()
    for fid in FileBlamedList.keys():
        fids.add(fid)

    for fid in FileBlamerList.keys():
        fids.add(fid)

    for fid in fids:
        allChgSet = set()
		
        i=0 
        for rev in allChgSet:
            blamedCount = 0
            blamerCount = 0

            if rev in FileBlamed[fid].keys():
                blamedCount = len(FileBlamed[fid][rev])
                if cmpName not in fidCBL[fid].keys():
                    fidCBL[fid][cmpName]=1
                else:
                    fidCBL[fid][cmpName]+=1

                if cmpName not in fidcmp[fid].keys():
                    fidcmp[fid][cmpName]=1
                else:
                    fidcmp[fid][cmpName]+=1


            if rev in FileBlamer[fid].keys():
                for b in FileBlamer[fid][rev].keys():
                    blamerCount+=len(FileBlamer[fid][rev][b].keys())
                if cmpName not in fidcmpBlamer[fid].keys():
                    fidcmpBlamer[fid][cmpName]=1
                else:
                    fidcmpBlamer[fid][cmpName]+=1

                if cmpName not in fidcmp[fid].keys():
                    fidcmp[fid][cmpName]=1
                else:
                    fidcmp[fid][cmpName]+=1


            allChangesets.add(MetricsChgData[str(fid)][rev]+';'+str(fid)+';'+rev+':'+str(blamedCount)+'@'+str(blamerCount))
            
            key = str(fid)+'-'+rev
            if  not key in ChgCompBlamed['FileJustBlamed'].keys():
                ChgCompBlamed['FileJustBlamed'][key] = set()
                ChgCompBlamed['FileJustBlamed'][key].add(cmpName)
            else:
                ChgCompBlamed['FileJustBlamed'][key].add(cmpName)
            if i%50==0:
                print (i)
            i+=1

			
o = open(Name+'--File--ChgCompBlamedJustBlamed.txt','w')
for key in ChgCompBlamed['FileJustBlamed'].keys():
    o.write('%s;%s;%d\n'%(key,ChgCompBlamed['FileJustBlamed'][key],len(ChgCompBlamed['FileJustBlamed'][key])))
o.close()


if not os.path.exists('htfidcmp.json'):
    json.dump(fidcmp,open('htfidcmp.json','w'))
