from multiprocessing import Pool
import subprocess
import os
import glob
import json
import shutil
import time
class runssl(object):
    def __init__(self,conffile='./settings.json'):
        with open(conffile,'r') as f:
            self.conf=json.load(f)
        self.stations=[]
        self.datadir=os.path.abspath(self.conf['datafolder'])
        self.resultdir=os.path.abspath(self.conf['resultfolder'])
        self.bindir=os.path.abspath(self.conf['binfolder'])
        self.bindir=os.path.abspath(os.path.expanduser(self.bindir))
        if(not os.path.exists(self.conf['backupfolder'])):
            os.makedirs(self.conf['backupfolder'])
        if(not os.path.exists(self.resultdir)):
            os.makedirs(self.resultdir)
    def get_sac_names(self,station=''):
        e_pattern=os.path.join(self.datadir,station+"*-[A-Z][A-Z]E-*.sac")
        efiles=[os.path.basename(x) for x in glob.glob(e_pattern)]
        nfiles=[x[:12]+"N"+x[13:] for x in efiles]
        zfiles=[x[:12]+"Z"+x[13:] for x in efiles]
        sacfiles=list(zip(efiles,nfiles,zfiles))
        sacfiles=list(filter(self.file_exist,sacfiles))
        return sacfiles
    def file_exist(self,files):
        flag=True
        for i in files:
            ipath=os.path.join(self.conf["datafolder"],os.path.basename(i))
            flag=flag and os.path.exists(ipath)
        return(flag)
    def get_stations(self):
        with open(self.conf["stationfile"],'r') as f:
            self.stations=f.readlines()[0].strip().split()
            self.stations=[i.replace('/','-') for i in self.stations]
    def run1station(self,station):
        sacs=self.get_sac_names(station)
        timefmt='%s'
        now=time.strftime(timefmt,time.localtime())
        stationres=station+'_res_'+now+'.txt'
        # log the problematic sac files
        errlog=open(os.path.join(self.resultdir,"errorlog_"+station+".log"),"a")
        sslpath=os.path.join(self.bindir,'SSL')
        for sac in sacs:
            fstring="-F"+"/".join(sac)
            complete=subprocess.run("{} {} -P3 -T0.5\/0.4 >> {}".format(sslpath,fstring,stationres),shell=True,cwd=self.datadir)
            if (complete.returncode != 0):
                errlog.write("{} {} {}\n".format(sac[0],sac[1],sac[2]))
            else:
                for component in sac:
                    src=os.path.join(self.conf["datafolder"],os.path.basename(component))
                    dst=os.path.join(self.conf["backupfolder"],os.path.basename(component))
                    shutil.move(src,dst)
        errlog.close()
        src=os.path.join(self.conf["datafolder"],stationres)
        dst=os.path.join(self.resultdir,stationres)
        shutil.move(src,dst)
    def run(self,stations=None):
        if(stations is None):
            stations=self.stations
        pool=Pool()
        print(stations)
        pool.map(self.run1station,stations)
        pool.close()
        pool.join()

if (__name__=="__main__"):
    test=runssl()
    test.get_stations()
    print(test.stations)
#    test.run(stations=['AH-HEF'])
    test.run()
