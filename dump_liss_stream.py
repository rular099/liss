import liss as liss
import os as os
import signal
import time
import json
def setterm(signum,traceback):
    raise KeyboardInterrupt
signal.signal(signal.SIGTERM,setterm)
signal.signal(signal.SIGINT,setterm)

if(__name__=="__main__"):
    with open('settings.json','r') as f:
        conf=json.load(f)
    if (not os.path.exists(conf["datafolder"])):
        os.makedirs(conf["datafolder"])
    with open('账号.txt','r') as f:
        lines=f.readlines()
        serverip=lines[1].strip().split()[1]
        serverport=lines[1].strip().split()[2]
        user=lines[2].strip().split()[1]
        passwd=lines[3].strip().split()[1]
    with liss.LISS(serverip,serverport,user,passwd) as myliss:
        myliss.connect()
        myliss.pasvMode()
        stations=myliss.getStations()
        nchunk=int(conf['dt_mseed']*len(stations)/3.5)
        print('nchunk is: {}'.format(nchunk))
        stations=' '.join(stations).strip()
        with open(conf['stationfile'],'w') as stationf:
            stationf.write(stations)
        print(stations)
        lastFile=None
        province=stations.split('/')[0]
        try:
            while(True):
                #timefmt='%Y_%m_%d_%H_%M_%S'
                timefmt='%s'
                now=time.strftime(timefmt,time.localtime())
                fname=province+'_'+now+'.mseed'
                fname=os.path.join(conf["datafolder"],fname)
                f=open(fname,'wb')
                status_code=myliss.getChunkStream(stations,f,lastFile,nchunk=nchunk)
                if(lastFile is not None):
                    lastFile.close()
                lastFile=f
                if(status_code==1):
                    myliss.closeConnection()
                    myliss.connect()
                    myliss.pasvMode()
                    continue
        except KeyboardInterrupt:
            pass
#            myliss.closeConnection()
        finally:
            print("process killed!!")
