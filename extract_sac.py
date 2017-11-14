import obspy
import json
import glob
import os
import signal
import shutil
import pdb
class extractSAC(object):
    def __init__(self,conf_file='./settings.json'):
        self.conf_file=conf_file
        with open(conf_file,'r') as f:
            self.conf=json.load(f)
        self.mseed_done=[]
        self.current=self.conf["starttime"]
        self.residual=None
        self.need_exit=False
        self.slice_log_cache={}
        if(not os.path.exists(self.conf["backupfolder"])):
            os.mkdir(self.conf["backupfolder"])
    def __enter__(self):
        signal.signal(signal.SIGTERM,self.sig_catcher)
        signal.signal(signal.SIGINT,self.sig_catcher)
        return(self)
    def __exit__(self,exc_type,exc_value,traceback):
        self.cleanup()
    def scanfolder(self):
        '''scan all mseed files in specified folder'''
        if (os.path.exists("stations.txt")):
            with open("stations.txt","r") as f:
                self.stations=f.readlines()[0].strip().split()
            # mseed_list holds all mseed files need be processed in this run.
            self.mseed_list=sorted(glob.glob(os.path.join(self.conf["datafolder"],self.conf["datapattern"])),
                                        key=lambda x: int(os.path.basename(x).split('.')[0].split('_')[1]))
            # the last mseed file is still downloading, we dont process it.
            self.mseed_list.pop()
            try:
                self.mseed_list.remove(os.path.join(self.conf["datafolder"],self.conf["residualfile"]))
            except:
                pass
            # if residual file is not none, we need read residual data; at the
            # other hand, if no residual data provided, leave residualfile=none in the json
            # configure file.
            #
            # the residual data after last run is always the first piece of data in
            # this run, so we insert the residual file at the beginning of mseed
            # list.
            if((self.conf["residualfile"].lower()!="none") and
               (os.path.exists(os.path.join(self.conf["datafolder"],self.conf["residualfile"])))):
                self.mseed_list.insert(0,os.path.join(self.conf["datafolder"],self.conf["residualfile"]))
        else:
            print("no stations.txt found!!")
    def get_gaps(self,stream,nchannel=3):
        min_gap=stream[0].stats.delta
        stream.sort()
        channels=[]
        for trace in stream:
            if trace.stats.channel not in channels:
                channels.append(trace.stats.channel)
        traces_per_channel=len(stream)//nchannel
        gaps=[]
        for channel in channels:
            channel_stream=stream.select(channel=channel)
            startgap=stream[0].stats.endtime
            for trace in channel_stream:
                if(startgap+min_gap >= trace.stats.starttime):
                    startgap=max(startgap,trace.stats.endtime)
                else:
                    endgap=trace.stats.starttime
                    tmp=[trace.stats.network,trace.stats.station,
                         trace.stats.location,trace.stats.channel,
                         startgap,endgap,endgap-startgap,(endgap-startgap)/min_gap]
                    gaps.append(tmp)
                    startgap=trace.stats.endtime
        gaps=sorted(gaps,key=lambda i:i[4])
        ngaps=len(gaps)
        res=[]
        igap=0
        while(igap<ngaps):
            tmp=gaps[igap]
            try:
                while(gaps[igap+1][4]<tmp[5]):
                    tmp[5]=max(tmp[5],gaps[igap+1][5])
                    igap+=1
                igap+=1
            except IndexError:
                igap+=1
            finally:
                res.append(tmp)
        return(res)
    # get starttime and endtime of a stream of one station. starttime is the
    # maximum among all channels, while endtime is the minimum. because each channel can
    # be presented as many traces, we sort the traces first and then get
    # starttime/endtime of each channel.
    def get_station_time_span(self,stream,nchannel=3):
        starttime=max(trace.stats.starttime for trace in stream)
        endtime=min(trace.stats.endtime for trace in stream)
        return([starttime,endtime])
    def clean_residual(self,max_time=86400):
        stream=self.residual.copy()
        stream.merge()
        endtime=max(trace.stats.endtime for trace in stream)
        for trace in self.residual:
            if (endtime-trace.stats.endtime > max_time):
                self.residual.remove(trace)

    def do_slice(self,stream,slice_log,dt=None,starttime=None,endtime=None,fname_set=None):
        if (self.need_exit):
            self.cleanup()
            raise SystemExit()
        if(dt is None):
            dt=self.conf['dt_sac']
        if (starttime is None):
            starttime=stream[0].stats.starttime
        if (endtime is None):
            endtime=stream[0].stats.endtime
        nslice=int((endtime-starttime)//dt)
        for islice in range(nslice):
            if (self.need_exit):
                self.cleanup()
                raise SystemExit()
            tmp=stream.slice(starttime,starttime+dt)
            for trace in tmp:
                tracename='-'.join([trace.stats.network,trace.stats.station,
                    trace.stats.location,trace.stats.channel,
                    str(starttime)])+'.sac'
                tracename=os.path.join(self.conf["datafolder"],tracename)
                try:
                    no_gap=(trace.data.data==trace.data.filled())
                    n_gaps=len(no_gap[no_gap==False])
                    if(n_gaps>0):
                        print("Fatal error!! Gaps found in sliced SAC!!")
                        self.cleanup()
                        pdb.set_trace()
            #            raise(SystemExit)
                    else:
                        trace.data=trace.data.data
                        trace.write(tracename,format='SAC')
                except AttributeError:
                    trace.write(tracename,format='SAC')
                if fname_set is None:
                    slice_log.write(tracename+":"+" ".join(self.cached_mseed)+"\n")
                else:
                    slice_log.write(tracename+":"+" ".join(fname_set)+"\n")
            starttime+=dt
        return(starttime)
    def run(self):
        # we need at least 2 mseed files to start our process. if this is not your
        # case, set correct min_mseeds in the json configure file.
        self.need_exit=False
        if(len(self.mseed_list)<self.conf['min_mseeds']):
            print('too few mseed files,quit...')
            return
        # the main job
        self.residual=obspy.read(self.mseed_list[0])
        for trace in self.residual:
            trace.filename=self.mseed_list[0]
        # mseed_done refers to mseed files has been processed, which should be
        # deleted after the main job.
        self.cached_mseed=[self.mseed_list[0]]
        dt=self.conf['dt_sac']
        slice_log=open("slice_log.txt","w")
        slice_log.close()
        slice_log=open("slice_log.txt","a")
        # residual refers to residual stream data after processing
        for i in range(1,len(self.mseed_list)):
            print("processing file {}".format(self.mseed_list[i]))
            if(os.path.basename(self.mseed_list[i])=='AH_1510497003.mseed'):
                pass
#                pdb.set_trace()
            self.residual+=obspy.read(self.mseed_list[i])
            for trace in self.residual:
                if not hasattr(trace,'filename'):
                    trace.filename=self.mseed_list[i]
            self.cached_mseed.append(self.mseed_list[i])
            if(i%500==0):
                self.clean_residual()
            # assuming times are not aligned between stations, but aligned for
            # the same station
            j=0
            for stn in self.stations:
                j+=1
                network,station=stn.split('/')
                station_stream=self.residual.select(network=network,station=station)
                merged_station_stream=station_stream.copy().merge(method=1)
                if(len(station_stream)==0):
                    continue
                fname_set=set()
                for station_trace in station_stream:
                    self.residual.remove(station_trace)
                    fname_set.add(station_trace.filename)
                gaps=self.get_gaps(station_stream)
                station_starttime,station_endtime=self.get_station_time_span(merged_station_stream)
                starttime=station_starttime
                for gap in gaps:
                    endtime=gap[4]
                    starttime=self.do_slice(merged_station_stream,slice_log,starttime=starttime,endtime=endtime,fname_set=fname_set)
                    self.residual+=station_stream.slice(starttime,endtime)
                    starttime=gap[5]
                endtime=station_endtime
                starttime=self.do_slice(merged_station_stream,slice_log,starttime=starttime,endtime=endtime,fname_set=fname_set)
                self.residual+=station_stream.slice(starttime)
            if(len(self.residual)==0):
                ndone=len(self.cached_mseed)
            else:
                ndone=len(self.cached_mseed)-1
            for i_done in range(len(self.cached_mseed)-1):
                self.mseed_done.append(self.cached_mseed[i_done])
            if(len(self.residual)==0):
                self.mseed_done.append(self.cached_mseed[-1])
                self.cached_mseed=[]
            else:
                self.cached_mseed=self.cached_mseed[-1:]
        slice_log.close()
        self.cleanup()
    def remove_mseed(self):
        for i in self.mseed_done:
            os.remove(i)
        try:
            self.conf["residualfile"]=self.mseed_done[-1]
        except:
            self.conf["residualfile"]='tmp.mseed'
        self.mseed_done=[]
    def backup_mseed(self):
        for i in self.mseed_done:
            src=i
            dst=os.path.join(self.conf["backupfolder"],os.path.basename(i))
            if(not os.path.exists(dst)):
                shutil.move(src,dst)
            else:
                f=open(dst,'ab')
                shutil.copyfileobj(open(src,'rb'),f)
                f.close()
                os.remove(src)
        try:
            self.conf["residualfile"]=self.mseed_done[-1]
        except:
            self.conf["residualfile"]='tmp.mseed'
        self.mseed_done=[]
    def recover_mseed(self):
        mseeds=glob.glob(os.path.join(self.conf["backupfolder"],self.conf["datapattern"]))
        for i in mseeds:
            src=i
            dst=os.path.join(self.conf["datafolder"],os.path.basename(i))
            if(not os.path.exists(dst)):
                shutil.move(src,dst)
            else:
                f=open(src,'ab')
                shutil.copyfileobj(open(dst,'rb'),f)
                f.close()
                shutil.move(src,dst)
    def write_conf(self):
#        self.conf['starttime']=str(self.current)
        with open(self.conf_file,'w') as f:
            json.dump(self.conf,f,indent=4,sort_keys=True)
    def write_residual(self,fname=None):
        if(fname is None):
            fname=os.path.join(self.datadir,"tmp.mseed")
        if(self.residual is not None):
            self.residual.write(fname,format='mseed')
        else:
            self.conf["residualfile"]='None'
    def cleanup(self):
        self.backup_mseed()
        self.write_residual(self.conf["residualfile"])
        self.write_conf()
    def sig_catcher(self,sigtype,frame):
        self.need_exit=True

def main():
    with extractSAC() as e:
        e.scanfolder()
        e.run()

if (__name__=="__main__"):
    main()
