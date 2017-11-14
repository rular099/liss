# -*- coding: utf-8 -*-
"""
Created on Tue Dec 22 10:16:08 2015

@author: Edward
"""
import socket
import re
import time

class LISS():
    timeInterval = 3600 #seconds
    def __init__(self,HOST,PORT,user,passwd,dataport=50288):
        self.HOST = HOST
        self.PORT = PORT
        self.user = user
        self.passwd = passwd
        self.not_retred= True
        self.dataport=dataport
        self.localhost="127.0.0.1"

    def __enter__(self):
        return(self)
    def __exit__(self,exc_type,exc_value,traceback):
        self.closeConnection()
        print('exit liss object safely')
    def connect(self):
        res = socket.getaddrinfo(str(self.HOST),self.PORT,0,socket.SOCK_STREAM)
        self.af,self.socktype,self.proto,self.canonname,self.sa = res[0]
        self.sock1 = socket.socket(self.af,self.socktype,self.proto)
        try:
            self.sock1.connect(self.sa)
            self.sock1.send(bytes('user '+str(self.user)+'\n','utf8'))
            self.sock1.send(bytes('pass '+str(self.passwd)+'\n','utf8'))
        except Exception as err:
            print(err)

    def pasvMode(self):
        if self.sock1:
            self.sock1.send(b'pasv rt\n')
            self.setInterval(1)
            msg = self.sock1.recv(512)
            msg=msg.decode('utf8')
            #print msg
            obj = re.findall(r'\d*,\d*,\d*,\d*,\d*,\d*',msg,re.I|re.M)
            tmp = obj[0].split(',')
            port2 = int(tmp[4])*256+int(tmp[5])
            print(port2)
            self.sock2 = socket.socket(self.af,self.socktype,self.proto)
            self.sock2.connect((self.HOST,port2))
            self.sock2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def getRealStream(self,stations,outFile):
        if self.sock2:
            self.sock1.send(bytes('retr all '+stations+'\n','utf8'))
            while (True):
                outFile.write(self.sock2.recv(512))
                outFile.flush()
                ## test：基于实时流的数据效验无效
                ## 原因：如果断开实时流的数据包会导致数据无法识别，因此只能采取基于文本的处理方式
                # matched =  re.search('[0-9]{6}D',data)
                # if matched:
                #     if matched.start() != 0:
                #         data = data[matched.start():] + self.sock2.recv(matched.start())
                #
                #     ## 数据导出
                #     outFile.write(data)
                #     outFile.flush()
    def getChunkStream(self,stations,outFile,lastFile=None,chunksize=512,nchunk=1000):
        if (self.sock2 ):
#            if (self.not_retred):
            self.sock1.send(bytes('retr all '+stations+'\n','utf8'))
            self.not_retred=False
            outFile_begin=False
            # errcount count successive recv errors. if errcount>errthrs, it
            # indicate some error occured and this subroutine return status code
            # 1
            errcount=0
            errthrs=10
            i=0
            while(i<nchunk):
                if(errcount>errthrs):
                    return(1)
                try:
                    s=self.sock2.recv(chunksize)
                    if (len(s)==0):
                        errcount+=1
                        continue
                    if(not outFile_begin):
                        matched=re.search(b'[0-9]{6}D',s)
                        if(matched):
                            data1=s[:matched.start()]
                            data2=s[matched.start():]
                        else:
                            data1=s
                            data2=''
                        if(data1 and (lastFile is not None)):
                            lastFile.write(data1)
                            lastFile.flush()
                        if(data2):
                            outFile.write(data2)
                            outFile.flush()
                            outFile_begin=True
                        # once recv successfully, reset the errcount
                        errcount=0
                        i+=1
                    else:
                        outFile.write(s)
                        outFile.flush()
                        # once recv successfully, reset the errcount
                        errcount=0
                        i+=1
                except socket.error:
                    return(1)
            return(0)
        return(2)

    def close(self):
        self.sock1.send(b'abor\n')
        self.sock1.send(b'quit\n')
        self.sock1.close()

    def reboot(self):
        #close all
        self.close()

        #re-connect
        self.connect()
        self.pasvMode()


    def getStations(self):
        self.sock1.send(b'stat upload\n')
        self.setInterval(1)
        return re.findall(r'\w+/\w+',self.sock1.recv(4096).decode('utf8'),re.I|re.M)

    def setInterval(self,seconds):
        time.sleep(seconds)

    '检测连接是否中断'
    def checkConnection(self):
        pass

    def closeConnection(self):
        if self.sock1:
            self.sock1.send(b'abor\n')
            self.sock1.send(b'quit\n')
            self.sock1.shutdown(socket.SHUT_RDWR)
            self.sock1.close()
            print("sokcet1 closed!")
        if self.sock2:
            self.sock2.shutdown(socket.SHUT_RDWR)
            self.sock2.close()
            print("sokcet2 closed!")


