name1="tests"
setwd()

# Global Strategies Setting
library(zoo)
library(lubridate)
library(PerformanceAnalytics)
library(bizdays)
library(matrixStats)

# read data from two source & process them for comdty returns
cindex <- as.data.frame(read.csv("./data/fut1return-com.csv", row.names=1,header=TRUE))
cindex1 <- as.data.frame(read.csv("./data/BCOM.csv", row.names=1,header=TRUE))
CRet1=cindex1[2:nrow(cindex1),]/cindex1[1:(nrow(cindex1)-1),]-1
CRet=cindex[2:nrow(cindex),]/cindex[1:(nrow(cindex)-1),]-1
CRet[is.na(CRet)]=CRet1[is.na(CRet)]

# from return data, making index
CRet[is.na(CRet)]=0
Cindex=CRet*0
Cindex[1,]=matrix(rep(1,ncol(Cindex)),nrow=1)
for (i in 2:nrow(Cindex)){
  Cindex[i,]=Cindex[(i-1),]*(1+CRet[i,])
}

# param setting part
RBP=3
CSNUM=0.35
CSpos=0

# param setting part
TSWGT=1
CSWGT=1
RB1=RBP
RB2=RBP
RET=CRet
index=Cindex
CSdesign="vol"

# stat3=CSS(RB1=RBP,RB2=RBP,RET=CRet,index=Cindex,CSdesign="vol")
RETfinal=RET

for(statrun in 1:2){
  if (statrun==1){RET=CRet[,c("C","S","SB","SM","W","KC","CT")]}
  if (statrun==2){RET=CRet[,!(names(CRet) %in%c("C","S","SB","SM","W","KC","CT"))]}
  index=Cindex[,colnames(RET)]

  Ret=RET

  minobs1=12
  minobs=60
  ##RV
  seasonal<-as.data.frame(read.csv("./data/seasonal.csv", row.names=1,header=TRUE))[,colnames(Ret)]

  carry <- as.data.frame(read.csv("./data/carry-com.csv", row.names=1,header=TRUE))[,colnames(Ret)]
  carry2 <- as.data.frame(read.csv("./data/carry-com2.csv", row.names=1,header=TRUE))[,colnames(Ret)]
  fundwgt=1
  statwgt=1

  minobs=500

  Expanding=0

  nopos=0.4##middle section->neutral zone
  MA=c(1,2,3,4,5)/15
  CS=CSNUM
  if(CSpos==1){CS=CSpostest}


  #Factor Portfolio construction
  Assetvol=0.02
  Strategyvol=0.02
  SMA=20
  LMA=250
  Lwindow=1125
  factorvol=0.02
  factorsd=260
  assetsd=260
  statsd=260
  ##volupdate trigger
  Volband=0.1

  ##carry score
  Ret=RET
  seasonal[is.na(seasonal)]=0
  seasonal$mon=month(rownames(seasonal))
  fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))
  TSpos=fx*0
  colnames(TSpos)=colnames(Ret)
  TSpos$mon=month(rownames(TSpos))

  # seaonality strategy 1.,, but it is not used. below, TSRV1 is reassign by signal 2.
  # maping sesaonal data to monthly time series
  for(i in 1:11){
    TSpos[TSpos$mon==seasonal$mon[i],1:ncol(index)]=seasonal[i+1,1:ncol(index)]

  }
  TSpos[TSpos$mon==seasonal$mon[12],1:ncol(index)]=seasonal[1,1:ncol(index)]
  TSRV1=TSpos[,1:ncol(index)]*fundwgt # change into signal.



  short=0


  # signal 1. 3month later 5 year moving average carry
  fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))

  carry$YM=as.yearmon(rownames(carry))
  carry$mon=month(rownames(carry))
  sss=intersect(rownames(carry),rownames(fx))
  fx=fx[sss,]
  bible=fx[61:nrow(fx),]*0 # after 5 year, it starts. bible row is 5 year later than row of fx.
  colnames(bible)=colnames(Ret)
  statday1=as.yearmon(rownames(bible))+3/12
  statday=as.yearmon(rownames(fx))+3/12 # add 3 month
  bible$YM=statday1
  bible$mon=month(bible$YM)


  # for (i in 1:nrow(bible)){
  #   ave=mean(colMeans(Ret[carry$YM<bible$YM[i]& carry$YM>=statday[1]&carry$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))
  #   bible[i,1:ncol(index)]=(colMeans(carry[carry$YM<bible$YM[i]& carry$YM>=statday[1]&carry$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))#/colSds(as.matrix(Ret[Ret$YM<bible$YM[i]& Ret$YM>=statday[1]& Ret$mon==bible$mon[i],1:ncol(CSRV)]),na.rm=TRUE)
  #   #bible[i,1:ncol(index)]=bible[i,1:ncol(index)]*sqrt(nrow(Ret[Ret$YM<bible$YM[i]& Ret$YM>=statday[1]&Ret$mon==bible$mon[i],1:ncol(index)]))
  # }

  for (i in 1:nrow(bible)){
    ave=(colMeans(carry[carry$YM<(bible$YM[i]) &carry$YM>=statday[i]&carry$mon!=bible$mon[i],1:ncol(index)],na.rm=TRUE)) # it is not used
    # for 5 years moving window, calculate average of 3 month later month carry.
    bible[i,1:ncol(index)]=(colMeans(carry[carry$YM<(bible$YM[i])& carry$YM>=statday[i]&carry$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))#/colSds(as.matrix(Ret[Ret$YM<(bible$YM[i])& Ret$YM>=statday[i]& Ret$mon==bible$mon[i],1:ncol(CSRV)]),na.rm=TRUE)

  }

  #
  # for (i in 1:60){
  #   ave=mean(colMeans(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[1]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))
  #   bible[i,1:ncol(index)]=(colMeans(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[1]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))/colSds(as.matrix(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[1]& Ret$mon==bible$mon[i],1:ncol(CSRV)]),na.rm=TRUE)
  #   bible[i,1:ncol(index)]=bible[i,1:ncol(index)]*sqrt(nrow(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[1]&Ret$mon==bible$mon[i],1:ncol(index)]))
  # }
  #
  # for (i in 61:nrow(bible)){
  #   ave=mean(colMeans(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[i-60]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))
  #   bible[i,1:ncol(index)]=(colMeans(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[i-60]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))/colSds(as.matrix(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[i-60]& Ret$mon==bible$mon[i],1:ncol(CSRV)]),na.rm=TRUE)
  #   bible[i,1:ncol(index)]=bible[i,1:ncol(index)]*sqrt(nrow(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[i-60]&Ret$mon==bible$mon[i],1:ncol(index)]))
  # }


  RV1=bible[,1:ncol(index)]
  truecount=round(rowSums(!is.na(RV1))*CS)
  truecount=matrix(rep(truecount,ncol(RV1)),nrow=nrow(RV1))

  bibleRV=as.data.frame(t(as.data.frame(apply(RV1,1,rank,ties.method="first",na.last="keep"))))
  bibleRV1=as.data.frame(t(as.data.frame(apply(-RV1,1,rank,ties.method="last",na.last="keep"))))
  bibleRVpos=bibleRV*0
  bibleRVpos[bibleRV[,]<=truecount]=-1
  bibleRVpos[bibleRV1[,]<=truecount]=1



  CSRV=bibleRVpos[,1:ncol(index)]
  rownames(CSRV)=rownames(fx)[(nrow(fx)-nrow(CSRV)+1):nrow(fx)]
  CSRVori=CSRV


  bibleTS1=bible[,1:ncol(index)]
  bibleTS=bibleTS1*0
  bibleTS[bibleTS1[]<(0)]=-1
  bibleTS[bibleTS1[]>(0)]=1

  TSRV2=bibleTS[,1:ncol(index)]
  rownames(TSRV2)=rownames(fx)[(nrow(fx)-nrow(TSRV2)+1):nrow(fx)]


  # signal 2. 2month later 5 year moving average carry
  fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))

  carry$YM=as.yearmon(rownames(carry))
  carry$mon=month(rownames(carry))
  sss=intersect(rownames(carry),rownames(fx))
  fx=fx[sss,]
  bible=fx[61:nrow(fx),]*0
  colnames(bible)=colnames(Ret)
  statday1=as.yearmon(rownames(bible))+2/12
  statday=as.yearmon(rownames(fx))+2/12
  bible$YM=statday1
  bible$mon=month(bible$YM)


  # for (i in 1:nrow(bible)){
  #   ave=mean(colMeans(Ret[carry$YM<bible$YM[i]& carry$YM>=statday[1]&carry$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))
  #   bible[i,1:ncol(index)]=(colMeans(carry[carry$YM<bible$YM[i]& carry$YM>=statday[1]&carry$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))#/colSds(as.matrix(Ret[Ret$YM<bible$YM[i]& Ret$YM>=statday[1]& Ret$mon==bible$mon[i],1:ncol(CSRV)]),na.rm=TRUE)
  #   #bible[i,1:ncol(index)]=bible[i,1:ncol(index)]*sqrt(nrow(Ret[Ret$YM<bible$YM[i]& Ret$YM>=statday[1]&Ret$mon==bible$mon[i],1:ncol(index)]))
  # }

  for (i in 1:nrow(bible)){
    ave=(colMeans(carry[carry$YM<(bible$YM[i]) &carry$YM>=statday[i]&carry$mon!=bible$mon[i],1:ncol(index)],na.rm=TRUE))
    bible[i,1:ncol(index)]=(colMeans(carry[carry$YM<(bible$YM[i])& carry$YM>=statday[i]&carry$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))#/colSds(as.matrix(Ret[Ret$YM<(bible$YM[i])& Ret$YM>=statday[i]& Ret$mon==bible$mon[i],1:ncol(CSRV)]),na.rm=TRUE)

  }



  #
  # for (i in 1:60){
  #   ave=mean(colMeans(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[1]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))
  #   bible[i,1:ncol(index)]=(colMeans(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[1]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))/colSds(as.matrix(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[1]& Ret$mon==bible$mon[i],1:ncol(CSRV)]),na.rm=TRUE)
  #   bible[i,1:ncol(index)]=bible[i,1:ncol(index)]*sqrt(nrow(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[1]&Ret$mon==bible$mon[i],1:ncol(index)]))
  # }
  #
  # for (i in 61:nrow(bible)){
  #   ave=mean(colMeans(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[i-60]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))
  #   bible[i,1:ncol(index)]=(colMeans(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[i-60]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))/colSds(as.matrix(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[i-60]& Ret$mon==bible$mon[i],1:ncol(CSRV)]),na.rm=TRUE)
  #   bible[i,1:ncol(index)]=bible[i,1:ncol(index)]*sqrt(nrow(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[i-60]&Ret$mon==bible$mon[i],1:ncol(index)]))
  # }


  RV1=bible[,1:ncol(index)]
  truecount=round(rowSums(!is.na(RV1))*CS)
  truecount=matrix(rep(truecount,ncol(RV1)),nrow=nrow(RV1))

  bibleRV=as.data.frame(t(as.data.frame(apply(RV1,1,rank,ties.method="first",na.last="keep"))))
  bibleRV1=as.data.frame(t(as.data.frame(apply(-RV1,1,rank,ties.method="last",na.last="keep"))))
  bibleRVpos=bibleRV*0
  bibleRVpos[bibleRV[,]<=truecount]=-1
  bibleRVpos[bibleRV1[,]<=truecount]=1

  fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))

  CSRV2=bibleRVpos[,1:ncol(index)]
  rownames(CSRV2)=rownames(fx)[(nrow(fx)-nrow(CSRV2)+1):nrow(fx)]

  bibleTS1=bible[,1:ncol(index)]
  bibleTS=bibleTS1*0
  bibleTS[bibleTS1[]<(0)]=-1
  bibleTS[bibleTS1[]>(0)]=1

  TSRV1=bibleTS[,1:ncol(Ret)]
  rownames(TSRV1)=rownames(fx)[(nrow(fx)-nrow(TSRV1)+1):nrow(fx)]


  # signal 3. 1month later 5 year moving average carry
  fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))

  Ret$YM=as.yearmon(rownames(Ret))
  Ret$mon=month(rownames(Ret))

  sss=intersect(rownames(Ret),rownames(fx))
  fx=fx[sss,]
  biblecv=fx[61:nrow(fx),]*0
  colnames(biblecv)=colnames(Ret)[1:ncol(index)]
  statday1=as.yearmon(rownames(biblecv))+1/12
  statday=as.yearmon(rownames(fx))+1/12
  biblecv$YM=statday1
  biblecv$YM=statday1
  biblecv$mon=month(biblecv$YM)



  for (i in 1:nrow(biblecv)){
    ave=mean(colMeans(Ret[Ret$YM<biblecv$YM[i]& Ret$YM>=statday[1]&Ret$mon==biblecv$mon[i],1:ncol(index)],na.rm=TRUE))
    # this time, divide 5 year moving average 1 month later carry average with stdev
    biblecv[i,1:ncol(index)]=(colMeans(Ret[Ret$YM<biblecv$YM[i]& Ret$YM>=statday[1]&Ret$mon==biblecv$mon[i],1:ncol(index)],na.rm=TRUE))/colSds(as.matrix(Ret[Ret$YM<biblecv$YM[i]& Ret$YM>=statday[1]& Ret$mon==biblecv$mon[i],1:ncol(CSRV)]),na.rm=TRUE)
    # below might not necessary,,
    biblecv[i,1:ncol(index)]=biblecv[i,1:ncol(index)]*sqrt(nrow(Ret[Ret$YM<biblecv$YM[i]& Ret$YM>=statday[1]&Ret$mon==biblecv$mon[i],1:ncol(index)]))
  }

  # for (i in 1:nrow(bible)){
  #   ave=(colMeans(Ret[Ret$YM<(bible$YM[i]) &Ret$YM>=statday[i]&Ret$mon!=bible$mon[i],1:ncol(index)],na.rm=TRUE))
  #   bible[i,1:ncol(index)]=(colMeans(Ret[Ret$YM<(bible$YM[i])& Ret$YM>=statday[i]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))/colSds(as.matrix(Ret[Ret$YM<(bible$YM[i])& Ret$YM>=statday[i]& Ret$mon==bible$mon[i],1:ncol(CSRV)]),na.rm=TRUE)
  #
  # }

  #
  # for (i in 1:60){
  #   ave=mean(colMeans(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[1]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))
  #   bible[i,1:ncol(index)]=(colMeans(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[1]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))/colSds(as.matrix(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[1]& Ret$mon==bible$mon[i],1:ncol(CSRV)]),na.rm=TRUE)
  #   bible[i,1:ncol(index)]=bible[i,1:ncol(index)]*sqrt(nrow(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[1]&Ret$mon==bible$mon[i],1:ncol(index)]))
  # }
  #
  # for (i in 61:nrow(bible)){
  #   ave=mean(colMeans(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[i-60]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))
  #   bible[i,1:ncol(index)]=(colMeans(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[i-60]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))/colSds(as.matrix(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[i-60]& Ret$mon==bible$mon[i],1:ncol(CSRV)]),na.rm=TRUE)
  #   bible[i,1:ncol(index)]=bible[i,1:ncol(index)]*sqrt(nrow(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[i-60]&Ret$mon==bible$mon[i],1:ncol(index)]))
  # }


  RV1=biblecv[,1:ncol(index)]
  truecount=round(rowSums(!is.na(RV1))*CS)
  truecount=matrix(rep(truecount,ncol(RV1)),nrow=nrow(RV1))

  bibleRVcv=as.data.frame(t(as.data.frame(apply(RV1,1,rank,ties.method="first",na.last="keep"))))
  bibleRV1cv=as.data.frame(t(as.data.frame(apply(-RV1,1,rank,ties.method="last",na.last="keep"))))
  bibleRVposcv=bibleRVcv*0
  bibleRVposcv[bibleRVcv[,]<=truecount]=-1
  bibleRVposcv[bibleRV1cv[,]<=truecount]=1

  fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))

  CSRVcv=bibleRVposcv[,1:ncol(index)]
  rownames(CSRVcv)=rownames(fx)[(nrow(fx)-nrow(CSRVcv)+1):nrow(fx)]

  #####need to adjust due to month end rebalancing(get Nov on oct/31)

  #
  bibleTS1=biblecv[,1:ncol(index)]
  bibleTS=bibleTS1*0
  bibleTS[bibleTS1[]<(0)]=-1
  bibleTS[bibleTS1[]>(0)]=1

  fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))

  TSRV4=bibleTS[,1:ncol(index)]
  rownames(TSRV4)=rownames(fx)[(nrow(fx)-nrow(TSRV4)+1):nrow(fx)]

  # signal 4? 2 month later 5 year moving average
  fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))

  Ret$YM=as.yearmon(rownames(Ret))
  Ret$mon=month(rownames(Ret))

  sss=intersect(rownames(Ret),rownames(fx))
  fx=fx[sss,]
  biblecv=fx[61:nrow(fx),]*0
  colnames(biblecv)=colnames(Ret)[1:ncol(index)]
  statday1=as.yearmon(rownames(biblecv))+2/12
  statday=as.yearmon(rownames(fx))+2/12
  biblecv$YM=statday1
  biblecv$YM=statday1
  biblecv$mon=month(biblecv$YM)



  for (i in 1:nrow(biblecv)){
    ave=mean(colMeans(Ret[Ret$YM<biblecv$YM[i]& Ret$YM>=statday[1]&Ret$mon==biblecv$mon[i],1:ncol(index)],na.rm=TRUE))
    biblecv[i,1:ncol(index)]=(colMeans(Ret[Ret$YM<biblecv$YM[i]& Ret$YM>=statday[1]&Ret$mon==biblecv$mon[i],1:ncol(index)],na.rm=TRUE))/colSds(as.matrix(Ret[Ret$YM<biblecv$YM[i]& Ret$YM>=statday[1]& Ret$mon==biblecv$mon[i],1:ncol(CSRV)]),na.rm=TRUE)
    biblecv[i,1:ncol(index)]=biblecv[i,1:ncol(index)]*sqrt(nrow(Ret[Ret$YM<biblecv$YM[i]& Ret$YM>=statday[1]&Ret$mon==biblecv$mon[i],1:ncol(index)]))
  }

  # for (i in 1:nrow(bible)){
  #   ave=(colMeans(Ret[Ret$YM<(bible$YM[i]) &Ret$YM>=statday[i]&Ret$mon!=bible$mon[i],1:ncol(index)],na.rm=TRUE))
  #   bible[i,1:ncol(index)]=(colMeans(Ret[Ret$YM<(bible$YM[i])& Ret$YM>=statday[i]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))/colSds(as.matrix(Ret[Ret$YM<(bible$YM[i])& Ret$YM>=statday[i]& Ret$mon==bible$mon[i],1:ncol(CSRV)]),na.rm=TRUE)
  #
  # }

  #
  # for (i in 1:60){
  #   ave=mean(colMeans(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[1]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))
  #   bible[i,1:ncol(index)]=(colMeans(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[1]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))/colSds(as.matrix(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[1]& Ret$mon==bible$mon[i],1:ncol(CSRV)]),na.rm=TRUE)
  #   bible[i,1:ncol(index)]=bible[i,1:ncol(index)]*sqrt(nrow(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[1]&Ret$mon==bible$mon[i],1:ncol(index)]))
  # }
  #
  # for (i in 61:nrow(bible)){
  #   ave=mean(colMeans(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[i-60]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))
  #   bible[i,1:ncol(index)]=(colMeans(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[i-60]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))/colSds(as.matrix(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[i-60]& Ret$mon==bible$mon[i],1:ncol(CSRV)]),na.rm=TRUE)
  #   bible[i,1:ncol(index)]=bible[i,1:ncol(index)]*sqrt(nrow(Ret[Ret$YM<(bible$YM[i]-2)& Ret$YM>=statday[i-60]&Ret$mon==bible$mon[i],1:ncol(index)]))
  # }


  RV1=biblecv[,1:ncol(index)]
  truecount=round(rowSums(!is.na(RV1))*CS)
  truecount=matrix(rep(truecount,ncol(RV1)),nrow=nrow(RV1))

  bibleRVcv=as.data.frame(t(as.data.frame(apply(RV1,1,rank,ties.method="first",na.last="keep"))))
  bibleRV1cv=as.data.frame(t(as.data.frame(apply(-RV1,1,rank,ties.method="last",na.last="keep"))))
  bibleRVposcv=bibleRVcv*0
  bibleRVposcv[bibleRVcv[,]<=truecount]=-1
  bibleRVposcv[bibleRV1cv[,]<=truecount]=1

  fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))

  CSRVcv2=bibleRVposcv[,1:ncol(index)]
  rownames(CSRVcv2)=rownames(fx)[(nrow(fx)-nrow(CSRVcv2)+1):nrow(fx)]

  #####need to adjust due to month end rebalancing(get Nov on oct/31)






  #
  #
  #
  bibleTS1=biblecv[,1:ncol(index)]
  bibleTS=bibleTS1*0
  bibleTS[bibleTS1[]<(0)]=-1
  bibleTS[bibleTS1[]>(0)]=1

  TSRV3=bibleTS[,1:ncol(index)]
  rownames(TSRV3)=rownames(fx)[(nrow(fx)-nrow(TSRV3)+1):nrow(fx)]






  AAA=intersect(rownames(TSRV1),intersect(rownames(TSRV2),intersect(rownames(TSRV3),rownames(TSRV4))))
  TSRV1=TSRV1[AAA,]
  #TSRV=TSRV[AAA,]
  TSRV2=TSRV2[AAA,]
  TSRV3=TSRV3[AAA,]
  TSRV4=TSRV4[AAA,]
  TSRV=TSRV1+TSRV2+TSRV3+TSRV4



  AAA=intersect(rownames(TSRV),rownames(CSRV))
  AAA=intersect(AAA,rownames(CSRVcv))
  CSRV=CSRV[AAA,]
  CSRV2=CSRV2[AAA,]
  TSRV=TSRV[AAA,]
  CSRVcv=CSRVcv[AAA,]
  CSRVcv2=CSRVcv2[AAA,]


  CSRV=CSRV+CSRV2+CSRVcv+CSRVcv2

  Ret=RET


  if(statrun==1){
    TSRVrun1=TSRV
    CSRVrun1=CSRV

  }
  if(statrun==2){
    TSRVrun2=TSRV
    CSRVrun2=CSRV
  }
}
TSRV=cbind(TSRVrun1,TSRVrun2)[,colnames(CRet)]
CSRV=cbind(CSRVrun1,CSRVrun2)[,colnames(CRet)]

TSRV$mon=as.yearmon(rownames(TSRV))+1/12
start=max(TSRV$mon[1],as.yearmon(rownames(Ret)[1]))

Ret=RETfinal
TSRVtemp=Ret[as.yearmon(rownames(Ret))>=start,]*NA
CSRVtemp=TSRVtemp
for (ss in 1:nrow(TSRV)){
  TSRVtemp[rownames(TSRVtemp)>=rownames(TSRV)[ss],1:ncol(Ret)]=TSRV[ss,1:ncol(Ret)]
  CSRVtemp[rownames(CSRVtemp)>=rownames(CSRV)[ss],1:ncol(Ret)]=CSRV[ss,1:ncol(Ret)]
}
TSRV=TSRVtemp
CSRV=CSRVtemp
Ret=RETfinal


# Run backtesting
source('grp_helpers.R')
functionoutput=factor(TSRV,CSRV,Ret,"month",CSLS=CSdesign,TSWGT,CSWGT,0,CSweek=0,rpname=name1)

# Charting
chart.CumReturns(functionoutput[[5]], main=name1)
chart.CumReturns(functionoutput[[6]], main=paste(name1,"lag"))


write.csv(functionoutput[[5]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\css.csv')
write.csv(functionoutput[[6]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\css_lag.csv')

setwd('C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\grf\\grf')

