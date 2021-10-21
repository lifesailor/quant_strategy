name1="tests"

# Global Strategies Setting
library(zoo)
library(lubridate)
library(PerformanceAnalytics)
library(bizdays)
library(matrixStats)

# read data from two source & process them for comdty returns
eindex <- as.data.frame(read.csv("./data/totindex.csv", row.names=1,header=TRUE))
eindex1 <- as.data.frame(read.csv("./data/priceindex.csv", row.names=1,header=TRUE))
efut <- as.data.frame(read.csv("./data/FutGenratio1.csv", row.names=1,header=TRUE))
rownames(efut)=as.Date(as.numeric(rownames(efut)),origin = "1899-12-30")

eindex1=eindex1[rownames(eindex1)<"2008-01-01",]
ERetp=eindex1[2:nrow(eindex1),]/eindex1[1:(nrow(eindex1)-1),]-1 #p
eindex=eindex[rownames(eindex)<"2008-01-01",]
ERett=eindex[2:nrow(eindex),]/eindex[1:(nrow(eindex)-1),]-1 #
fRet=efut[2:nrow(efut),]/efut[1:(nrow(efut)-1),]-1

compRet=ERett

compRet[is.na(compRet)]=ERetp[is.na(compRet)]
ERet1=compRet
ERet=rbind(compRet[rownames(ERet1)<"2008-01-01",],fRet[rownames(fRet)>="2008-01-01",])

# from return data, making index
ERet[is.na(ERet)]=0
Eindex=ERet*0
Eindex[1,]=matrix(rep(1,ncol(Eindex)),nrow=1)
for (i in 2:nrow(Eindex)){
  Eindex[i,]=Eindex[(i-1),]*(1+ERet[i,])
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
RET=ERet
index=Eindex
CSdesign="notional"

# stat13=EPM(RB1=RBP,RB2=RBP,RET=ERet,index=Eindex,CSdesign="notional")

RETfinal=RET

for(statrun in 1:2){
  if (statrun==1){RET=ERet[,1:10]}
  if (statrun==2){RET=ERet[,11:14]}
  index=Eindex[,colnames(RET)]

  ## Momentum

  fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))
  index=index[wday(rownames(index))==RB1,]

  minobs1=52
  minobs=260
  longlen=52
  shortlen=2
  Expanding=0

  nopos=0.4##middle section->neutral zone
  SDEV=12

  # WGT=c(0,0,0,1)
  # WGT2=WGT

  WGT=c(1/3,1/3,1/3)
  WGT2=c(1,1,0)/3

  CS=CSNUM # assets to long and short

  #Factor Portfolio construction
  Assetvol=0.02
  Strategyvol=0.02
  factorvol=0.02
  factorsd=260
  assetsd=260
  statsd=260
  ##volupdate trigger
  Volband=0.1


  ##Magnitude
  Ret=RET
  Mag=index[(longlen+1):nrow(index),]
  obs=nrow(Mag)-1
  Mag=index[(1+longlen-shortlen):(1+longlen-shortlen+obs),]/index[(1):(1+obs),]-1

  rownames(Mag)=rownames(index)[(longlen+1):nrow(index)]

  # signal 1. 2 week ago 52 - 2 week return
  RVrank=Mag[(minobs):nrow(Mag),]*0
  RV=Mag


  ##Reliability
  ret=index[2:nrow(index),]/index[1:(nrow(index)-1),]-1

  STDEV=as.data.frame(rollapplyr(ret,longlen,sd))*sqrt(52)
  rownames(STDEV)=rownames(index)[(longlen+1):nrow(index)]
  STDEV1=STDEV[rownames(STDEV)%in%rownames(Mag),]


  RV=Mag#/STDEV1


  # STRV=-(index[2:nrow(index),]/index[1:(nrow(index)-1),]-1)
  #
  #
  # RV=STRV[52:nrow(STRV),]


  RVrank=RV[(minobs1):nrow(RV),]*0

  RV1=(RV[(minobs1):nrow(RV),])
  truecount=round(rowSums(!is.na(RV1))*CS)
  truecount=matrix(rep(truecount,ncol(RV1)),nrow=nrow(RV1))
  CSRV=as.data.frame(t(as.data.frame(apply(RV1,1,rank,ties.method="first",na.last="keep"))))
  CSRV1=as.data.frame(t(as.data.frame(apply(-RV1,1,rank,ties.method="last",na.last="keep"))))
  CSRVpos=CSRV*0
  CSRVpos[CSRV[,]<=truecount]=-1
  CSRVpos[CSRV1[,]<=truecount]=1


  Relrank=RVrank
  CSRelpos=CSRVpos

  # check if signal 1. is greater or less than 0
  TSRel=RV[(minobs1):nrow(RV),]
  TS1=TSRel*0
  TS1[TSRel[,]>(0)]=1
  TS1[TSRel[,]<(-0)]=-1
  #
  # TS1=TSRel*0
  # TS1[TSRel[,]>0.5]=1
  # TS1[TSRel[,]<(0.5)]=-1

  # signal 2. percentage of up days
  up=ret*0
  up[ret[,]>=0]=1
  Conroll=rollapplyr(up,longlen-shortlen,sum)/(longlen-shortlen)
  rownames(Conroll)=rownames(ret)[(longlen-shortlen):nrow(ret)]
  RV=as.data.frame(Conroll[1:(nrow(Conroll)-shortlen),])
  rownames(RV)=rownames(Mag)

  RVrank=RV[(minobs1):nrow(RV),]*0

  RV1=(RV[(minobs1):nrow(RV),])
  truecount=round(rowSums(!is.na(RV1))*CS)
  truecount=matrix(rep(truecount,ncol(RV1)),nrow=nrow(RV1))
  CSRV=as.data.frame(t(as.data.frame(apply(RV1,1,rank,ties.method="first",na.last="keep"))))
  CSRV1=as.data.frame(t(as.data.frame(apply(-RV1,1,rank,ties.method="last",na.last="keep"))))
  CSRVpos=CSRV*0
  CSRVpos[CSRV[,]<=truecount]=-1
  CSRVpos[CSRV1[,]<=truecount]=1


  Relrank=RVrank
  CSConpos=CSRVpos
  TSRel=RV[(minobs1):nrow(RV),]

  # TS1=TSRel*0
  # TS1[TSRel[,]>(0)]=1
  # TS1[TSRel[,]<(-0)]=-1
  #
  TS2=TSRel*0
  TS2[TSRel[,]>0.5]=1
  TS2[TSRel[,]<(0.5)]=-1


  ####Final Position
  TSRVL=TS1*WGT[1]+TS2*WGT[2]
  CSRVL=CSRelpos*WGT[1]+CSConpos*WGT2[2]
  #CSRV=CSConpos*WGT2[2]+CSRelpos*WGT2[1]



  ##3shortterm momentum
  longlen=13
  ##Magnitude
  Ret=RET
  Mag=index[(longlen+1):nrow(index),]
  obs=nrow(Mag)-1
  Mag=index[(1+longlen-shortlen):(1+longlen-shortlen+obs),]/index[(1):(1+obs),]-1

  rownames(Mag)=rownames(index)[(longlen+1):nrow(index)]

  RVrank=Mag[(minobs):nrow(Mag),]*0

  RV=Mag


  ##Reliability
  ret=index[2:nrow(index),]/index[1:(nrow(index)-1),]-1

  STDEV=as.data.frame(rollapplyr(ret,longlen,sd))*sqrt(52)
  rownames(STDEV)=rownames(index)[(longlen+1):nrow(index)]
  STDEV1=STDEV[rownames(STDEV)%in%rownames(Mag),]


  RV=Mag#/STDEV1

  RVrank=RV[(minobs1):nrow(RV),]*0

  RV1=(RV[(minobs1):nrow(RV),])
  truecount=round(rowSums(!is.na(RV1))*CS)
  truecount=matrix(rep(truecount,ncol(RV1)),nrow=nrow(RV1))
  CSRV=as.data.frame(t(as.data.frame(apply(RV1,1,rank,ties.method="first",na.last="keep"))))
  CSRV1=as.data.frame(t(as.data.frame(apply(-RV1,1,rank,ties.method="last",na.last="keep"))))
  CSRVpos=CSRV*0
  CSRVpos[CSRV[,]<=truecount]=-1
  CSRVpos[CSRV1[,]<=truecount]=1


  Relrank=RVrank
  CSRelpos=CSRVpos
  TSRel=RV[(minobs1):nrow(RV),]

  TS1=TSRel*0
  TS1[TSRel[,]>(0)]=1
  TS1[TSRel[,]<(-0)]=-1
  #
  # TS1=TSRel*0
  # TS1[TSRel[,]>0.5]=1
  # TS1[TSRel[,]<(0.5)]=-1




  up=ret*0
  up[ret[,]>=0]=1
  Conroll=rollapplyr(up,longlen-shortlen,sum)/(longlen-shortlen)
  rownames(Conroll)=rownames(ret)[(longlen-shortlen):nrow(ret)]
  RV=as.data.frame(Conroll[1:(nrow(Conroll)-shortlen),])
  rownames(RV)=rownames(Mag)

  RVrank=RV[(minobs1):nrow(RV),]*0

  RV1=(RV[(minobs1):nrow(RV),])
  truecount=round(rowSums(!is.na(RV1))*CS)
  truecount=matrix(rep(truecount,ncol(RV1)),nrow=nrow(RV1))
  CSRV=as.data.frame(t(as.data.frame(apply(RV1,1,rank,ties.method="first",na.last="keep"))))
  CSRV1=as.data.frame(t(as.data.frame(apply(-RV1,1,rank,ties.method="last",na.last="keep"))))
  CSRVpos=CSRV*0
  CSRVpos[CSRV[,]<=truecount]=-1
  CSRVpos[CSRV1[,]<=truecount]=1


  Relrank=RVrank
  CSConpos=CSRVpos
  TSRel=RV[(minobs1):nrow(RV),]

  # TS1=TSRel*0
  # TS1[TSRel[,]>(0)]=1
  # TS1[TSRel[,]<(-0)]=-1
  #
  TS2=TSRel*0
  TS2[TSRel[,]>0.5]=1
  TS2[TSRel[,]<(0.5)]=-1



  STRV=-(index[5:nrow(index),]/index[1:(nrow(index)-4),]-1)
  #
  #
  RV=STRV[52:nrow(STRV),]

  RVrank=RV[(minobs1):nrow(RV),]*0

  RV1=(RV[(minobs1):nrow(RV),])
  truecount=round(rowSums(!is.na(RV1))*CS)
  truecount=matrix(rep(truecount,ncol(RV1)),nrow=nrow(RV1))
  CSRV=as.data.frame(t(as.data.frame(apply(RV1,1,rank,ties.method="first",na.last="keep"))))
  CSRV1=as.data.frame(t(as.data.frame(apply(-RV1,1,rank,ties.method="last",na.last="keep"))))
  CSRVpos=CSRV*0
  CSRVpos[CSRV[,]<=truecount]=-1
  CSRVpos[CSRV1[,]<=truecount]=1


  Relrank=RVrank
  CSREVpos=CSRVpos
  STRV=-(index[2:nrow(index),]/index[1:(nrow(index)-1),]-1)
  #
  #
  RV=STRV[52:nrow(STRV),]

  RVrank=RV[(minobs1):nrow(RV),]*0

  RV1=(RV[(minobs1):nrow(RV),])
  truecount=round(rowSums(!is.na(RV1))*CS)
  truecount=matrix(rep(truecount,ncol(RV1)),nrow=nrow(RV1))
  CSRV=as.data.frame(t(as.data.frame(apply(RV1,1,rank,ties.method="first",na.last="keep"))))
  CSRV1=as.data.frame(t(as.data.frame(apply(-RV1,1,rank,ties.method="last",na.last="keep"))))
  CSRVpos=CSRV*0
  CSRVpos[CSRV[,]<=truecount]=-1
  CSRVpos[CSRV1[,]<=truecount]=1


  Relrank=RVrank
  CSREV2pos=CSRVpos


  ####Final Position
  TSRVSh=TS1*WGT[1]+TS2*WGT[2]
  CSRVSh=CSREVpos*0.5+CSREV2pos[rownames(CSREVpos),]



  TSRV=TSRVSh[rownames(TSRVL),]*0.5+TSRVL*1

  err<- as.data.frame(read.csv("./data/EPS.csv", row.names=1,header=TRUE))
  err3=err[2:nrow(err),]/err[1:(nrow(err)-1),]-1
  indexmon=Eindex[rownames(err),]
  retmon=indexmon[2:nrow(indexmon),]/indexmon[1:(nrow(indexmon)-1),]-1
  RV=err3-retmon
  RV1=RV[(minobs1):nrow(RV),]
  truecount=round(rowSums(!is.na(RV1))*CS)
  truecount=matrix(rep(truecount,ncol(RV1)),nrow=nrow(RV1))
  CSRV=as.data.frame(t(as.data.frame(apply(RV1,1,rank,ties.method="first",na.last="keep"))))
  CSRV1=as.data.frame(t(as.data.frame(apply(-RV1,1,rank,ties.method="last",na.last="keep"))))
  CSRVpos=CSRV*0
  CSRVpos[CSRV[,]<=truecount]=-1
  CSRVpos[CSRV1[,]<=truecount]=1

  CSRV=CSRVpos
  CSRV[is.na(CSRV)]=0
  CSRVmonthly=CSRV


  CSRV=CSRVSh
  TSRV=TSRV[rownames(CSRV),]





  if(statrun==1){
    TSRVrun1=TSRV
    CSRVrun1=CSRV

  }
  if(statrun==2){
    TSRVrun2=TSRV
    CSRVrun2=CSRV
  }
}
TSRV=cbind(TSRVrun1,TSRVrun2)[,colnames(ERet)]
CSRV=cbind(CSRVrun1,CSRVrun2)[,colnames(ERet)]

TSRV$mon=as.yearmon(rownames(TSRV))+1/12
start=max(TSRV$mon[1],as.yearmon(rownames(Ret)[1]))

Ret=RETfinal
TSRVtemp=Ret[as.yearmon(rownames(Ret))>=start,]*NA
CSRVtemp=TSRVtemp
CSRVtemp2=TSRVtemp
for (ss in 1:nrow(TSRV)){
  TSRVtemp[rownames(TSRVtemp)>=rownames(TSRV)[ss],1:ncol(Ret)]=TSRV[ss,1:ncol(Ret)]
  CSRVtemp[rownames(CSRVtemp)>=rownames(CSRV)[ss],1:ncol(Ret)]=CSRV[ss,1:ncol(Ret)]

}
CSRVtemp2[is.na(CSRVtemp2)]=0
for (sss in 1:nrow(CSRVmonthly)){
  CSRVtemp2[rownames(CSRVtemp2)>=rownames(CSRVmonthly)[sss],1:ncol(Ret)]=CSRVmonthly[sss,1:ncol(Ret)]
}
TSRV=TSRVtemp






CSRV=CSRVtemp*0.5+CSRVtemp2


Ret=RETfinal

# Run backtesting
source('grp_helpers.R')
functionoutput=factor(TSRV,CSRV,Ret,"week",CSLS=CSdesign,TSWGT,CSWGT,0,CSweek=1,rpname=name1,BETA=betamat)

# Charting
chart.CumReturns(functionoutput[[5]], main=name1)
chart.CumReturns(functionoutput[[6]], main=paste(name1,"lag"))
