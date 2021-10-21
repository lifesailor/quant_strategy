library(PerformanceAnalytics)
library(bizdays)
library(zoo)
library(Hmisc)
library(RiskPortfolios)
library(lubridate)
library(qcc)
library(matrixStats)

name1 <- 'test'
IRCSmod=0.8
IRTSmod=0.8
ComCSmod=0.8
ComTSmod=0.8
DMCSmod=1
EMCSmod=1
DMTSmod=1
EMTSmod=1
positions=1
updatepos=0
RBP=3
targetvol=0.0475
#targetvol=0.035
Volband=0.05
elimit=3.8

CSpos=0
CSpostest=0.35
CSNUM=0.35
CSNUM2=0.5
CSNUM3=0.5

TSWGT=1
CSWGT=1
RB1=3
RB2=3
RET = EMRet
index = EMindex
CSdesign = 'notional'
IDN = 'out'




# Read Data from csv & process data
library(gdata)
emindex <- as.data.frame(read.csv("./data/totindex-em.csv", row.names=1,header=TRUE))
emindex1 <- as.data.frame(read.csv("./data/priceindex-em.csv", row.names=1,header=TRUE))
emfut <- as.data.frame(read.csv("./data/fut1return-em.csv", row.names=1,header=TRUE))
rownames(emfut)=as.Date(as.numeric(rownames(emfut)),origin = "1899-12-30")

emindex=emindex[rownames(emindex)<"2013-01-01",]
emindex1=emindex1[rownames(emindex1)<"2013-01-01",]
EMRet=emindex1[2:nrow(emindex1),]/emindex1[1:(nrow(emindex1)-1),]-1
EMTRet=emindex[2:nrow(emindex),]/emindex[1:(nrow(emindex)-1),]-1
fRet=emfut[2:nrow(emfut),]/emfut[1:(nrow(emfut)-1),]-1
fRet=fRet[wday(rownames(fRet))!=1 & wday(rownames(fRet))!=7, ]
compRet=EMRet
compRet[rownames(EMTRet),]=EMTRet[rownames(EMTRet),]
EMRet1=compRet
EMRet=rbind(compRet[rownames(EMRet1)<"2013-01-01",],fRet[rownames(fRet)>="2013-01-01",])

EMRet[is.na(EMRet)]=0
EMindex=EMRet*0
EMindex[1,]=matrix(rep(1,ncol(EMindex)),nrow=1)
for (i in 2:nrow(EMindex)){
  EMindex[i,]=EMindex[(i-1),]*(1+EMRet[i,])
}

TSWGT=1
CSWGT=1
RB1=3
RB2=3
RET <- EMRet
index <- EMindex
CSdesign <- "notional"


RETfinal=RET

for(statrun in 1:2){
  if (statrun==1){RET=EMRet[,1:4]}
  if (statrun==2){RET=EMRet[,5:10]}
  index=EMindex[,colnames(RET)]


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

  RVrank=Mag[(minobs):nrow(Mag),]*0

  RV=Mag


  ##Reliability
  ret=index[2:nrow(index),]/index[1:(nrow(index)-1),]-1

  STDEV=as.data.frame(rollapplyr(ret,longlen,sd))*sqrt(52)
  rownames(STDEV)=rownames(index)[(longlen+1):nrow(index)]
  STDEV1=STDEV[rownames(STDEV)%in%rownames(Mag),]


  RV=Mag#/STDEV1

  if (IDN=="out"){
    RV[,colnames(RV)=="ID"]=NA
  }
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
  if (IDN=="out"){
    RV[,colnames(RV)=="ID"]=NA
  }
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
  if (IDN=="out"){
    RV[,colnames(RV)=="ID"]=NA
  }
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
  if (IDN=="out"){
    RV[,colnames(RV)=="ID"]=NA
  }
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
  if (IDN=="out"){
    RV[,colnames(RV)=="ID"]=NA
  }
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
  if (IDN=="out"){
    RV[,colnames(RV)=="ID"]=NA
  }
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


TSRV=cbind(TSRVrun1,TSRVrun2)[,colnames(EMRet)]
CSRV=cbind(CSRVrun1,CSRVrun2)[,colnames(EMRet)]

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
functionoutput=factor(TSRV,CSRV,Ret,"month",CSLS=CSdesign,TSWGT,CSWGT,0,CSweek=1,rpname=name1)

# Charting
chart.CumReturns(functionoutput[[5]], main=name1)
chart.CumReturns(functionoutput[[6]], main=paste(name1,"lag"))


# write csv
write.csv(functionoutput[[5]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\empm.csv')
write.csv(functionoutput[[6]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\empm_lag.csv')

