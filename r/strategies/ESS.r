name1="tests"
setwd("C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\grf\\grf")

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

# stat8=EEM(RB1=RBP,RB2=RBP,RET=ERet,index=Eindex,CSdesign="notional")
RETfinal=RET

for(statrun in 1:2){
  if (statrun==1){RET=ERet[,1:10]}
  if (statrun==2){RET=ERet[,11:14]}
  index=Eindex[,colnames(RET)]
  
  
  fundwgt=1
  statwgt=1
  
  minobs=500
  
  Expanding=1
  
  
  short=0.2
  
  day1=24#rebalance
  day2=7
  
  nopos=0.4##middle section->neutral zone
  MA=c(1,2,3,4,5)/15
  
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
  
  ##carry score
  Ret=RET
  cal <- create.calendar("Actual",weekdays=c("saturday", "sunday"))
  TOM=as.data.frame(day(add.bizdays(rownames(Ret),1,cal)))
  rownames(TOM)=rownames(Ret)
  TOM2=TOM*0-1*short
  
  TOM2[TOM[,1]>=day1,1]=1
  # TOM2[TOM[,1]==1 & wday(rownames(TOM))!=3,1]=1
  # TOM2[TOM[,1]==2 & wday(rownames(TOM))!=3& wday(rownames(TOM))!=4,1]=1
  # TOM2[TOM[,1]==3 & wday(rownames(TOM))!=3& wday(rownames(TOM))!=4& wday(rownames(TOM))!=5,1]=1
  # TOM2[TOM[,1]==4 & wday(rownames(TOM))!=3& wday(rownames(TOM))!=4& wday(rownames(TOM))!=5& wday(rownames(TOM))!=6,1]=1
  # TOM2[TOM[,1]==5 & wday(rownames(TOM))!=3& wday(rownames(TOM))!=4& wday(rownames(TOM))!=5& wday(rownames(TOM))!=6& wday(rownames(TOM))!=7,1]=1
  # TOM2[TOM[,1]==6 & wday(rownames(TOM))==2,1]=1
  
  SIG=Ret*0+1
  SIG=SIG*TOM2[,1]
  
  # FOMC <- as.data.frame(read.csv("D:/R/GRP/FOMC2.csv", row.names=1,header=TRUE))
  # FOMCDM=TOM2*0
  # FOMCEM=TOM2*0
  # FOMCDM[as.Date(rownames(FOMCDM))%in%add.bizdays(rownames(FOMC),-2,cal),1]=1
  # FOMCEM[as.Date(rownames(FOMCEM))%in%add.bizdays(rownames(FOMC),-1,cal),1]=1
  #
  # SIG2=Ret*0
  # SIG2[,1:10]=FOMCDM
  # SIG2[,11:14]=FOMCEM
  #
  TSRV1=SIG*fundwgt#+SIG2
  
  short=0
  
  
  CSRV=index*0
  statday=unique(as.yearmon(rownames(Ret))+1/12)
  bible=as.data.frame(matrix(0,nrow=(length(statday)-36),ncol=ncol(index)))
  bible$YM=statday[(36+1-short):(length(statday)-short)]
  bible$mon=month(bible$YM)
  Ret$YM=as.yearmon(rownames(Ret))
  Ret$mon=month(rownames(Ret))
  
  # for (i in 1:nrow(bible)){
  #   ave=mean(colMeans(Ret[Ret$YM<bible$YM[i]& Ret$YM>=statday[1]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))
  #   bible[i,1:ncol(index)]=(colMeans(Ret[Ret$YM<bible$YM[i]& Ret$YM>=statday[1]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))/colSds(as.matrix(Ret[Ret$YM<bible$YM[i]& Ret$YM>=statday[1]& Ret$mon==bible$mon[i],1:ncol(CSRV)]),na.rm=TRUE)
  #   bible[i,1:ncol(index)]=bible[i,1:ncol(index)]*sqrt(nrow(Ret[Ret$YM<bible$YM[i]& Ret$YM>=statday[1]&Ret$mon==bible$mon[i],1:ncol(index)]))
  # }
  
  for (i in 1:nrow(bible)){
    ave=(colMeans(Ret[Ret$YM<(bible$YM[i]) &Ret$YM>=statday[i]&Ret$mon!=bible$mon[i],1:ncol(index)],na.rm=TRUE))
    bible[i,1:ncol(index)]=(colMeans(Ret[Ret$YM<(bible$YM[i])& Ret$YM>=statday[i]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))/colSds(as.matrix(Ret[Ret$YM<(bible$YM[i])& Ret$YM>=statday[i]& Ret$mon==bible$mon[i],1:ncol(CSRV)]),na.rm=TRUE)
    
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
  
  CSRV=CSRV[as.yearmon(rownames(CSRV))>=bible$YM[1],]
  
  #####need to adjust due to month end rebalancing(get Nov on oct/31)
  for(i in 1:nrow(bible)){
    CSRV[as.yearmon(rownames(CSRV))==(bible$YM[i]-1/12),1:ncol(index)]=bibleRVpos[i,1:ncol(index)]*statwgt
  }
  
  
  bibleTS1=bible[,1:ncol(index)]
  bibleTS=bibleTS1*0
  bibleTS[bibleTS1[]<(-0.5)]=-1
  bibleTS[bibleTS1[]>(0.5)]=1
  TSRV=CSRV*0
  for(i in 1:nrow(bible)){
    TSRV[as.yearmon(rownames(TSRV))==bible$YM[i],1:ncol(index)]=bibleTS[i,1:ncol(index)]
  }
  
  AAA=intersect(rownames(TSRV1),rownames(TSRV))
  TSRV1=TSRV1[AAA,]
  TSRV=TSRV[AAA,]
  
  TSRV=TSRV1*1+TSRV*0
  AAA=intersect(rownames(TSRV),rownames(CSRV))
  CSRV=CSRV[AAA,]
  TSRV=TSRV[AAA,]
  
  
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
for (ss in 1:nrow(TSRV)){
  TSRVtemp[rownames(TSRVtemp)>=rownames(TSRV)[ss],1:ncol(Ret)]=TSRV[ss,1:ncol(Ret)]
  CSRVtemp[rownames(CSRVtemp)>=rownames(CSRV)[ss],1:ncol(Ret)]=CSRV[ss,1:ncol(Ret)]
}
TSRV=TSRVtemp
CSRV=CSRVtemp
Ret=RETfinal

# Run backtesting
source('grp_helpers.R')
functionoutput=factor(TSRV,CSRV,Ret,FALSE,CSLS=CSdesign,TSWGT,CSWGT,0,CSweek=0,rpname=name1,BETA=betamat)

# Charting
chart.CumReturns(functionoutput[[5]], main=name1)
chart.CumReturns(functionoutput[[6]], main=paste(name1,"lag"))

# write csv
write.csv(functionoutput[[5]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\ess.csv')
write.csv(functionoutput[[6]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\ess_lag.csv')
