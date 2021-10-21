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
eindex1=eindex1[rownames(eindex1)<"2008-01-01",]
ERetp=eindex1[2:nrow(eindex1),]/eindex1[1:(nrow(eindex1)-1),]-1 #p

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
RETfinal=RET

for(statrun in 1:2){
  if (statrun==1){RET=EMRet[,1:4]}
  if (statrun==2){RET=EMRet[,5:10]}
  index=EMindex[,colnames(RET)]



  Ret=RET

  fundwgt=1
  statwgt=1

  minobs=500

  Expanding=1


  short=0.2

  day1=24#rebalance
  day2=2

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
  # SIG2[,1:6]=FOMCEM
  # SIG2[,7:10]=FOMCDM

  TSRV1=SIG*fundwgt*1#+SIG2*0
  if (IDN=="out"){
    TSRV1[,colnames(TSRV1)=="ID"]=NA
  }


  short=0


  CSRV=index*0
  statday=unique(as.yearmon(rownames(Ret))+1/12)
  bible=as.data.frame(matrix(0,nrow=(length(statday)-36),ncol=ncol(index)))
  bible$YM=statday[(36+1-short):(length(statday)-short)]
  bible$mon=month(bible$YM)
  Ret$YM=as.yearmon(rownames(Ret))
  Ret$mon=month(rownames(Ret))
  if (IDN=="out"){
    Ret[,colnames(Ret)=="ID"]=NA
  }

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

  if (IDN=="out"){
    RV1[,colnames(RV1)=="ID"]=NA
  }


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
functionoutput=factor(TSRV,CSRV,Ret,"month",CSLS=CSdesign,TSWGT,CSWGT,0,CSweek=0,rpname=name1,BETA=betamat)

# Charting
chart.CumReturns(functionoutput[[5]], main=name1)
chart.CumReturns(functionoutput[[6]], main=paste(name1,"lag"))


# write csv
write.csv(functionoutput[[5]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\emss.csv')
write.csv(functionoutput[[6]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\emss_lag.csv')


