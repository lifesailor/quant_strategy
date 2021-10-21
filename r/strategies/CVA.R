name1="tests"

# Global Strategies Setting
library(zoo)
library(lubridate)
library(PerformanceAnalytics)

CSNUM=0.35

# Factor Settings
TSWGT=1
CSWGT=1
#setwd("D:/R/GRP") commented by k
RB1=3
RB2=3
RBP=3
IDN="out"
CSdesign="vol"

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

RET=CRet
index=Cindex

##RV
Ret=RET

RETfinal=RET

# Creating Sginals
for(statrun in 1:2){
  # Make two groups
  if (statrun==1){RET=CRet[,c("C","S","SB","SM","W","KC","CT")]}
  if (statrun==2){RET=CRet[,!(names(CRet) %in%c("C","S","SB","SM","W","KC","CT"))]}

  index=Cindex[,colnames(RET)]

  minobs1=52
  minobs=260

  Expanding=0

  nopos=0.6 # middle section -> neutral zone for Time Series
  MA=c(1,2,3)/6

  CS=CSNUM # assets to long and short, ratio of number of assets for cross sectional


  #Factor Portfolio construction settings
  Assetvol=0.02
  Strategyvol=0.02
  factorvol=0.02
  factorsd=260
  assetsd=260
  statsd=260
  ##volupdate trigger
  Volband=0.1

  # Signal Part
  Ret=RET
  Retpos=Ret
  Retneg=Ret
  Retpos[Ret<0]=NA
  Retneg[Ret>0]=NA

  per=63
  STDpos=as.data.frame(rollapplyr(Retpos,per,sd,na.rm=TRUE))
  STDneg=as.data.frame(rollapplyr(Retneg,per,sd,na.rm=TRUE))

  rownames(STDpos)=rownames(Retpos)[(per):nrow(Retpos)]
  rownames(STDneg)=rownames(Retpos)[(per):nrow(Retpos)]

  RV=-STDpos+STDneg
  #RV=(-STDpos+STDneg)/(STDpos+STDneg)

  RV=RV[wday(rownames(RV))==RB1,]

  RVrank=RV[(minobs1):nrow(RV),]*0


  # for(i in 1:(minobs-minobs1)) {
  #   RVrank[i,]=apply(RV[1:(minobs1+i-1),],2,rank,na.last="keep")[i+minobs1-1,]/((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))
  #   RVrank[i,((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))<(minobs1)]=NA
  # }#i
  #
  # for(i in (minobs-minobs1+1):(nrow(RV)-minobs1+1)) {
  #
  #   RVrank[i,]=apply(RV[(i-(minobs-minobs1)):(minobs1+i-1),],2,rank,na.last="keep")[minobs,]/((minobs)-colSums(is.na(RV[(i-(minobs-minobs1)):(minobs1+i-1),])))
  #   RVrank[i,((minobs)-colSums(is.na(RV[(i-(minobs-minobs1)):(minobs1+i-1),])))<(minobs1)]=NA
  # }#i

  # Calculating expanding window Time Series Ranking
  for(i in 1:(nrow(RV)-minobs1+1)) {
    RVrank[i,]=apply(RV[1:(minobs1+i-1),],2,rank,na.last="keep")[i+minobs1-1,]/((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))
    RVrank[i,((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))<(minobs1)]=NA # Make NA for minimum observation dates
  }#i

  # Signal based on Time Series Ranking / 'nopos' is neutral zone for time series
  TSRV=RVrank*0
  TSRV[RVrank[,]>(nopos+(1-nopos)/2)]=1
  TSRV[RVrank[,]<((1-nopos)/2)]=-1

  # truecount=round(rowSums(!is.na(RVrank))*CS)
  # truecount=matrix(rep(truecount,ncol(RVrank)),nrow=nrow(RVrank))
  # tiebreaker=as.data.frame(rbind(matrix(0,4,ncol(index)),as.matrix(rollapplyr(RVrank,5,mean)))*0.0000001)
  # CSRV=as.data.frame(t(as.data.frame(apply(RVrank+tiebreaker,1,rank,ties.method="first",na.last="keep"))))
  # CSRV1=as.data.frame(t(as.data.frame(apply(-RVrank-tiebreaker,1,rank,ties.method="last",na.last="keep"))))
  # CSRVpos=CSRV*0
  # CSRVpos[CSRV[,]<=truecount]=-1
  # CSRVpos[CSRV1[,]<=truecount]=1

  # Signal based on Cross Sectional ranking
  RV1=RV[(minobs1):nrow(RV),]
  truecount=round(rowSums(!is.na(RV1))*CS) # number of assets to long/short
  truecount=matrix(rep(truecount,ncol(RV1)),nrow=nrow(RV1))
  CSRV=as.data.frame(t(as.data.frame(apply(RV1,1,rank,ties.method="first",na.last="keep"))))
  CSRV1=as.data.frame(t(as.data.frame(apply(-RV1,1,rank,ties.method="last",na.last="keep"))))
  CSRVpos=CSRV*0
  CSRVpos[CSRV[,]<=truecount]=-1
  CSRVpos[CSRV1[,]<=truecount]=1
  rownames(CSRVpos)=rownames(RV1)

  ##Final CS signal(weighted)
  CSRV=CSRVpos
  CSRV[is.na(CSRV)]=0

  #translate to positions

  if(statrun==1){
    TSRVrun1=TSRV
    CSRVrun1=CSRV

  }
  if(statrun==2){
    TSRVrun2=TSRV
    CSRVrun2=CSRV
  }
}

# Combining Signal of two groups
TSRV=cbind(TSRVrun1,TSRVrun2)[,colnames(CRet)]
CSRV=cbind(CSRVrun1,CSRVrun2)[,colnames(CRet)]

TSRV$mon=as.yearmon(rownames(TSRV))+1/12
start=max(TSRV$mon[1],as.yearmon(rownames(Ret)[1]))

# Align signal series to Ret series
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
#functionoutput=factor(TSRV,CSRV,Ret,"month",CSLS=CSdesign,TSWGT,CSWGT,0,CSweek=0,rpname=name1)
functionoutput=factor(TSRV,CSRV,Ret,"week",CSLS=CSdesign,TSWGT,CSWGT,0,CSweek=1,rpname=name1)

# Charting
chart.CumReturns(functionoutput[[5]], main=name1)
chart.CumReturns(functionoutput[[6]], main=paste(name1,"lag"))

# Output
# output=functionoutput[[2]]
TStest=functionoutput[[1]]
CStest=functionoutput[[2]]
CSTO=sum(rowSums(abs(CStest[2:nrow(CStest),]-CStest[1:(nrow(CStest)-1),]),na.rm=TRUE))
TSTO=sum(rowSums(abs(TStest[2:nrow(TStest),]-TStest[1:(nrow(TStest)-1),]),na.rm=TRUE))
print(CSTO)
print(sum(RAWlag(CStest,Ret)))
print(sum(RAW(CStest,Ret)))
print(CSTO/sum(RAW(CStest,Ret))/100)
print(TSTO)
print(sum(RAWlag(TStest,Ret)))
print(sum(RAW(TStest,Ret)))
#write.csv(RAWlag(TStest,Ret),"TS.csv")
#write.csv(RAWlag(CStest,Ret),"CS.csv")
#write.csv(functionoutput[[4]],"CSpos.csv")
#write.csv(functionoutput[[3]],"TSpos.csv")
print(colSums(CSRV))
print(colSums(TSRV))


write.csv(functionoutput[[5]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\cva.csv')
write.csv(functionoutput[[6]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\cva_lag.csv')
