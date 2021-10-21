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

# stat6=ECA(RB1=RBP,RB2=RBP,RET=ERet,index=Eindex,CSdesign="notional")
RETfinal=RET

for(statrun in 1:2){
  # why seperate indice?
  if (statrun==1){RET=ERet[,1:10]}
  if (statrun==2){RET=ERet[,11:14]}
  index=Eindex[,colnames(RET)]

  ##RV
  # carry is monthly series
  carry <- as.data.frame(read.csv("./data/carry-dm.csv", row.names=1,header=TRUE))[,colnames(RET)]
  carry[,"DAX"]=NA # why NA DAX?
  # fut1price <- as.data.frame(read.csv("D:/R/GRP/FutGenNone1.csv", row.names=1,header=TRUE))
  # fut2price <- as.data.frame(read.csv("D:/R/GRP/FutGenNone2.csv", row.names=1,header=TRUE))
  # expday <- as.data.frame(read.csv("D:/R/GRP/fut1expiry.csv", row.names=1,header=TRUE))




  minobs1=12
  minobs=60

  Expanding=0

  nopos=0.4##middle section->neutral zone
  MA=c(1,2,3)/6

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


  # carry=(fut1price-fut2price)/fut2price/expday
  fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))
  # carry=carry[rownames(carry)%in%rownames(fx),]
  Ret=RET

  # to monthly
  index=index[rownames(index)%in%rownames(fx),]
  ret=index[2:nrow(index),]/index[1:(nrow(index)-1),]-1


  RV=carry[12:nrow(carry),]


  # MAM=matrix(rep(MA,ncol(Ret)),ncol=ncol(Ret))
  # for(i in length(MA):nrow(index)){
  #   RV[i,]=colSums(carry[(i-length(MA)+1):i,]*MAM)
  #
  # }
  # RV=RV[length(MA):nrow(index),]


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

  # expanding window rank
  for(i in 1:(nrow(RV)-minobs1+1)) {
    RVrank[i,]=apply(RV[1:(minobs1+i-1),],2,rank,na.last="keep")[i+minobs1-1,]/((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))
    RVrank[i,((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))<(minobs1)]=NA
  }#i



  # truecount=round(rowSums(!is.na(RVrank))*CS)
  # truecount=matrix(rep(truecount,ncol(RVrank)),nrow=nrow(RVrank))
  # tiebreaker=as.data.frame(rbind(matrix(0,4,ncol(index)),as.matrix(rollapplyr(RVrank,5,mean)))*0.0000001)
  # CSRV=as.data.frame(t(as.data.frame(apply(RVrank+tiebreaker,1,rank,ties.method="first",na.last="keep"))))
  # CSRV1=as.data.frame(t(as.data.frame(apply(-RVrank-tiebreaker,1,rank,ties.method="last",na.last="keep"))))
  # CSRVpos=CSRV*0
  # CSRVpos[CSRV[,]<=truecount]=-1
  # CSRVpos[CSRV1[,]<=truecount]=1

  RV1=RV[(minobs1):nrow(RV),]
  truecount=round(rowSums(!is.na(RV1))*CS)
  truecount=matrix(rep(truecount,ncol(RV1)),nrow=nrow(RV1))
  CSRV=as.data.frame(t(as.data.frame(apply(RV1,1,rank,ties.method="first",na.last="keep"))))
  CSRV1=as.data.frame(t(as.data.frame(apply(-RV1,1,rank,ties.method="last",na.last="keep"))))
  CSRVpos=CSRV*0
  CSRVpos[CSRV[,]<=truecount]=-1
  CSRVpos[CSRV1[,]<=truecount]=1



  ##Final CS signal(weighted)
  CSRV=CSRVpos
  CSRV[is.na(CSRV)]=0
  #translate to positions

  TSRV=RVrank*0

  TSRV[RVrank[,]>(nopos+(1-nopos)/2)]=1
  TSRV[RVrank[,]<((1-nopos)/2)]=-1
  # TSRV[RVrank[,]>(nopos+(1-nopos)/2)&RV[(minobs1):nrow(RV),]>0]=1
  # TSRV[RVrank[,]<((1-nopos)/2)&RV[(minobs1):nrow(RV),]<0]=-1
  # TSRV[RVrank[,]>0.95]=0
  # TSRV[RVrank[,]<0.05]=0

  # TSRV[RVrank[,]>(nopos+(1-nopos)/2)&RV[(minobs1):nrow(RV),]>0]=1
  # TSRV[RVrank[,]<((1-nopos)/2)&RV[(minobs1):nrow(RV),]<0]=-1

  # up=rep((nopos+(1-nopos)/2),ncol(TSRV))
  # down=rep(((1-nopos)/2),ncol(TSRV))
  #
  # TSRV[1,]=(RVrank[1,]>=(up))*1+-1*(RVrank[1,]<=(down))
  # TSRV[1,is.na(TSRV[1,])]=0
  # for(i in 2:nrow(RVrank)){
  #   TSRV[i,]=(RVrank[i,]>=(up-(TSRV[i-1,]>0)*0.025))*1+(RVrank[i,]<=(down+(TSRV[i-1,]<0)*0.025))*-1
  #   TSRV[i,is.na(TSRV[i,])]=0
  # }
  #


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
functionoutput=factor(TSRV,CSRV,Ret,"month",CSLS=CSdesign,TSWGT,CSWGT,0,CSweek=0,rpname=name1,BETA=betamat)

# Charting
chart.CumReturns(functionoutput[[5]], main=name1)
chart.CumReturns(functionoutput[[6]], main=paste(name1,"lag"))

# write csv
write.csv(functionoutput[[5]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\eca.csv')
write.csv(functionoutput[[6]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\eca_lag.csv')

