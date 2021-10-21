name1="tests"

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
RETfinal=RET

for(statrun in 1:2){
  if (statrun==1){RET=CRet[,c("C","S","SB","SM","W","KC","CT")]}
  if (statrun==2){RET=CRet[,!(names(CRet) %in%c("C","S","SB","SM","W","KC","CT"))]}
  index=Cindex[,colnames(RET)]
  
  
  Ret=RET
  compos <- as.data.frame(read.csv("./data/commo pos.csv", row.names=1,header=TRUE))[,colnames(Ret)]
  
  
  minobs1=12
  minobs=60
  
  Expanding=0
  
  nopos=0.4##middle section->neutral zone
  MA=c(1,2,3)/6
  
  CS=CSNUM
  
  
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
  
  fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))
  #compos1=compos[wday(rownames(compos))==RB1,]
  compos1=compos[rownames(compos)%in%rownames(fx),]
  compos1=compos1[rownames(compos1)>"1998-01-01",]
  Ret=RET
  
  
  #RV=-compos1+rowMeans(compos1,na.rm=TRUE)
  Zscore=(compos1[(12):nrow(compos1),]-as.data.frame(rollapplyr(compos1,12,mean)))/as.data.frame(rollapplyr(compos1,12,sd))
  rownames(Zscore)=rownames(compos1)[(12):nrow(compos1)]
  RV=-Zscore
  #RV=-Zscore[2:nrow(Zscore),]+Zscore[1:(nrow(Zscore)-1),]
  #RV[,c("XBW","SM","BO")]=NA
  
  RVrank=RV[(minobs1):nrow(RV),]*0
  
  
  #
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
  
  for(i in 1:(nrow(RV)-minobs1+1)) {
    RVrank[i,]=apply(RV[1:(minobs1+i-1),],2,rank,na.last="keep")[i+minobs1-1,]/((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))
    RVrank[i,((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))<(minobs1)]=NA
  }#i
  
  
  truecount=round(rowSums(!is.na(RVrank))*CS)
  truecount=matrix(rep(truecount,ncol(RVrank)),nrow=nrow(RVrank))
  tiebreaker=as.data.frame(rbind(matrix(0,4,ncol(index)),as.matrix(rollapplyr(RVrank,5,mean)))*0.0000001)
  CSRV=as.data.frame(t(as.data.frame(apply(RVrank+tiebreaker,1,rank,ties.method="first",na.last="keep"))))
  CSRV1=as.data.frame(t(as.data.frame(apply(-RVrank-tiebreaker,1,rank,ties.method="last",na.last="keep"))))
  CSRVpos=CSRV*0
  CSRVpos[CSRV[,]<=truecount]=-1
  CSRVpos[CSRV1[,]<=truecount]=1
  
  # RV1=RV[(minobs1):nrow(RV),]
  # truecount=round(rowSums(!is.na(RV1))*CS)
  # truecount=matrix(rep(truecount,ncol(RV1)),nrow=nrow(RV1))
  # CSRV=as.data.frame(t(as.data.frame(apply(RV1,1,rank,ties.method="first",na.last="keep"))))
  # CSRV1=as.data.frame(t(as.data.frame(apply(-RV1,1,rank,ties.method="last",na.last="keep"))))
  # CSRVpos=CSRV*0
  # CSRVpos[CSRV[,]<=truecount]=-1
  # CSRVpos[CSRV1[,]<=truecount]=1
  
  
  
  ##Final CS signal(weighted)
  CSRVone=CSRVpos
  CSRVone[is.na(CSRVone)]=0
  #translate to positions
  
  
  TSRVone=RVrank*0
  
  TSRVone[RVrank[,]>(nopos+(1-nopos)/2)]=1
  TSRVone[RVrank[,]<((1-nopos)/2)]=-1
  # TSRVone[RV[(minobs1):nrow(RV),]>(2.5)]=1
  # TSRVone[RV[(minobs1):nrow(RV),]<(-2.5)]=-1
  
  #
  #
  # RV=-compos1[2:nrow(compos1),]+rowMeans(compos1[2:nrow(compos1),],na.rm=TRUE)
  #
  # # RV=-compos1[2:nrow(compos1),]+compos1[1:(nrow(compos1)-1),]
  #
  #
  # RVrank=RV[(minobs1):nrow(RV),]*0
  #
  #
  # #
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
  #
  # # for(i in 1:(nrow(RV)-minobs1+1)) {
  # #   RVrank[i,]=apply(RV[1:(minobs1+i-1),],2,rank,na.last="keep")[i+minobs1-1,]/((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))
  # #   RVrank[i,((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))<(minobs1)]=NA
  # # }#i
  #
  #
  # truecount=round(rowSums(!is.na(RVrank))*CS)
  # truecount=matrix(rep(truecount,ncol(RVrank)),nrow=nrow(RVrank))
  # tiebreaker=as.data.frame(rbind(matrix(0,4,ncol(index)),as.matrix(rollapplyr(RVrank,5,mean)))*0.0000001)
  # CSRV=as.data.frame(t(as.data.frame(apply(RVrank+tiebreaker,1,rank,ties.method="first",na.last="keep"))))
  # CSRV1=as.data.frame(t(as.data.frame(apply(-RVrank-tiebreaker,1,rank,ties.method="last",na.last="keep"))))
  # CSRVpos=CSRV*0
  # CSRVpos[CSRV[,]<=truecount]=-1
  # CSRVpos[CSRV1[,]<=truecount]=1
  #
  # # RV1=RV[(minobs1):nrow(RV),]
  # # truecount=round(rowSums(!is.na(RV1))*CS)
  # # truecount=matrix(rep(truecount,ncol(RV1)),nrow=nrow(RV1))
  # # CSRV=as.data.frame(t(as.data.frame(apply(RV1,1,rank,ties.method="first",na.last="keep"))))
  # # CSRV1=as.data.frame(t(as.data.frame(apply(-RV1,1,rank,ties.method="last",na.last="keep"))))
  # # CSRVpos=CSRV*0
  # # CSRVpos[CSRV[,]<=truecount]=-1
  # # CSRVpos[CSRV1[,]<=truecount]=1
  #
  #
  #
  # ##Final CS signal(weighted)
  # CSRVtwo=CSRVpos
  # CSRVtwo[is.na(CSRVtwo)]=0
  # #translate to positions
  #
  #
  # TSRVtwo=RVrank*0
  #
  # TSRVtwo[RVrank[,]>(nopos+(1-nopos)/2)]=1
  # TSRVtwo[RVrank[,]<((1-nopos)/2)]=-1
  
  CSRV=(CSRVone)*0.5
  TSRV=(TSRVone)*0.5
  
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
functionoutput=factor(TSRV,CSRV,Ret,"week",CSLS=CSdesign,TSWGT,CSWGT,0,CSweek=1,rpname=name1)

# Charting
chart.CumReturns(functionoutput[[5]], main=name1)
chart.CumReturns(functionoutput[[6]], main=paste(name1,"lag"))