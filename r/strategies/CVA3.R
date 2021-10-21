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

# CVA3=function(TSWGT=1,CSWGT=1,RB1=3,RB2=3,RET,index,CSdesign){
RETfinal=RET

for(statrun in 1:2){
  if (statrun==1){RET=CRet[,c("C","S","SB","SM","W","KC","CT")]}
  if (statrun==2){RET=CRet[,!(names(CRet) %in%c("C","S","SB","SM","W","KC","CT"))]}
  index=Cindex[,colnames(RET)]

  Ret=RET
  fut1price<- as.data.frame(read.csv("./data/fut1price-com.csv", row.names=1,header=TRUE))[,colnames(Ret)]
  fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))
  # to monthly
  fut1price=fut1price[rownames(fut1price)%in%rownames(fx),]


  minobs1=12
  minobs=60

  Expanding=0

  nopos=0.4##middle section->neutral zone
  MA=c(1,2,3)/6

  CS=CSNUM
  if(CSpos==1){CS=CSpostest}
  SMA=1
  LMA=12
  Lwindow=54
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



  shortma=rollapplyr(fut1price,SMA,mean)
  rownames(shortma)=rownames(fut1price)[SMA:nrow(fut1price)]
  longma=rollapplyr(fut1price,LMA,mean)
  rownames(longma)=rownames(fut1price)[LMA:nrow(fut1price)]

  longma1=longma[1:(nrow(longma)-Lwindow+1),]
  rownames(longma1)=rownames(longma)[Lwindow:nrow(longma)]
  shortma=shortma[rownames(shortma)%in%rownames(longma1),]
  # ratio of long ma price to short ma price
  Rvalue=(longma1/shortma)-1

  # substract average of all other comdty
  RV=(Rvalue-rowMeans(Rvalue))

  RVrank=RV[(minobs1):nrow(RV),]*0


  # expanding moving window rank -> it is not used
  for(i in 1:(minobs-minobs1)) {
    RVrank[i,]=apply(RV[1:(minobs1+i-1),],2,rank,na.last="keep")[i+minobs1-1,]/((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))
    RVrank[i,((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))<(minobs1)]=NA
  }#i
  # 60 month rolling window rank
  for(i in (minobs-minobs1+1):(nrow(RV)-minobs1+1)) {
    RVrank[i,]=apply(RV[(i-(minobs-minobs1)):(minobs1+i-1),],2,rank,na.last="keep")[minobs,]/((minobs)-colSums(is.na(RV[(i-(minobs-minobs1)):(minobs1+i-1),])))
    RVrank[i,((minobs)-colSums(is.na(RV[(i-(minobs-minobs1)):(minobs1+i-1),])))<(minobs1)]=NA
  }#i

  # for(i in 1:(nrow(RV)-minobs1+1)) {
  #   RVrank[i,]=apply(RV[1:(minobs1+i-1),],2,rank,na.last="keep")[i+minobs1-1,]/((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))
  #   RVrank[i,((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))<(minobs1)]=NA
  # }#i

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
  rownames(CSRVpos)=rownames(RV1)


  ##Final CS signal(weighted)
  CSRV=CSRVpos
  CSRV[is.na(CSRV)]=0
  #translate to positions


  TSRV=as.data.frame(RVrank*0)
  TSRV[RVrank[,]>(nopos+(1-nopos)/2)]=1
  TSRV[RVrank[,]<((1-nopos)/2)]=-1

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


write.csv(functionoutput[[5]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\cva3.csv')
write.csv(functionoutput[[6]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\cva3_lag.csv')

