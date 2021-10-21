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

# stat1=CCA3(RB1=RBP,RB2=RBP,RET=CRet,index=Cindex,CSdesign="vol")
RETfinal=RET

for(statrun in 1:2){
  if (statrun==1){RET=CRet[,c("C","S","SB","SM","W","KC","CT")]}
  if (statrun==2){RET=CRet[,!(names(CRet) %in%c("C","S","SB","SM","W","KC","CT"))]}
  index=Cindex[,colnames(RET)]

  ##RV
  #setwd("D:/R/GRP")
  minobs1=12
  minobs=60
  Expanding=0

  nopos=0.4##middle section->neutral zone
  MA=c(1,2,3)/6
  minobs1=52
  minobs=260

  Expanding=0

  nopos=0.4##middle section->neutral zone
  MA=c(1,2,3)/6

  CS=CSNUM # assets to long and short
  if(CSpos==1){CS=CSpostest}

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

  carry <- as.data.frame(read.csv("./data/carry-com.csv", row.names=1,header=TRUE))[,colnames(Ret)] # it is monthly Series
  # fut1price <- as.data.frame(read.csv("D:/R/GRP/fut1price-com.csv", row.names=1,header=TRUE))
  # fut2price <- as.data.frame(read.csv("D:/R/GRP/fut2price-com.csv", row.names=1,header=TRUE))
  # expday <- as.data.frame(read.csv("D:/R/GRP/days-com.csv", row.names=1,header=TRUE))

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
  # fx<- as.data.frame(read.csv("D:/R/GRP/fx.csv", row.names=1,header=TRUE))
  # carry=carry[rownames(carry)%in%rownames(fx),]
  #carry=carry[wday(rownames(carry))==RB1,]
  Ret=RET

  lag=0
  RV0=carry[(4):(nrow(carry)-lag),]
  rownames(RV0)=rownames(carry)[(1+lag+3):nrow(carry)]
  lag=1
  RV1=carry[(3):(nrow(carry)-lag),]
  rownames(RV1)=rownames(carry)[(1+lag+2):nrow(carry)]
  lag=2
  RV2=carry[(2):(nrow(carry)-lag),]
  rownames(RV2)=rownames(carry)[(1+lag+1):nrow(carry)]
  lag=3
  RV3=carry[(1):(nrow(carry)-lag),]
  rownames(RV3)=rownames(carry)[(1+lag):nrow(carry)]

  RV=RV0+RV1 # this month carry + last month carry

  RVrank=RV[(minobs1):nrow(RV),]*0

  # until 60 month, expanding window ranking
  for(i in 1:(minobs-minobs1)) {
    RVrank[i,]=apply(RV[1:(minobs1+i-1),],2,rank,na.last="keep")[i+minobs1-1,]/((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))
    RVrank[i,((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))<(minobs1)]=NA
  }#i

  # from 60 month, 60 month rolling window ranking.
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



  ##Final CS signal(weighted)
  CSRV=CSRVpos

  CSRV[is.na(CSRV)]=0
  #translate to positions


  TSRV=RVrank*0
  TSRV[RVrank[,]>(nopos+(1-nopos)/2)]=1
  TSRV[RVrank[,]<((1-nopos)/2)]=-1

  # RVrank=RV[(minobs1):nrow(RV),]
  # TSRV[RVrank[,]>(0)]=1
  # TSRV[RVrank[,]<(0)]=-1
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



  TSRV$mon=as.yearmon(rownames(TSRV))+1/12
  start=max(TSRV$mon[1],as.yearmon(rownames(Ret)[1]))
  TSRVtemp=Ret[as.yearmon(rownames(Ret))>=start,]*NA
  CSRVtemp=TSRVtemp
  for (ss in 1:nrow(TSRV)){
    TSRVtemp[rownames(TSRVtemp)>=rownames(TSRV)[ss],1:ncol(Ret)]=TSRV[ss,1:ncol(Ret)]
    CSRVtemp[rownames(CSRVtemp)>=rownames(CSRV)[ss],1:ncol(Ret)]=CSRV[ss,1:ncol(Ret)]
  }
  TSRVone=TSRVtemp
  CSRVone=CSRVtemp
  Ret=RET


  ##RV

  carry <- as.data.frame(read.csv("./data/carry-com2.csv", row.names=1,header=TRUE))[,colnames(Ret)]
  # fut1price <- as.data.frame(read.csv("D:/R/GRP/fut1price-com.csv", row.names=1,header=TRUE))
  # fut2price <- as.data.frame(read.csv("D:/R/GRP/fut2price-com.csv", row.names=1,header=TRUE))
  # expday <- as.data.frame(read.csv("D:/R/GRP/days-com.csv", row.names=1,header=TRUE))

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
  # fx<- as.data.frame(read.csv("D:/R/GRP/fx.csv", row.names=1,header=TRUE))
  # carry=carry[rownames(carry)%in%rownames(fx),]
  #carry=carry[wday(rownames(carry))==RB1,]
  Ret=RET




  lag=0
  RV0=carry[(4):(nrow(carry)-lag),]
  rownames(RV0)=rownames(carry)[(1+lag+3):nrow(carry)]
  lag=1
  RV1=carry[(3):(nrow(carry)-lag),]
  rownames(RV1)=rownames(carry)[(1+lag+2):nrow(carry)]
  lag=2
  RV2=carry[(2):(nrow(carry)-lag),]
  rownames(RV2)=rownames(carry)[(1+lag+1):nrow(carry)]
  lag=3
  RV3=carry[(1):(nrow(carry)-lag),]
  rownames(RV3)=rownames(carry)[(1+lag):nrow(carry)]



  RV=RV0+RV1


  RVrank=RV[(minobs1):nrow(RV),]*0



  for(i in 1:(minobs-minobs1)) {
    RVrank[i,]=apply(RV[1:(minobs1+i-1),],2,rank,na.last="keep")[i+minobs1-1,]/((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))
    RVrank[i,((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))<(minobs1)]=NA
  }#i

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



  ##Final CS signal(weighted)
  CSRV=CSRVpos

  CSRV[is.na(CSRV)]=0
  #translate to positions

  RVrank=RV[(minobs1):nrow(RV),]
  TSRV=RVrank*0

  TSRV[RVrank[,]>(0)]=1
  TSRV[RVrank[,]<(0)]=-1
  # TSRV[RV1[,]>(0)]=1
  # TSRV[RV1[,]<(0)]=-1
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

  TSRV$mon=as.yearmon(rownames(TSRV))+1/12
  start=max(TSRV$mon[1],as.yearmon(rownames(Ret)[1]))
  TSRVtemp=Ret[as.yearmon(rownames(Ret))>=start,]*NA
  CSRVtemp=TSRVtemp
  for (ss in 1:nrow(TSRV)){
    TSRVtemp[rownames(TSRVtemp)>=rownames(TSRV)[ss],1:ncol(Ret)]=TSRV[ss,1:ncol(Ret)]
    CSRVtemp[rownames(CSRVtemp)>=rownames(CSRV)[ss],1:ncol(Ret)]=CSRV[ss,1:ncol(Ret)]
  }
  TSRVtwo=TSRVtemp
  CSRVtwo=CSRVtemp
  Ret=RET

  TSRV=TSRVone[intersect(rownames(TSRVone),rownames(TSRVtwo)),]*0.5+TSRVtwo[intersect(rownames(TSRVone),rownames(TSRVtwo)),]
  CSRV=CSRVone[intersect(rownames(CSRVone),rownames(CSRVtwo)),]*0.5+CSRVtwo[intersect(rownames(CSRVone),rownames(CSRVtwo)),]



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
Ret=RETfinal


# Run backtesting
source('grp_helpers.R')
functionoutput=factor(TSRV,CSRV,Ret,"month",CSLS=CSdesign,TSWGT,CSWGT,0,CSweek=0,IR=0,rpname=name1)

# Charting
chart.CumReturns(functionoutput[[5]], main=name1)
chart.CumReturns(functionoutput[[6]], main=paste(name1,"lag"))

write.csv(functionoutput[[5]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\cca3.csv')
write.csv(functionoutput[[6]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\cca3_lag.csv')
