name1="tests"
JGB="out"

# Global Strategies Setting
library(zoo)
library(lubridate)
library(PerformanceAnalytics)
library(bizdays)
library(matrixStats)

# read data
# bond
bond <- as.data.frame(read.csv("./data/bonds.csv", row.names=1,header=TRUE))
rownames(bond)=as.Date(as.numeric(rownames(bond)),origin = "1899-12-30")
BRet=bond[2:nrow(bond),]/bond[1:(nrow(bond)-1),]-1
if(JGB=="out"){
  BRet=BRet[,1:4]
  bond=bond[,1:4]
}

# from return data, making index
# bond
Bindex=BRet*0
Bindex[1,]=matrix(rep(1,ncol(Bindex)),nrow=1)
for (i in 2:nrow(Bindex)){
  Bindex[i,]=Bindex[(i-1),]*(1+BRet[i,])
}

# param setting part
RBP=3
CSNUM2=0.5

# param setting part
TSWGT=1
CSWGT=1
RB1=RBP
RB2=RBP
RET=BRet
index=Bindex
CSdesign="vol"

# IRS5=IRV(RB1=RBP,RB2=RBP,RET=BRet,index=Bindex,CSdesign="vol")
Ret=RET


minobs1=12
minobs=60
Expanding=0

nopos=0.4##middle section->neutral zone
MA=c(1,2,3)/6

CS=CSNUM2# assets to long and short

#Factor Portfolio construction
Assetvol=0.02
Strategyvol=0.02
factorvol=0.02
factorsd=260
assetsd=260
statsd=260
##volupdate trigger
Volband=0.1

# Time Series Signal - using equity revision?
twoYR <- as.data.frame(read.csv("./data/bonds2yr.csv", row.names=1,header=TRUE))
tenYR <- as.data.frame(read.csv("./data/bonds10yr.csv", row.names=1,header=TRUE))

fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))

Ret=RET
if(JGB=="out"){
  index=index[,1:4]
}

Eqindex<- as.data.frame(read.csv("./data/ERR.csv", row.names=1,header=TRUE))
Eqindex=Eqindex[,c("SPX","TSX","DAX","FTSE","NKY")]

if(JGB=="out"){
  Eqindex=Eqindex[,c("SPX","TSX","DAX","FTSE")]
}

L=3
Eqindex2=Eqindex
RV=-(Eqindex2[(L+1):nrow(Eqindex2),]-Eqindex2[1:(nrow(Eqindex2)-L),]) # 3Month Change of ERR
RV3=RV[rownames(RV)>=rownames(index)[1],]
RV=RV3

# expanding window rank after minimum observation period 12
RVrank=RV[(minobs1):nrow(RV),]*0
for(i in 1:(nrow(RV)-minobs1+1)) {
  RVrank[i,]=apply(RV[1:(minobs1+i-1),],2,rank,na.last="keep")[i+minobs1-1,]/((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))
  RVrank[i,((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))<(minobs1)]=NA
}#i
RVrankTS=RVrank

# Cross Sectional Signal1 - 10yr nominal rate - real yield2
ry <- as.data.frame(read.csv("./data/realyield2.csv", row.names=1,header=TRUE))
#cpi <- as.data.frame(read.csv("D:/R/GRP/cpistat.csv", row.names=1,header=TRUE))
fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))

Ret=RET
obslen=2
tenYR <- as.data.frame(read.csv("./data/bonds10yr.csv", row.names=1,header=TRUE))

tenYR=tenYR[rownames(tenYR)%in%rownames(fx),]
tenYR=tenYR[rownames(tenYR)%in%rownames(ry),1:4] # JGB Out

RV=tenYR-ry # 10yr nominal rate - real yield2
# expanding window rank
RVrank=RV[(minobs1):nrow(RV),]*0
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
# rownames(CSRVpos)=rownames(RV1)
# # #

##Final CS signal(weighted)
CSRV=CSRVpos
CSRV[is.na(CSRV)]=0
#translate to positions
CSRVone=CSRV

# Cross Sectional Signal2 -
ry <- as.data.frame(read.csv("./data/realyield.csv", row.names=1,header=TRUE))
#cpi <- as.data.frame(read.csv("D:/R/GRP/cpistat.csv", row.names=1,header=TRUE))
fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))

Ret=RET
obslen=2

# 2month different of realyield???
RV=ry[(obslen+1):nrow(ry),]-ry[1:(nrow(ry)-obslen),]

RV1=RV[(minobs1):nrow(RV),]
truecount=round(rowSums(!is.na(RV1))*CS)
truecount=matrix(rep(truecount,ncol(RV1)),nrow=nrow(RV1))
CSRV=as.data.frame(t(as.data.frame(apply(RV1,1,rank,ties.method="first",na.last="keep"))))
CSRV1=as.data.frame(t(as.data.frame(apply(-RV1,1,rank,ties.method="last",na.last="keep"))))
CSRVpos=CSRV*0
CSRVpos[CSRV[,]<=truecount]=-1
CSRVpos[CSRV1[,]<=truecount]=1
rownames(CSRVpos)=rownames(RV1)
# # # #

##Final CS signal(weighted)
CSRV=CSRVpos
CSRV[is.na(CSRV)]=0
#translate to positions
CSRVtwo=CSRV

AA=intersect(rownames(CSRVone),rownames(CSRVtwo))

CSRVone=CSRVone[AA,]
CSRVtwo=CSRVtwo[AA,]

CSRV=CSRVone+CSRVtwo*0.5
TSRV=RVrankTS*0
TSRV[RVrankTS[,]>(nopos+(1-nopos)/2)]=1
TSRV[RVrankTS[,]<((1-nopos)/2)]=-1

AA=intersect(rownames(CSRV),rownames(TSRV))
TSRV=TSRV[AA,]
CSRV=CSRV[AA,]

# TSRVraw=RV[(minobs1):nrow(RV),]
# TSRV=TSRVraw*0
# TSRV[TSRVraw[,]>(0)]=1
# TSRV[TSRVraw[,]<(0)]=-1


TSRV$mon=as.yearmon(rownames(TSRV))+1/12
if(JGB=="out"){Ret=Ret[,1:4]}

start=max(TSRV$mon[1],as.yearmon(rownames(Ret)[1]))
TSRVtemp=Ret[as.yearmon(rownames(Ret))>=start,]*0
CSRVtemp=TSRVtemp
for (ss in 1:nrow(TSRV)){
  TSRVtemp[rownames(TSRVtemp)>=rownames(TSRV)[ss],1:ncol(Ret)]=TSRV[ss,1:ncol(Ret)]
  CSRVtemp[rownames(CSRVtemp)>=rownames(CSRV)[ss],1:ncol(Ret)]=CSRV[ss,1:ncol(Ret)]
}
TSRV=TSRVtemp
CSRV=CSRVtemp
Ret=RET

if(JGB=="out"){Ret=Ret[,1:4]}


# Run backtesting
source('grp_helpers.R')
functionoutput=irfactor(TSRV,CSRV,Ret,"month",CSLS=CSdesign,TSWGT,CSWGT,0,0,rpname=name1)

# Charting
chart.CumReturns(functionoutput[[5]], main=name1)
chart.CumReturns(functionoutput[[6]], main=paste(name1,"lag"))


# write csv
write.csv(functionoutput[[5]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\irv.csv')
write.csv(functionoutput[[6]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\irv_lag.csv')

