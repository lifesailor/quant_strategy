name1="tests"
JGB="out"

# Global Strategies Setting
library(zoo)
library(lubridate)
library(PerformanceAnalytics)

# read data
bond <- as.data.frame(read.csv("./data/bonds.csv", row.names=1,header=TRUE))
rownames(bond)=as.Date(as.numeric(rownames(bond)),origin = "1899-12-30")
BRet=bond[2:nrow(bond),]/bond[1:(nrow(bond)-1),]-1
if(JGB=="out"){
  BRet=BRet[,1:4]
  bond=bond[,1:4]
}
# from return data, making index
Bindex=BRet*0
Bindex[1,]=matrix(rep(1,ncol(Bindex)),nrow=1)
for (i in 2:nrow(Bindex)){
  Bindex[i,]=Bindex[(i-1),]*(1+BRet[i,])
}

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

# IRS1=IPM(RB1=RBP,RB2=RBP,RET=BRet,index=Bindex,CSdesign="vol")
fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))
index=index[wday(rownames(index))==RB1,] # daily index to weekly index

minobs1=52
minobs=260
longlen=52
shortlen=4
Expanding=0

nopos=0.4 ## middle section->neutral zone
SDEV=12

# WGT=c(0,0,0,1)
# WGT2=WGT

WGT=c(1/3,1/3,1/3)
WGT2=c(1,1,0)/3

CS=CSNUM2 # assets to long and short

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
Mag=index[(1+longlen-shortlen):(1+longlen-shortlen+obs),]/index[(1):(1+obs),]-1 # 52w - 4w return

rownames(Mag)=rownames(index)[(longlen+1):nrow(index)]

RVrank=Mag[(minobs):nrow(Mag),]*0 # not used?

RV=Mag


##Reliability -> below is not used
ret=index[2:nrow(index),]/index[1:(nrow(index)-1),]-1 # 'ret' is weekly return

STDEV=as.data.frame(rollapplyr(ret,longlen,sd))*sqrt(52)
rownames(STDEV)=rownames(index)[(longlen+1):nrow(index)]
STDEV1=STDEV[rownames(STDEV)%in%rownames(Mag),]


RV=Mag#/STDEV1


# STRV=-(index[2:nrow(index),]/index[1:(nrow(index)-1),]-1)
#
#
# RV=STRV[52:nrow(STRV),]


RVrank=RV[(minobs1):nrow(RV),]*0 # not used?

RV1=(RV[(minobs1):nrow(RV),])
truecount=round(rowSums(!is.na(RV1))*CS)
truecount=matrix(rep(truecount,ncol(RV1)),nrow=nrow(RV1))
CSRV=as.data.frame(t(as.data.frame(apply(RV1,1,rank,ties.method="first",na.last="keep"))))
CSRV1=as.data.frame(t(as.data.frame(apply(-RV1,1,rank,ties.method="last",na.last="keep"))))
CSRVpos=CSRV*0
CSRVpos[CSRV[,]<=truecount]=-1
CSRVpos[CSRV1[,]<=truecount]=1


Relrank=RVrank # not used?

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
Conroll=rollapplyr(up,longlen-shortlen,sum)/(longlen-shortlen) # count number of up weeks
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
TSRVL=TS1*WGT[1]#+TS2*WGT[2]
CSRVL=CSRelpos*WGT[1]#+CSConpos*WGT2[2]
#CSRV=CSConpos*WGT2[2]+CSRelpos*WGT2[1]



##3shortterm momentum
longlen=4
shortlen=0
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



####Final Position
TSRVSh=TS1*WGT[1]#+TS2*WGT[2]
CSRVSh=CSRelpos*WGT[1]#+CSConpos*WGT2[2]


TSRV=TSRVSh[rownames(TSRVL),]*0.5+TSRVL*1
CSRV=CSRVSh[rownames(CSRVL),]*0.5+CSRVL*1
AAA=intersect(rownames(TSRV),rownames(CSRV))
TSRV=TSRV[AAA,]
CSRV=CSRV[AAA,]
TSRV$mon=as.yearmon(rownames(TSRV))+1/12
start=max(TSRV$mon[1],as.yearmon(rownames(Ret)[1]))
TSRVtemp=Ret[as.yearmon(rownames(Ret))>=start,]*NA
CSRVtemp=TSRVtemp
for (ss in 1:nrow(TSRV)){
  TSRVtemp[rownames(TSRVtemp)>=rownames(TSRV)[ss],1:ncol(Ret)]=TSRV[ss,1:ncol(Ret)]
  CSRVtemp[rownames(CSRVtemp)>=rownames(CSRV)[ss],1:ncol(Ret)]=CSRV[ss,1:ncol(Ret)]
}
TSRV=TSRVtemp
CSRV=CSRVtemp
Ret=RET


# Run backtesting
source('grp_helpers.R')
functionoutput=irfactor(TSRV,CSRV,Ret,"week",CSLS=CSdesign,TSWGT,CSWGT,0,CSweek=1,IR=1,rpname=name1)

# Charting
chart.CumReturns(functionoutput[[5]], main=name1)
chart.CumReturns(functionoutput[[6]], main=paste(name1,"lag"))


# write csv
write.csv(functionoutput[[5]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\ipm.csv')
write.csv(functionoutput[[6]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\ipm_lag.csv')
