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

# IRS2=ICA(RB1=RBP,RB2=RBP,RET=BRet,index=Bindex,CSdesign="vol")
twoYR <- as.data.frame(read.csv("./data/bonds2yr.csv", row.names=1,header=TRUE))
tenYR <- as.data.frame(read.csv("./data/bonds10yr.csv", row.names=1,header=TRUE))
fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))

if(JGB=="out"){
  twoYR=twoYR[,1:4]
  tenYR=tenYR[,1:4]
}


minobs1=12
minobs=60

Expanding=0

nopos=0.4##middle section->neutral zone
MA=c(1,2,3)/6

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

##carry score


carry=tenYR-twoYR
fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))
carry=carry[rownames(carry)%in%rownames(fx),] # Series into monthly Series
#carry=carry[wday(rownames(carry))==RB1,]
Ret=RET
carryd=tenYR-twoYR # daily carry series

STDEV=as.data.frame(rollapplyr(carryd,252,sd)) # calculating daily volatility
rownames(STDEV)=rownames(carryd)[(252):nrow(carryd)]
STDEV=STDEV[rownames(STDEV)%in%rownames(fx),] # to monthly series
carry=carry[rownames(carry)%in%rownames(STDEV),]/STDEV # adjusting carry by carry volatility

RV=carry[12:nrow(carry),]#-rowMeans(carry[12:nrow(carry),])
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

# expanding window percentage rank for each assets
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


# carry=tenYR-twoYR
# carry=carry[rownames(carry)%in%rownames(STDEV),]
#
# RV=carry[12:nrow(carry),]


RV1=RV[(minobs1):nrow(RV),]
truecount=round(rowSums(!is.na(RV1))*CS)
truecount=matrix(rep(truecount,ncol(RV1)),nrow=nrow(RV1))
CSRV=as.data.frame(t(as.data.frame(apply(RV1,1,rank,ties.method="first",na.last="keep"))))
CSRV1=as.data.frame(t(as.data.frame(apply(-RV1,1,rank,ties.method="last",na.last="keep"))))
CSRVpos=CSRV*0
CSRVpos[CSRV[,]<=truecount]=-1
CSRVpos[CSRV1[,]<=truecount]=1



##Final CS signal(weighted)
CSRVone=CSRVpos
CSRVone[is.na(CSRVone)]=0
#translate to positions

TSRVone=RVrank*0

TSRVone[RVrank[,]>(nopos+(1-nopos)/2)]=1
TSRVone[RVrank[,]<((1-nopos)/2)]=-1
# TSRVone[RV1[,]>(0)]=1
# TSRVone[RV1[,]<(0)]=-1
#
#
# carry <- as.data.frame(read.csv("D:/R/GRP/bondscarry.csv", row.names=1,header=TRUE))
# if(JGB=="out"){carry=carry[,1:4]}
# fx<- as.data.frame(read.csv("D:/R/GRP/fx.csv", row.names=1,header=TRUE))
# Ret=RET
#
# RV=carry[12:nrow(carry),]
#
# RVrank=RV[(minobs1):nrow(RV),]*0
#
# for(i in 1:(nrow(RV)-minobs1+1)) {
#   RVrank[i,]=apply(RV[1:(minobs1+i-1),],2,rank,na.last="keep")[i+minobs1-1,]/((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))
#   RVrank[i,((i+minobs1-1)-colSums(is.na(RV[1:(i+minobs1-1),])))<(minobs1)]=NA
# }#i
#
# RV1=RV[(minobs1):nrow(RV),]
# truecount=round(rowSums(!is.na(RV1))*CS)
# truecount=matrix(rep(truecount,ncol(RV1)),nrow=nrow(RV1))
# CSRV=as.data.frame(t(as.data.frame(apply(RV1,1,rank,ties.method="first",na.last="keep"))))
# CSRV1=as.data.frame(t(as.data.frame(apply(-RV1,1,rank,ties.method="last",na.last="keep"))))
# CSRVpos=CSRV*0
# CSRVpos[CSRV[,]<=truecount]=-1
# CSRVpos[CSRV1[,]<=truecount]=1
#
#
#
# ##Final CS signal(weighted)
# CSRVthree=CSRVpos
# CSRVthree[is.na(CSRVthree)]=0
# #translate to positions
#
# TSRVthree=RVrank*0
#
# TSRVthree[RVrank[,]>(nopos+(1-nopos)/2)]=1
# TSRVthree[RVrank[,]<((1-nopos)/2)]=-1
# TSRVthree[RV1[,]>(0)]=1
# TSRVthree[RV1[,]<(0)]=-1
# # TSRV=TSRVone+TSRVtwo
# # CSRV=CSRVone+CSRVtwo
#
# TSRV=TSRVone[rownames(TSRVone)%in%rownames(TSRVthree),]+TSRVthree
# CSRV=CSRVone[rownames(CSRVone)%in%rownames(CSRVthree),]+CSRVthree
#
TSRV=TSRVone
CSRV=CSRVone
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
functionoutput=irfactor(TSRV,CSRV,Ret,"month",CSLS=CSdesign,TSWGT,CSWGT,ex=0,CSweek=0,monitor=1,IR=1,RB1=RBP,rpname=name1)

# Charting
chart.CumReturns(functionoutput[[5]], main=name1)
chart.CumReturns(functionoutput[[6]], main=paste(name1,"lag"))



# write csv
write.csv(functionoutput[[5]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\ica.csv')
write.csv(functionoutput[[6]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\ica_lag.csv')



