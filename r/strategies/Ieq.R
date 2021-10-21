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
# DM Equity
# Total Return Index
eindex <- as.data.frame(read.csv("./data/totindex.csv", row.names=1,header=TRUE))
eindex=eindex[rownames(eindex)<"2008-01-01",]
ERett=eindex[2:nrow(eindex),]/eindex[1:(nrow(eindex)-1),]-1 #
# Price Index
eindex1 <- as.data.frame(read.csv("./data/priceindex.csv", row.names=1,header=TRUE))
eindex1=eindex1[rownames(eindex1)<"2008-01-01",]
ERetp=eindex1[2:nrow(eindex1),]/eindex1[1:(nrow(eindex1)-1),]-1 #p
# Futures Return
efut <- as.data.frame(read.csv("./data/FutGenratio1.csv", row.names=1,header=TRUE))
fRet=efut[2:nrow(efut),]/efut[1:(nrow(efut)-1),]-1

compRet=ERett
compRet[is.na(compRet)]=ERetp[is.na(compRet)] # if no total return index return, then price return
ERet1=compRet
ERet=rbind(compRet[rownames(ERet1)<"2008-01-01",],fRet[rownames(fRet)>="2008-01-01",]) # after 2008, use future return
ERet[is.na(ERet)]=0

# from return data, making index
# bond
Bindex=BRet*0
Bindex[1,]=matrix(rep(1,ncol(Bindex)),nrow=1)
for (i in 2:nrow(Bindex)){
  Bindex[i,]=Bindex[(i-1),]*(1+BRet[i,])
}

# DM equity
Eindex=ERet*0
Eindex[1,]=matrix(rep(1,ncol(Eindex)),nrow=1)
for (i in 2:nrow(Eindex)){
  Eindex[i,]=Eindex[(i-1),]*(1+ERet[i,])
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

# IRS4=Ieq(RB1=RBP,RB2=RBP,RET=BRet,index=Bindex,CSdesign="vol")
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



fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))

Ret=RET

Eqindex=Eindex[rownames(Eindex)%in%rownames(fx),] # to monthly

Eqindex=Eqindex[,c("SPX","TSX","DAX","FTSE","NKY")]
if(JGB=="out"){
  Eqindex=Eqindex[,c("SPX","TSX","DAX","FTSE")]
}

# 6 month return
L=6
RV=-Eqindex[(L+1):nrow(Eqindex),]/Eqindex[1:(nrow(Eqindex)-L),]
RV1=RV[rownames(RV)>=rownames(index)[1],]

# 1 month return
L=1
RV=-Eqindex[(L+1):nrow(Eqindex),]/Eqindex[1:(nrow(Eqindex)-L),]
RV3=RV[rownames(RV)>=rownames(index)[1],]

# 3 month return
L=3
RV=-Eqindex[(L+1):nrow(Eqindex),]/Eqindex[1:(nrow(Eqindex)-L),]
RV6=RV[rownames(RV)>=rownames(index)[1],]
# Eqindex=Eqindex[rownames(Eqindex)%in%rownames(index)]

RV=RV1+RV3+RV6

RV=RV[12:nrow(RV),]


# MAM=matrix(rep(MA,ncol(Ret)),ncol=ncol(Ret))
# for(i in length(MA):nrow(index)){
#   RV[i,]=colSums(carry[(i-length(MA)+1):i,]*MAM)
#
# }
# RV=RV[length(MA):nrow(index),]


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
#

# 12(minobs1 is 12) month expanding percent rank
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
AAA=intersect(rownames(TSRV),rownames(CSRV))
TSRV=TSRV[AAA,]
CSRV=CSRV[AAA,]


TSRV$mon=as.yearmon(rownames(TSRV))+1/12


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


# Run backtesting
source('grp_helpers.R')
functionoutput=irfactor(TSRV,CSRV,Ret,"month",CSLS=CSdesign,TSWGT,CSWGT,0,CSweek=0,IR=1,rpname=name1)

# Charting
chart.CumReturns(functionoutput[[5]], main=name1)
chart.CumReturns(functionoutput[[6]], main=paste(name1,"lag"))


# write csv
write.csv(functionoutput[[5]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\ieq.csv')
write.csv(functionoutput[[6]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\ieq_lag.csv')

