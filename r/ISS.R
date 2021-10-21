name1="tests"
JGB="out"

# Global Strategies Setting
library(zoo)
library(lubridate)
library(PerformanceAnalytics)
library(bizdays)
library(matrixStats)

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

# IRS3=ISS(RB1=RBP,RB2=RBP,RET=BRet,index=Bindex,CSdesign="vol")
fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))



fundwgt=1
statwgt=1

minobs=500

Expanding=1


short=0.2 # how much should I short?

day1=24#rebalance
day2=7

nopos=0.4##middle section->neutral zone
MA=c(1,2,3,4,5)/15

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
Ret=RET
cal <- create.calendar("Actual",weekdays=c("saturday", "sunday"))
TOM=as.data.frame(day(add.bizdays(rownames(Ret),1,cal)))
rownames(TOM)=rownames(Ret)
TOM2=TOM*0-1*short # short 0.2 for all days

TOM2[TOM[,1]>=day1,1]=1 # long for 23 to one day before last biz day
# TOM2[TOM[,1]==1 & wday(rownames(TOM))!=3,1]=1
# TOM2[TOM[,1]==2 & wday(rownames(TOM))!=3& wday(rownames(TOM))!=4,1]=1
# TOM2[TOM[,1]==3 & wday(rownames(TOM))!=3& wday(rownames(TOM))!=4& wday(rownames(TOM))!=5,1]=1
# TOM2[TOM[,1]==4 & wday(rownames(TOM))!=3& wday(rownames(TOM))!=4& wday(rownames(TOM))!=5& wday(rownames(TOM))!=6,1]=1
# TOM2[TOM[,1]==5 & wday(rownames(TOM))!=3& wday(rownames(TOM))!=4& wday(rownames(TOM))!=5& wday(rownames(TOM))!=6& wday(rownames(TOM))!=7,1]=1
# TOM2[TOM[,1]==6 & wday(rownames(TOM))==2,1]=1

SIG=Ret*0+1
SIG=SIG*TOM2[,1]

FOMC <- as.data.frame(read.csv("./data/FOMC2.csv", row.names=1,header=TRUE))
FOMCDM=TOM2*0
FOMCEM=TOM2*0
FOMCDM[as.Date(rownames(FOMCDM))%in%add.bizdays(rownames(FOMC),-2,cal),1]=1
FOMCEM[as.Date(rownames(FOMCEM))%in%add.bizdays(rownames(FOMC),-1,cal),1]=1

SIG2=Ret*0
SIG2[,1:4]=FOMCDM # long before 1 and 2 biz day - this is not used
if(JGB!="out"){SIG2[,5]=FOMCEM}


TSRV1=SIG*fundwgt*1#+SIG2*0.5



short=0

CSRV=index*0
statday=unique(as.yearmon(rownames(Ret))+1/12) # month year + 1month series of 'Ret'. for example, for index 1992-01-01, it marked as "2 1992"
bible=as.data.frame(matrix(0,nrow=(length(statday)-60),ncol=ncol(index)))
bible$YM=statday[(60+1-short):(length(statday)-short)]
bible$mon=month(bible$YM) # 'bible' is unique year month and month series of 'Ret'. it begins from '1 1997'
Ret$YM=as.yearmon(rownames(Ret))
Ret$mon=month(rownames(Ret))

for (i in 1:nrow(bible)){
  # first, for each month before '1 1997', calculate average daily Return of each asset of that month. (this is done by expanding window basis) Then calculate mean of all asset. -> but its not used below..
  ave=mean(colMeans(Ret[Ret$YM<bible$YM[i]& Ret$YM>=statday[1]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))
  # For each month before '1 1997', calculate average daily Return of each asset of that month. Divide calculated daily return average of month by stdev of daily return average of month. (this is done by expanding window basis)
  bible[i,1:ncol(index)]=(colMeans(Ret[Ret$YM<bible$YM[i]& Ret$YM>=statday[1]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))/colSds(as.matrix(Ret[Ret$YM<bible$YM[i]& Ret$YM>=statday[1]& Ret$mon==bible$mon[i],1:ncol(CSRV)]),na.rm=TRUE)
  #bible[i,1:ncol(index)]=bible[i,1:ncol(index)]*sqrt(nrow(Ret[Ret$YM<bible$YM[i]& Ret$YM>=statday[1]&Ret$mon==bible$mon[i],1:ncol(index)]))
}

# for (i in 1:nrow(bible)){
#   ave=(colMeans(Ret[Ret$YM<(bible$YM[i]) &Ret$YM>=statday[i]&Ret$mon!=bible$mon[i],1:ncol(index)],na.rm=TRUE))
#   bible[i,1:ncol(index)]=(colMeans(Ret[Ret$YM<(bible$YM[i])& Ret$YM>=statday[i]&Ret$mon==bible$mon[i],1:ncol(index)],na.rm=TRUE))/colSds(as.matrix(Ret[Ret$YM<(bible$YM[i])& Ret$YM>=statday[i]& Ret$mon==bible$mon[i],1:ncol(CSRV)]),na.rm=TRUE)
#
# }

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




# below is another strategy but not used for Time Series Strategy.
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

TSRV=TSRV1*1+TSRV*0 # only use TSRV1
AAA=intersect(rownames(TSRV),rownames(CSRV))
CSRV=CSRV[AAA,]
TSRV=TSRV[AAA,]

Ret=RET


# Run backtesting
source('grp_helpers.R')
functionoutput=irfactor(TSRV,CSRV,Ret,FALSE,CSLS=CSdesign,TSWGT,CSWGT,0,CSweek=0,IR=1,rpname=name1)

# Charting
chart.CumReturns(functionoutput[[5]], main=name1)
chart.CumReturns(functionoutput[[6]], main=paste(name1,"lag"))
