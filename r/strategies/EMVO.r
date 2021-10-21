library(PerformanceAnalytics)
library(bizdays)
library(zoo)
library(Hmisc)
library(RiskPortfolios)
library(lubridate)
library(qcc)
library(matrixStats)

name1 <- 'test'
IRCSmod=0.8
IRTSmod=0.8
ComCSmod=0.8
ComTSmod=0.8
DMCSmod=1
EMCSmod=1
DMTSmod=1
EMTSmod=1
positions=1
updatepos=0
RBP=3
targetvol=0.0475
#targetvol=0.035
Volband=0.05
elimit=3.8

CSpos=0
CSpostest=0.35
CSNUM=0.35
CSNUM2=0.5
CSNUM3=0.5

TSWGT=1
CSWGT=1
RB1=3
RB2=3
RET = EMRet
index = EMindex
CSdesign = 'notional'
IDN = 'out'




# Read Data from csv & process data
emindex <- as.data.frame(read.csv("./data/totindex-em.csv", row.names=1,header=TRUE))
emindex1 <- as.data.frame(read.csv("./data/priceindex-em.csv", row.names=1,header=TRUE))
emfut <- as.data.frame(read.csv("./data/fut1return-em.csv", row.names=1,header=TRUE))
rownames(emfut)=as.Date(as.numeric(rownames(emfut)),origin = "1899-12-30")
eindex1=eindex1[rownames(eindex1)<"2008-01-01",]
ERetp=eindex1[2:nrow(eindex1),]/eindex1[1:(nrow(eindex1)-1),]-1 #p

emindex=emindex[rownames(emindex)<"2013-01-01",]
emindex1=emindex1[rownames(emindex1)<"2013-01-01",]
EMRet=emindex1[2:nrow(emindex1),]/emindex1[1:(nrow(emindex1)-1),]-1
EMTRet=emindex[2:nrow(emindex),]/emindex[1:(nrow(emindex)-1),]-1
fRet=emfut[2:nrow(emfut),]/emfut[1:(nrow(emfut)-1),]-1
fRet=fRet[wday(rownames(fRet))!=1 & wday(rownames(fRet))!=7, ]
compRet=EMRet
compRet[rownames(EMTRet),]=EMTRet[rownames(EMTRet),]
EMRet1=compRet
EMRet=rbind(compRet[rownames(EMRet1)<"2013-01-01",],fRet[rownames(fRet)>="2013-01-01",])

EMRet[is.na(EMRet)]=0
EMindex=EMRet*0
EMindex[1,]=matrix(rep(1,ncol(EMindex)),nrow=1)
for (i in 2:nrow(EMindex)){
  EMindex[i,]=EMindex[(i-1),]*(1+EMRet[i,])
}

TSWGT=1
CSWGT=1
RB1=3
RB2=3
RET <- EMRet
index <- EMindex
CSdesign <- "notional"


RETfinal=RET


RETfinal=RET

for(statrun in 1:2){
  if (statrun==1){RET=EMRet[,1:4]}
  if (statrun==2){RET=EMRet[,5:10]}
  index=EMindex[,colnames(RET)]


  Ret=RET

  minobs1=12
  minobs=60
  longlen=12
  shortlen=0
  Expanding=0

  nopos=0.4##middle section->neutral zone
  SDEV=12

  # WGT=c(0,0,0,1)
  # WGT2=WGT

  WGT=c(0.5,0.5)
  WGT2=WGT

  CS=CSNUM # assets to long and short

  #Factor Portfolio construction
  Assetvol=0.02
  Strategyvol=0.02
  factorvol=0.02
  factorsd=12
  assetsd=12
  statsd=12
  ##volupdate trigger
  Volband=0.1



  fx<- as.data.frame(read.csv("./data/fx-em.csv", row.names=1,header=TRUE))[,colnames(RET)]


  ##Magnitude

  Mag=fx[(longlen+1):nrow(fx),]
  obs=nrow(Mag)-1
  Mag=fx[(1+longlen-shortlen):(1+longlen-shortlen+obs),]/fx[(1):(1+obs),]-1

  rownames(Mag)=rownames(fx)[(longlen+1):nrow(fx)]

  RV=(Mag)

  if (IDN=="out"){
    RV[,colnames(RV)=="ID"]=NA
  }
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

  Magrank=RVrank
  CSMagpos=CSRVpos



  ##Reliability

  # ret=fx[2:nrow(fx),]/fx[1:(nrow(fx)-1),]-1
  # STDEV=as.data.frame(rollapplyr(ret,SDEV,sd))*sqrt(12)
  # rownames(STDEV)=rownames(fx)[(SDEV+1):nrow(fx)]
  # STDEV1=STDEV[rownames(Mag)%in%rownames(fx)[(1+longlen):(1+longlen+obs)],]

  longlen=3

  Mag=fx[(longlen+1):nrow(fx),]
  obs=nrow(Mag)-1
  Mag=fx[(1+longlen-shortlen):(1+longlen-shortlen+obs),]/fx[(1):(1+obs),]-1

  rownames(Mag)=rownames(fx)[(longlen+1):nrow(fx)]

  RV=(Mag)



  RV=(Mag)#/STDEV1
  if (IDN=="out"){
    RV[,colnames(RV)=="ID"]=NA
  }
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


  Relrank=RVrank
  CSRelpos=CSRVpos

  AAA=intersect(rownames(CSRelpos),rownames(CSMagpos))
  CSMagpos=CSMagpos[AAA,]
  CSRelpos=CSRelpos[AAA,]
  ##Final CS signal(weighted)
  CSRV=CSRelpos*WGT2[2]+CSMagpos*WGT2[1]

  #translate to positions


  TS1=Magrank*0
  TS1[Magrank[,]>(nopos+(1-nopos)/2)]=1
  TS1[Magrank[,]<((1-nopos)/2)]=-1


  TS2=Relrank*0
  TS2[Relrank[,]>(nopos+(1-nopos)/2)]=1
  TS2[Relrank[,]<((1-nopos)/2)]=-1

  # TS1=Magrank*0
  #
  #
  # up=rep((nopos+(1-nopos)/2),ncol(TS1))
  # down=rep(((1-nopos)/2),ncol(TS1))
  #
  # TS1[1,]=(Magrank[1,]>=(up))*1+-1*(Magrank[1,]<=(down))
  # for(i in 2:nrow(Magrank)){
  #   TS1[i,]=(Magrank[i,]>=(up-(TS1[i-1,]>0)*0.025))*1+(Magrank[i,]<=(down+(TS1[i-1,]<0)*0.025))*-1
  #
  # }
  #
  # TS2=Magrank*0
  #
  # TS2[1,]=(Relrank[1,]>=(up))*1+-1*(Relrank[1,]<=(down))
  # for(i in 2:nrow(Relrank)){
  #   TS2[i,]=(Relrank[i,]>=(up-(TS2[i-1,]>0)*0.025))*1+(Relrank[i,]<=(down+(TS2[i-1,]<0)*0.025))*-1
  #
  # }

  AAA=intersect(rownames(TS1),rownames(TS2))
  TS1=TS1[AAA,]
  TS2=TS2[AAA,]


  ####Final Position
  TSRV=TS1*WGT[1]+TS2*WGT[2]


  TSRV$mon=as.yearmon(rownames(TSRV))+1/12
  start=max(TSRV$mon[1],as.yearmon(rownames(Ret)[1]))
  TSRVtemp=Ret[as.yearmon(rownames(Ret))>=start,]*NA
  CSRVtemp=TSRVtemp
  for (ss in 1:nrow(TSRV)){
    TSRVtemp[rownames(TSRVtemp)>=rownames(TSRV)[ss],1:ncol(Ret)]=TSRV[ss,1:ncol(Ret)]
    CSRVtemp[rownames(CSRVtemp)>=rownames(CSRV)[ss],1:ncol(Ret)]=CSRV[ss,1:ncol(Ret)]
  }
  TSRVstat1=TSRVtemp
  CSRVstat1=CSRVtemp
  Ret=RET



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



  if (IDN=="out"){
    RV[,colnames(RV)=="ID"]=NA
  }

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
  # #

  ##Final CS signal(weighted)
  CSRV=CSRVpos
  CSRV[is.na(CSRV)]=0
  #translate to positions


  TSRV=RVrank*0
  TSRV[RVrank[,]>(nopos+(1-nopos)/2)]=1
  TSRV[RVrank[,]<((1-nopos)/2)]=-1





  TSRV$mon=as.yearmon(rownames(TSRV))+1/12
  start=max(TSRV$mon[1],as.yearmon(rownames(Ret)[1]))
  TSRVtemp=Ret[as.yearmon(rownames(Ret))>=start,]*NA
  CSRVtemp=TSRVtemp
  for (ss in 1:nrow(TSRV)){
    TSRVtemp[rownames(TSRVtemp)>=rownames(TSRV)[ss],1:ncol(Ret)]=TSRV[ss,1:ncol(Ret)]
    CSRVtemp[rownames(CSRVtemp)>=rownames(CSRV)[ss],1:ncol(Ret)]=CSRV[ss,1:ncol(Ret)]
  }
  TSRVstat2=TSRVtemp
  CSRVstat2=CSRVtemp
  Ret=RET


  AAA=intersect(rownames(TSRVstat1),rownames(TSRVstat2))
  TSRVstat1=TSRVstat1[AAA,]
  TSRVstat2=TSRVstat2[AAA,]
  CSRVstat1=CSRVstat1[AAA,]
  CSRVstat2=CSRVstat2[AAA,]

  TSRV=TSRVstat1*1+TSRVstat2
  CSRV=CSRVstat1*1+CSRVstat2


  if(statrun==1){
    TSRVrun1=TSRV
    CSRVrun1=CSRV

  }
  if(statrun==2){
    TSRVrun2=TSRV
    CSRVrun2=CSRV
  }
}
TSRV=cbind(TSRVrun1,TSRVrun2)[,colnames(EMRet)]
CSRV=cbind(CSRVrun1,CSRVrun2)[,colnames(EMRet)]

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
write.csv(functionoutput[[5]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\emvo.csv')
write.csv(functionoutput[[6]], file='C:\\Users\\jungyoon.choi\\Desktop\\jungyoon\\project\\risk_premia\\python\\strategy\\check\\past\\emvo_lag.csv')


