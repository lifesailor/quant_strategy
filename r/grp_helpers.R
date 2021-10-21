# This Script is for helper functions for GRP
# Attempt to seperate helper function and strategy function

# Libraries
library(bizdays)

# General Setting
create.calendar(name='MyCalendar', weekdays=c('sunday', 'saturday'))
perfdate11=add.bizdays(today(),-6,'MyCalendar')
perfdate1="2017-12-30"
perfdate2="2015-12-30"
perfdate3="2013-12-30"
perfdate4="1990-12-30"

factor=function(TSRV,CSRV,Ret,TSweek=FALSE, CSLS="notional",TSWGT=1,CSWGT=1,ex=0,CSweek=1,monitor=1,IR=0,RB1=RBP, rpname=name1,BETA=betamat){
  #
  #
  # Args:
  #   TSRV:
  #   CSRV:
  #   Ret:
  #   TSweek:
  #   CSLS:
  #   TSWGT:
  #   CSWGT:
  #   ex:
  #   CSweek
  #   monitor
  #   IR
  #   RB1
  #   rpname:
  # Returns:
  #

  fx<-as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE)) # this series is for month end date
  RB2=RB1

  #Factor Portfolio construction
  Assetvol=0.02
  Strategyvol=0.02
  factorvol=0.02
  factorsd=260
  assetsd=90 # asset stdev calculation period
  #assetsd=259
  statsd=90
  ##volupdate trigger
  Volband=0.05

  # Calculate Volatility
  std=as.data.frame(rollapplyr(Ret,assetsd,sd))*sqrt(260)
  rownames(std)=rownames(Ret)[(assetsd):nrow(Ret)]
  STD=std*0

  # std=as.data.frame(rollapplyr(Ret,assetsd,EWMAvol))*sqrt(260)
  # rownames(std)=rownames(Ret)[(assetsd):nrow(Ret)]
  # STD=std*0

  # Making Volatility Series by Volband
  STD[1,]=std[1,]
  for (k in 2:nrow(std)){
    count=0
    for(j in 1:ncol(std)){
      if(!is.na(std[k,j])){
        if(is.na(STD[k-1,j])){
          STD[k-1,j]=std[k,j]
        }
        # if there is any asset that exceed volband then add count
        if(abs(std[k,j]-STD[k-1,j])>Volband*STD[k-1,j]){
          count=count+1
        }
      }
    }
    for(jj in 1:ncol(std)){
      if (count>0){
        STD[k,jj]=std[k,jj]
      }
      else {
        STD[k,jj]=STD[k-1,jj]
      }
    }
  }
  # Set min vol to 0.15
  if(IR!=1){
    STD[STD[,]<0.15]=0.15
  }

  # TS Portfolio
  # Time Seires Signal change by 'TSweek'
  if(TSweek=="week"){
    TSRV1=TSRV*0
    TSRV1[1,]=TSRV[1,]

    for (k in 2:nrow(TSRV)){
      if((wday(rownames(TSRV)[k])==RB1|wday(rownames(TSRV)[k])==RB2)){
        TSRV1[k,]=TSRV[k,]
      } else(TSRV1[k,]=TSRV1[k-1,])
    }
  }

  if(TSweek=="month"){
    TSRV1=TSRV*0
    TSRV1[1,]=TSRV[1,]
    monthcount=month(rownames(TSRV))

    for (k in 2:nrow(CSRV)){
      if(rownames(TSRV)[k]%in%rownames(fx)){TSRV1[k,]=TSRV[k,]
      #if((month(rownames(TSRV)[k-1]))!= (month(rownames(TSRV)[k]))){TSRV1[k,]=TSRV[k,]
      } else(TSRV1[k,]=TSRV1[k-1,])
    }
  }

  if(TSweek==FALSE){TSRV1=TSRV}

  TSRV=TSRV1

  # target vol index for Time Series signal
  VCweight=Assetvol/STD
  rownames(VCweight)=rownames(std)

  AA=intersect(rownames(VCweight),rownames(TSRV))
  VCTSpos=VCweight[AA,]*TSRV[AA,] # Vol Control Time Series Position

  VCTSpos[VCTSpos[,]==Inf]=NA

  # return of VC strategy
  ret1=Ret[rownames(Ret)%in%rownames(VCTSpos),]
  Strategy=as.data.frame(rowSums(ret1[2:nrow(ret1),]*VCTSpos[1:(nrow(VCTSpos)-1),],na.rm=TRUE))

  # risk of strategy expanding window
  Strategyrisk=as.data.frame(Strategy[statsd:nrow(Strategy),1])
  for (i in 1:nrow(Strategyrisk)){
    Strategyrisk[i,1]=sd(Strategy[1:(i+statsd-1),1])*sqrt(260)
  }

  # risk of strategy moving window. window is specified by 'statsd'
  # if(ex!=1){
  Strategyrisk1=as.data.frame(rollapplyr(Strategy, statsd, sd)*sqrt(260))
  # }
  Strategyrisk=(Strategyrisk1+Strategyrisk)/2 # using average of expanding & moving window strategy risk

  # Making Volatility Series by Volband
  bufrisk=Strategyrisk*0
  bufrisk[1,]=Strategyrisk[1,]
  for(k in 2:nrow(Strategyrisk)){
    if(abs(Strategyrisk[k,1]-bufrisk[k-1,1])>Volband*Strategyvol){
      bufrisk[k,1]=Strategyrisk[k,1]
    } else{bufrisk[k,1]=bufrisk[k-1,1]}

  }

  # calcuate ex-post risk control with volband buffer strategy
  kk=Strategyvol/bufrisk[,1]
  statlev=matrix(rep(kk,ncol(STD)),nrow=length(kk))
  rownames(statlev)=rownames(Strategy)[statsd:nrow(Strategy)]

  TSposition=VCTSpos[rownames(VCTSpos)%in%rownames(statlev),]*statlev
  TSposition[is.na(TSposition)]=0

  # TSposition1=TSposition*0
  # sss=kk*0
  # for(k in 1:nrow(TSposition))
  #   sss[k]=findwgtvol(Ret,rownames(TSposition)[k],TSposition[k,])
  #   if(findwgt(Ret,rownames(TSposition)[k],TSposition[k,])>(2*Strategyvol)){
  #     TSposition1[k,]=TSposition[k,]/2
  #   } else (TSposition1[k,]=TSposition[k,])

  # return of Vol Control strategy lever by statlev
  TSposition=TSposition*TSWGT
  ret1=Ret[rownames(Ret)%in%rownames(TSposition),]
  TSStrategy=as.data.frame(rowSums(ret1[2:nrow(ret1),]*TSposition[1:(nrow(TSposition)-1),],na.rm=TRUE))
  TSStrategylag=as.data.frame(rowSums(ret1[3:nrow(ret1),]*TSposition[1:(nrow(TSposition)-2),],na.rm=TRUE))
  TSStrategylag=rbind(0,TSStrategylag)

  TSTO=rowSums(abs(TSposition[2:nrow(TSposition),]-TSposition[1:(nrow(TSposition)-1),]),na.rm=TRUE) # Position Change

  # CS portfolio
  # Cross Sectional options by 'CSLS'
  STD1=STD[rownames(STD)%in%rownames(CSRV),]
  BETA1=STD1*0+1
  ret2=Ret[rownames(Ret)%in%rownames(CSRV),]

  if(CSLS=="notional"){
    CSRV2=CSRV
    #if ("RU"%in% colnames(CSRV)) {CSRV2[,"RU"]=0.5*CSRV2[,"RU"]}
    #if ("BR"%in% colnames(CSRV)) {CSRV2[,"BR"]=0.5*CSRV2[,"BR"]}
  }
  # not used
  if(CSLS=="beta"){
    for (ss in 1:nrow(BETA)){
      BETA1[rownames(BETA1)>=rownames(BETA)[ss],]=BETA[ss,]
    }
    CSRV2=CSRV/BETA1
  }
  if(CSLS=="vol"){
    CSRV2=CSRV*Assetvol/STD1
  }
  if(CSLS=="US"){
    CSRV2=CSRV
    CSRV2[,"SPX"]=3*CSRV2[,"SPX"]
  }
  if(CSLS=="BRRU"){
    CSRV2=CSRV
    CSRV2[,"RU"]=0.5*CSRV2[,"RU"]
    CSRV2[,"BR"]=0.5*CSRV2[,"BR"]
  }


  # Calculating Ex-ante Vol and adjust to target vol which specified by 'Strategyvol'
  CSRV1=CSRV*0 # empty series would be vol adjust signal
  week=as.numeric(strftime(as.POSIXlt(rownames(CSRV)),format="%W"))
  # if weekly
  if(CSweek==1){
    CSRV1[1,]=CSRV2[1,1:ncol(CSRV1)]*Strategyvol/findwgtewma(Ret,rownames(CSRV)[1],CSRV2[1,1:ncol(CSRV1)])
    for (k in 2:nrow(CSRV)){
      if((wday(rownames(CSRV)[k])==RB1|wday(rownames(CSRV)[k])==RB2)& week[k]%%1==0 ){
        CSRV1[k,]=CSRV2[k,1:ncol(CSRV1)]*Strategyvol/findwgtewma(Ret,rownames(CSRV)[k],CSRV2[k,1:ncol(CSRV1)])
      } else(CSRV1[k,]=CSRV1[k-1,])
    }
  }
  # if monthly
  if(CSweek!=1){
    monthcount=month(rownames(CSRV))
    CSRV1[1,]=CSRV2[1,1:ncol(CSRV1)]*Strategyvol/findwgtewma(Ret,rownames(CSRV)[1],CSRV2[1,1:ncol(CSRV1)])
    for (k in 2:nrow(CSRV)){
      if(rownames(CSRV)[k]%in%rownames(fx)){
        CSRV1[k,]=CSRV2[k,1:ncol(CSRV1)]*Strategyvol/findwgtewma(Ret,rownames(CSRV)[k],CSRV2[k,1:ncol(CSRV1)])
        #if((month(rownames(CSRV)[k-1]))!= (month(rownames(CSRV)[k]))){CSRV1[k,]=CSRV2[k,1:ncol(CSRV1)]*Strategyvol/findwgtewma(Ret,rownames(CSRV)[k],CSRV2[k,1:ncol(CSRV1)])
      } else(CSRV1[k,]=CSRV1[k-1,])
    }
  }

  ##Need to make time series of CS raw strategy, then vol control and match dates with TS strategy
  CSraw=as.data.frame(rowSums(ret2[2:nrow(ret2),]*CSRV1[1:(nrow(CSRV1)-1),],na.rm=TRUE))
  # if 'ex' == 1, expanding window, otherwise moving window
  if(ex==1){
    CSrisk=as.data.frame(CSraw[statsd:nrow(CSraw),1])
    for (i in 1:nrow(CSrisk)){
      CSrisk[i,1]=sd(CSraw[1:(i+statsd-1),1])*sqrt(260)
    }
  }
  if(ex!=1){
    CSrisk=as.data.frame(rollapplyr(CSraw, statsd, sd)*sqrt(260))
  }


  rownames(CSrisk)=rownames(CSraw)[statsd:nrow(CSraw)]
  week=as.numeric(strftime(as.POSIXlt(rownames(CSrisk)),format="%W"))

  # Making Volatility Series by Volband
  bufrisk1=CSrisk*0
  bufrisk1[1,]=CSrisk[1,]
  if(monitor==1){
    for(k in 2:nrow(CSrisk)){
      if(abs(CSrisk[k,1]-bufrisk1[k-1,1])>Volband* Strategyvol & (wday(rownames(CSrisk)[k])==RB1|wday(rownames(CSrisk)[k])==RB2)){
        bufrisk1[k,1]=CSrisk[k,1]
      } else{bufrisk1[k,1]=bufrisk1[k-1,1]}

    }}
  if(monitor==0){
    for(k in 2:nrow(CSrisk)){
      if(abs(CSrisk[k,1]-bufrisk1[k-1,1])>Volband* Strategyvol & (rownames(CSrisk)[k] %in% rownames(fx))){
        # if(abs(CSrisk[k,1]-bufrisk1[k-1,1])>Volband* Strategyvol & (month(rownames(CSrisk)[k])!=month(rownames(CSrisk)[k-1]))){
        bufrisk1[k,1]=CSrisk[k,1]
      } else{bufrisk1[k,1]=bufrisk1[k-1,1]}

    }}

  # calcuate ex-post risk control with volband buffer strategy
  # kkk=(Strategyvol/bufrisk1[,1])*0+1
  kkk=Strategyvol/bufrisk1[,1]
  statlev1=as.data.frame(matrix(rep(kkk,ncol(STD)),nrow=length(kkk)))
  rownames(statlev1)=rownames(CSrisk)

  CSposition=CSRV1[rownames(CSRV1)%in%rownames(statlev1),]*statlev1*CSWGT
  CSposition[is.na(CSposition)]=0


  ret1=Ret[rownames(Ret)%in%rownames(CSposition),]

  CSStrategy=as.data.frame(rowSums(ret1[2:nrow(ret1),]*CSposition[1:(nrow(CSposition)-1),],na.rm=TRUE))
  CSStrategylag=as.data.frame(rowSums(ret1[3:nrow(ret1),]*CSposition[1:(nrow(CSposition)-2),],na.rm=TRUE))
  CSStrategylag=rbind(0,CSStrategylag)
  CSTO=rowSums(abs(CSposition[2:nrow(CSposition),]-CSposition[1:(nrow(CSposition)-1),]),na.rm=TRUE)

  ####output
  CS=CSposition
  TS=TSposition


  #####Factor vol control
  # factor=CSposition+TSposition
  #
  # ret3=Ret[rownames(Ret)%in%rownames(factor),]
  # factorraw=as.data.frame(rowSums(ret3[2:nrow(ret3),]*factor[1:(nrow(factor)-1),],na.rm=TRUE))
  # factorrisk=as.data.frame(rollapplyr(factorraw, factorsd, sd)*sqrt(260))
  # rownames(factorrisk)=rownames(factorraw)[factorsd:nrow(factorraw)]
  #
  # bufrisk2=factorrisk*0
  # bufrisk2[1,]=factorrisk[1,]
  # if(monitor==1){
  # for(k in 2:nrow(factorrisk)){
  #   if(abs(factorrisk[k,1]-bufrisk2[k-1,1])>Volband*factorrisk[k,1] & (wday(rownames(factorrisk)[k])==RB1|wday(rownames(factorrisk)[k])==RB2)){
  #     bufrisk2[k,1]=factorrisk[k,1]
  #   } else{bufrisk2[k,1]=bufrisk2[k-1,1]}
  # }}
  #
  # if(monitor==0){
  # for(k in 2:nrow(factorrisk)){
  #   if(abs(factorrisk[k,1]-bufrisk2[k-1,1])>Volband*factorrisk[k,1] & (month(rownames(factorrisk)[k])!=month(rownames(factorrisk)[k-1]))){
  #     bufrisk2[k,1]=factorrisk[k,1]
  #   } else{bufrisk2[k,1]=bufrisk2[k-1,1]}
  # }
  # }
  #
  # kkkk=factorvol/bufrisk2[,1]
  # factorlev1=as.data.frame(matrix(rep(kkkk,ncol(STD)),nrow=length(kkkk)))
  # rownames(factorlev1)=rownames(factorrisk)
  # factorposition=factor[rownames(factor)%in%rownames(factorlev1),]*factorlev1
  # colnames(factorposition)=colnames(index)
  #
  # AGGTO=rowSums(abs(factorposition[2:nrow(factorposition),]-factorposition[1:(nrow(factorposition)-1),]),na.rm=TRUE)
  #
  #
  #
  # ret4=Ret[rownames(Ret)%in%rownames(factorposition),]
  #
  #
  # factorreturn=as.data.frame(rowSums(ret4[2:nrow(ret4),]*factorposition[1:(nrow(factorposition)-1),],na.rm=TRUE))
  # #quickcheck
  # output=cbind(TSStrategy,TSStrategylag,CSStrategy,CSStrategylag,TSTO,CSTO)
  # output1=output[rownames(output)%in%rownames(factorreturn),]
  # output2=cbind(factorreturn,output1,AGGTO)

  # giveup=list(factorposition,output2,TSposition,CSposition,TSRV,CSRV1,kk,kkk,kkkk)

  #TScheck11=paste(100*round(sum(RAWlag(TSposition[rownames(TSposition)>perfdate11,],Ret)),4),"%",sep="")
  TScheck1=paste(100*round(sum(RAWlag(TSposition[rownames(TSposition)>perfdate1,],Ret)),4),"%",sep="")
  TScheck2=paste(100*round(sum(RAWlag(TSposition[rownames(TSposition)>perfdate2,],Ret)),4),"%",sep="")
  TScheck3=paste(100*round(sum(RAWlag(TSposition[rownames(TSposition)>perfdate3,],Ret)),4),"%",sep="")

  #CScheck11=paste(100*round(sum(RAWlag(CSposition[rownames(CSposition)>perfdate11,],Ret)),4),"%",sep="")
  CScheck1=paste(100*round(sum(RAWlag(CSposition[rownames(CSposition)>perfdate1,],Ret)),4),"%",sep="")
  CScheck2=paste(100*round(sum(RAWlag(CSposition[rownames(CSposition)>perfdate2,],Ret)),4),"%",sep="")
  CScheck3=paste(100*round(sum(RAWlag(CSposition[rownames(CSposition)>perfdate3,],Ret)),4),"%",sep="")

  print(paste(TScheck1,TScheck2,TScheck3)) #TScheck11,
  print(paste(CScheck1,CScheck2,CScheck3)) #CScheck11,
  print(colSums(TSRV))
  print(tail(TS))
  print(colSums(CSRV))
  print(tail(CS))

  nolag_perf = cbind(RAW(TSposition[rownames(TSposition)>perfdate4,],Ret),RAW(CSposition[rownames(CSposition)>perfdate4,],Ret))
  colnames(nolag_perf) = c("TS", "CS")
  lag_perf = cbind(RAWlag(TSposition[rownames(TSposition)>perfdate4,],Ret),RAWlag(CSposition[rownames(CSposition)>perfdate4,],Ret))
  colnames(lag_perf) = c("TS", "CS")

  giveup=list(TSposition,CSposition,TSRV1,CSRV1, nolag_perf, lag_perf)

  #chart.CumReturns(nolag_perf, main=rpname) #charts.PerformanceSummary(nolag_perf, main=rpname)
  #chart.CumReturns(lag_perf, main=paste(rpname,"lag")) #charts.PerformanceSummary(lag_perf, main=paste(rpname,"lag"))
  # charts.RollingPerformance(RAWlag(CSposition[rownames(CSposition)>perfdate3,],Ret),main=paste(rpname,"_CS",sep=""))

  return(giveup)
}

findwgtewma=function(data, date, wgt){
  #
  #
  # Arg:
  #   data: Return of asset
  #   date: Start date
  #   wgt: initial weight of signal series which would be volatility adjusted signal
  # Return:
  #

  sdobs=130
  sdobs1=130
  data1=data

  # Contraints for Russia and Brazil
  # if("RU"%in%colnames(data)){ data1[,"RU"]=data[,"RU"]*1.5}
  #
  # if("BR"%in%colnames(data)){ data1[,"BR"]=data[,"BR"]*1.5}

  ends=match(date,rownames(data))
  begin=ends-sdobs+1
  begin1=ends-sdobs1+1
  covar=matrix(NA,ncol(data),ncol(data))
  corr=matrix(NA,ncol(data),ncol(data))
  corr1=matrix(NA,ncol(data),ncol(data))
  var=array()
  rownames(covar)=colnames(data)
  colnames(covar)=colnames(data)

  for (i in 1:ncol(data1)){
    var[i]=sqrt(var(data1[begin:ends,i],data1[begin:ends,i],na.rm=TRUE))
  }

  for (i in 1:ncol(data)){
    for (j in 1:ncol(data)){
      corr[i,j]=cor(data1[begin:ends,i],data1[begin:ends,j],"pairwise.complete.obs")
    }
  }

  corrdiag=diag(1,ncol(data),ncol(data))
  corr=0.9*corr+0.1*corrdiag

  #corr=1*corr+0*corrdiag
  covar=(var%o%var)*corr

  # covar=covEstimation(as.matrix(data1[begin:ends,]),control = list(type = 'ewma',lambda=0.97))
  #

  covar[is.na(covar[,])]=0
  wgt[is.na(wgt)]=0
  realized=sqrt((t(t(wgt))%*%covar)%*%t(wgt))*sqrt(260)

  return(realized)

}

RAWlag=function(POS,Ret){
  Ret=Ret[rownames(POS),colnames(POS)]
  Strategy=as.data.frame(rowSums(Ret[3:nrow(Ret),]*POS[1:(nrow(POS)-2),],na.rm=TRUE))
}

RAW=function(POS, Ret){
  Ret=Ret[rownames(POS),colnames(POS)]
  Strategy=as.data.frame(rowSums(Ret[2:nrow(Ret),]*POS[1:(nrow(POS)-1),], na.rm=TRUE))
}

irfactor=function(TSRV,CSRV,Ret,TSweek=FALSE,CSLS="vol",TSWGT=1,CSWGT=1,ex=0,CSweek=1,monitor=1,IR=1,RB1=RBP,rpname=name1){
  #
  #
  # Args:
  #   TSRV:
  #   CSRV:
  #   Ret:
  #   TSweek:
  #   CSLS:
  #   TSWGT:
  #   CSWGT:
  #   ex:
  #   CSweek:
  #   monitor:
  #   IR: default is 1. default is different with 'factor' function
  #   RB1:
  #   rpname:
  # Returns:

  fx<- as.data.frame(read.csv("./data/fx.csv", row.names=1,header=TRUE))
  RB2=RB1

  #Factor Portfolio construction
  Assetvol=0.02
  Strategyvol=0.02
  factorvol=0.02
  factorsd=260
  assetsd=90 # asset stdev calculation period
  statsd=90
  ##volupdate trigger
  Volband=0.05

  # Calculate Volatility
  std=as.data.frame(rollapplyr(Ret,assetsd,sd))*sqrt(260)
  rownames(std)=rownames(Ret)[(assetsd):nrow(Ret)]

  STD=std*0

  # std=as.data.frame(rollapplyr(Ret,assetsd,EWMAvol,lambda=0.97))*sqrt(260)
  # rownames(std)=rownames(Ret)[(assetsd):nrow(Ret)]
  # STD=std*0

  # Making Volatility Series by Volband for individual assets
  STD[1,]=std[1,]
  print(head(std,10))
  for (k in 2:nrow(std)){
    count=0
    for(j in 1:ncol(std)){
      if(!is.na(std[k,j])){
        if(is.na(STD[k-1,j])){
          STD[k-1,j]=std[k,j]
        }
        # if there is any asset that exceed volband then add count
        if(abs(std[k,j]-STD[k-1,j])>Volband*STD[k-1,j]){
          count=count+1
        }
      }
    }
    for(jj in 1:ncol(std)){

      if (count>0) {STD[k,jj]=std[k,jj]}
      else {STD[k,jj]=STD[k-1,jj]}

    }
  }

  # Set min vol to 0.04 <- this is where 'irfactor' is different with 'factor' function
  STD[STD[,]<0.04]=0.04

  # TS Portfolio
  # Time Seires Signal change by 'TSweek'
  if(TSweek=="week"){
    TSRV1=TSRV*0
    TSRV1[1,]=TSRV[1,]

    for (k in 2:nrow(TSRV)){
      if((wday(rownames(TSRV)[k])==RB1|wday(rownames(TSRV)[k])==RB2)){TSRV1[k,]=TSRV[k,]
      } else(TSRV1[k,]=TSRV1[k-1,])
    }
  }

  if(TSweek=="month"){
    TSRV1=TSRV*0
    TSRV1[1,]=TSRV[1,]
    monthcount=month(rownames(TSRV))

    for (k in 2:nrow(CSRV)){
      if(rownames(TSRV)[k]%in%rownames(fx)){TSRV1[k,]=TSRV[k,]
      #if((month(rownames(TSRV)[k-1]))!= (month(rownames(TSRV)[k]))){TSRV1[k,]=TSRV[k,]
      } else(TSRV1[k,]=TSRV1[k-1,])
    }
  }

  if(TSweek==FALSE){TSRV1=TSRV}

  TSRV=TSRV1

  # target vol index for Time Series signal
  VCweight=Assetvol/STD
  rownames(VCweight)=rownames(std)

  AA=intersect(rownames(VCweight),rownames(TSRV))
  VCTSpos=VCweight[AA,]*TSRV[AA,] # Vol Control Time Series Position
  VCTSpos[VCTSpos[,]==Inf]=NA

  # return of VC strategy
  ret1=Ret[rownames(Ret)%in%rownames(VCTSpos),]
  Strategy=as.data.frame(rowSums(ret1[2:nrow(ret1),]*VCTSpos[1:(nrow(VCTSpos)-1),],na.rm=TRUE))
  print(head(Strategy, 30))



  # risk of strategy expanding window
  Strategyrisk=as.data.frame(Strategy[statsd:nrow(Strategy),1])
  for (i in 1:nrow(Strategyrisk)){
    Strategyrisk[i,1]=sd(Strategy[1:(i+statsd-1),1])*sqrt(260)
  }

  # risk of strategy moving window. window is specified by 'statsd'
  # if(ex!=1){
  Strategyrisk1=as.data.frame(rollapplyr(Strategy, statsd, sd)*sqrt(260))
  Strategyrisk=(Strategyrisk1+Strategyrisk)/2  # using average of expanding & moving window strategy risk

  # Making Volatility Series by Volband
  bufrisk=Strategyrisk*0
  bufrisk[1,]=Strategyrisk[1,]
  for(k in 2:nrow(Strategyrisk)){
    if(abs(Strategyrisk[k,1]-bufrisk[k-1,1])>Volband* Strategyvol){
      bufrisk[k,1]=Strategyrisk[k,1]
    } else{bufrisk[k,1]=bufrisk[k-1,1]}
  }

  # calcuate ex-post risk control with volband buffer strategy
  kk=Strategyvol/bufrisk[,1]
  statlev=matrix(rep(kk,ncol(STD)),nrow=length(kk))
  rownames(statlev)=rownames(Strategy)[statsd:nrow(Strategy)]

  TSposition=VCTSpos[rownames(VCTSpos)%in%rownames(statlev),]*statlev
  TSposition[is.na(TSposition)]=0

  # TSposition1=TSposition*0
  # sss=kk*0
  # for(k in 1:nrow(TSposition))
  #   sss[k]=findwgtvol(Ret,rownames(TSposition)[k],TSposition[k,])
  #   if(findwgt(Ret,rownames(TSposition)[k],TSposition[k,])>(2*Strategyvol)){
  #     TSposition1[k,]=TSposition[k,]/2
  #   } else (TSposition1[k,]=TSposition[k,])

  # return of Vol Control strategy lever by statlev
  TSposition=TSposition*TSWGT
  ret1=Ret[rownames(Ret)%in%rownames(TSposition),]
  TSStrategy=as.data.frame(rowSums(ret1[2:nrow(ret1),]*TSposition[1:(nrow(TSposition)-1),],na.rm=TRUE))
  TSStrategylag=as.data.frame(rowSums(ret1[3:nrow(ret1),]*TSposition[1:(nrow(TSposition)-2),],na.rm=TRUE))
  TSStrategylag=rbind(0,TSStrategylag)

  TSTO=rowSums(abs(TSposition[2:nrow(TSposition),]-TSposition[1:(nrow(TSposition)-1),]),na.rm=TRUE) # Position Change


  # CS portfolio
  ########THIS IS VOL ADJUSTED!!!
  STD1=STD[rownames(STD)%in%rownames(CSRV),]
  ret2=Ret[rownames(Ret)%in%rownames(CSRV),]
  if(CSLS=="notional"){CSRV2=CSRV}
  if(CSLS=="vol"){CSRV2=CSRV*Assetvol/STD1}


  # Calculating Ex-ante Vol and adjust to target vol which specified by 'Strategyvol'
  CSRV1=CSRV*0 # empty series would be vol adjust signal
  week=as.numeric(strftime(as.POSIXlt(rownames(CSRV)),format="%W"))
  # if weekly
  if(CSweek==1){
    CSRV1[1,]=CSRV2[1,1:ncol(CSRV1)]*Strategyvol/findwgtewma(Ret,rownames(CSRV)[1],CSRV2[1,1:ncol(CSRV1)])
    for (k in 2:nrow(CSRV)){
      if((wday(rownames(CSRV)[k])==RB1|wday(rownames(CSRV)[k])==RB2)& week[k]%%1==0 ){
        CSRV1[k,]=CSRV2[k,1:ncol(CSRV1)]*Strategyvol/findwgtewma(Ret,rownames(CSRV)[k],CSRV2[k,1:ncol(CSRV1)])
      } else(CSRV1[k,]=CSRV1[k-1,])
    }
  }
  # if monthly
  if(CSweek!=1){
    monthcount=month(rownames(CSRV))
    CSRV1[1,]=CSRV2[1,1:ncol(CSRV1)]*Strategyvol/findwgtewma(Ret,rownames(CSRV)[1],CSRV2[1,1:ncol(CSRV1)])
    for (k in 2:nrow(CSRV)){
      if(rownames(CSRV)[k]%in%rownames(fx)){
        CSRV1[k,]=CSRV2[k,1:ncol(CSRV1)]*Strategyvol/findwgtewma(Ret,rownames(CSRV)[k],CSRV2[k,1:ncol(CSRV1)])
        #if((month(rownames(CSRV)[k-1]))!= (month(rownames(CSRV)[k]))){CSRV1[k,]=CSRV2[k,1:ncol(CSRV1)]*Strategyvol/findwgtewma(Ret,rownames(CSRV)[k],CSRV2[k,1:ncol(CSRV1)])
      } else(CSRV1[k,]=CSRV1[k-1,])
    }
  }


  ##Need to make time series of CS raw strategy, then vol control and match dates with TS strategy
  CSraw=as.data.frame(rowSums(ret2[2:nrow(ret2),]*CSRV1[1:(nrow(CSRV1)-1),],na.rm=TRUE))
  # if 'ex' == 1, expanding window, otherwise moving window. default is 0
  if(ex==1){
    CSrisk=as.data.frame(CSraw[statsd:nrow(CSraw),1])
    for (i in 1:nrow(CSrisk)){
      CSrisk[i,1]=sd(CSraw[1:(i+statsd-1),1])*sqrt(260)

    }
  }
  if(ex!=1){
    CSrisk=as.data.frame(rollapplyr(CSraw, statsd, sd)*sqrt(260))
  }


  rownames(CSrisk)=rownames(CSraw)[statsd:nrow(CSraw)]
  week=as.numeric(strftime(as.POSIXlt(rownames(CSrisk)),format="%W"))

  # Making Volatility Series by Volband
  bufrisk1=CSrisk*0
  bufrisk1[1,]=CSrisk[1,]
  if(monitor==1){
    for(k in 2:nrow(CSrisk)){
      if(abs(CSrisk[k,1]-bufrisk1[k-1,1])>Volband* Strategyvol & (wday(rownames(CSrisk)[k])==RB1|wday(rownames(CSrisk)[k])==RB2)){
        bufrisk1[k,1]=CSrisk[k,1]
      } else{bufrisk1[k,1]=bufrisk1[k-1,1]}

    }}
  if(monitor==0){
    for(k in 2:nrow(CSrisk)){
      if(abs(CSrisk[k,1]-bufrisk1[k-1,1])>Volband* Strategyvol & (rownames(CSrisk)[k] %in% rownames(fx))){
        # if(abs(CSrisk[k,1]-bufrisk1[k-1,1])>Volband* Strategyvol & (month(rownames(CSrisk)[k])!=month(rownames(CSrisk)[k-1]))){
        bufrisk1[k,1]=CSrisk[k,1]
      } else{bufrisk1[k,1]=bufrisk1[k-1,1]}

    }}

  # calcuate ex-post risk control with volband buffer strategy
  # kkk=(Strategyvol/bufrisk1[,1])*0+1
  kkk=Strategyvol/bufrisk1[,1]
  statlev1=as.data.frame(matrix(rep(kkk,ncol(STD)),nrow=length(kkk)))
  rownames(statlev1)=rownames(CSrisk)

  CSposition=CSRV1[rownames(CSRV1)%in%rownames(statlev1),]*statlev1*CSWGT
  CSposition[is.na(CSposition)]=0


  ret1=Ret[rownames(Ret)%in%rownames(CSposition),]

  CSStrategy=as.data.frame(rowSums(ret1[2:nrow(ret1),]*CSposition[1:(nrow(CSposition)-1),],na.rm=TRUE))
  CSStrategylag=as.data.frame(rowSums(ret1[3:nrow(ret1),]*CSposition[1:(nrow(CSposition)-2),],na.rm=TRUE))
  CSStrategylag=rbind(0,CSStrategylag)
  CSTO=rowSums(abs(CSposition[2:nrow(CSposition),]-CSposition[1:(nrow(CSposition)-1),]),na.rm=TRUE)

  ####output
  CS=CSposition
  TS=TSposition


  #####Factor vol control
  # factor=CSposition+TSposition
  #
  # ret3=Ret[rownames(Ret)%in%rownames(factor),]
  # factorraw=as.data.frame(rowSums(ret3[2:nrow(ret3),]*factor[1:(nrow(factor)-1),],na.rm=TRUE))
  # factorrisk=as.data.frame(rollapplyr(factorraw, factorsd, sd)*sqrt(260))
  # rownames(factorrisk)=rownames(factorraw)[factorsd:nrow(factorraw)]
  #
  # bufrisk2=factorrisk*0
  # bufrisk2[1,]=factorrisk[1,]
  # if(monitor==1){
  # for(k in 2:nrow(factorrisk)){
  #   if(abs(factorrisk[k,1]-bufrisk2[k-1,1])>Volband*factorrisk[k,1] & (wday(rownames(factorrisk)[k])==RB1|wday(rownames(factorrisk)[k])==RB2)){
  #     bufrisk2[k,1]=factorrisk[k,1]
  #   } else{bufrisk2[k,1]=bufrisk2[k-1,1]}
  # }}
  #
  # if(monitor==0){
  # for(k in 2:nrow(factorrisk)){
  #   if(abs(factorrisk[k,1]-bufrisk2[k-1,1])>Volband*factorrisk[k,1] & (month(rownames(factorrisk)[k])!=month(rownames(factorrisk)[k-1]))){
  #     bufrisk2[k,1]=factorrisk[k,1]
  #   } else{bufrisk2[k,1]=bufrisk2[k-1,1]}
  # }
  # }
  #
  # kkkk=factorvol/bufrisk2[,1]
  # factorlev1=as.data.frame(matrix(rep(kkkk,ncol(STD)),nrow=length(kkkk)))
  # rownames(factorlev1)=rownames(factorrisk)
  # factorposition=factor[rownames(factor)%in%rownames(factorlev1),]*factorlev1
  # colnames(factorposition)=colnames(index)
  #
  # AGGTO=rowSums(abs(factorposition[2:nrow(factorposition),]-factorposition[1:(nrow(factorposition)-1),]),na.rm=TRUE)
  #
  #
  #
  # ret4=Ret[rownames(Ret)%in%rownames(factorposition),]
  #
  #
  # factorreturn=as.data.frame(rowSums(ret4[2:nrow(ret4),]*factorposition[1:(nrow(factorposition)-1),],na.rm=TRUE))
  # #quickcheck
  # output=cbind(TSStrategy,TSStrategylag,CSStrategy,CSStrategylag,TSTO,CSTO)
  # output1=output[rownames(output)%in%rownames(factorreturn),]
  # output2=cbind(factorreturn,output1,AGGTO)

  # giveup=list(factorposition,output2,TSposition,CSposition,TSRV,CSRV1,kk,kkk,kkkk)

  #TScheck11=paste(100*round(sum(RAWlag(TSposition[rownames(TSposition)>perfdate11,],Ret)),4),"%",sep="")
  TScheck1=paste(100*round(sum(RAWlag(TSposition[rownames(TSposition)>perfdate1,],Ret)),4),"%",sep="")
  TScheck2=paste(100*round(sum(RAWlag(TSposition[rownames(TSposition)>perfdate2,],Ret)),4),"%",sep="")
  TScheck3=paste(100*round(sum(RAWlag(TSposition[rownames(TSposition)>perfdate3,],Ret)),4),"%",sep="")

  #CScheck11=paste(100*round(sum(RAWlag(CSposition[rownames(CSposition)>perfdate11,],Ret)),4),"%",sep="")
  CScheck1=paste(100*round(sum(RAWlag(CSposition[rownames(CSposition)>perfdate1,],Ret)),4),"%",sep="")
  CScheck2=paste(100*round(sum(RAWlag(CSposition[rownames(CSposition)>perfdate2,],Ret)),4),"%",sep="")
  CScheck3=paste(100*round(sum(RAWlag(CSposition[rownames(CSposition)>perfdate3,],Ret)),4),"%",sep="")

  print(paste(TScheck1,TScheck2,TScheck3)) #TScheck11,
  print(paste(CScheck1,CScheck2,CScheck3)) #CScheck11,
  print(colSums(TSRV))
  print(tail(TSRV))
  print(colSums(CSRV))
  print(tail(CSRV))

  nolag_perf = cbind(RAW(TSposition[rownames(TSposition)>perfdate4,],Ret),RAW(CSposition[rownames(CSposition)>perfdate4,],Ret))
  colnames(nolag_perf) = c("TS", "CS")
  lag_perf = cbind(RAWlag(TSposition[rownames(TSposition)>perfdate4,],Ret),RAWlag(CSposition[rownames(CSposition)>perfdate4,],Ret))
  colnames(lag_perf) = c("TS", "CS")

  #charts.PerformanceSummary(cbind(RAW(TSposition[rownames(TSposition)>perfdate4,],Ret),RAW(CSposition[rownames(CSposition)>perfdate4,],Ret)),main=rpname)
  #charts.PerformanceSummary(cbind(RAWlag(TSposition[rownames(TSposition)>perfdate4,],Ret),RAWlag(CSposition[rownames(CSposition)>perfdate4,],Ret)),main=paste(rpname,"lag"))

  giveup=list(TSposition,CSposition,TSRV1,CSRV1, nolag_perf, lag_perf)

  return(giveup)
}
