IRCSmod=0.8
IRTSmod=0.8
ComCSmod=0.8
ComTSmod=0.8
DMCSmod=1
EMCSmod=1
DMTSmod=1
EMTSmod=1
###################################################################################
positions=1
##기존 포지션 이용
updatepos=0
##기존 포지션에 오늘 포지션 더함
###########################################################################################
library(PerformanceAnalytics)
library(bizdays)
library(zoo)
library(Hmisc)
library(RiskPortfolios)
library(lubridate)
library(qcc)
library(matrixStats)

library(MTS)
RBP=3
setwd("D:/R/GRP")
targetvol=0.0475
#targetvol=0.035
Volband=0.05
elimit=3.8

test=1
##하드코딩된 과거 시그널 사용시 0, 새로운 시그널 생성시 1

write=0
##하드코딩된 시그널 오버라이드해서 새로 저장시 1

JGB="out"
##일본 채권 미포함시 "out"

IDN="out"
# #indonesia 뺄려면 "out"
name1=test

####원래 세팅

updatedaily=1
##매일매일 시그널 한줄 record keeping할시 (리벨런싱 하는날 전날 수익률 체크만하고 싶을때) 단, ISI는 아래에서 따로 관리

isiready=1
##citi ISI 시그널 없데이트 안되었을때 0으로 놓고 돌리면 데이터베이스에 시그널값 저장 안함 (내일 1로 놓고 돌리면 그때 어제 값도 저장)

RB1=3


library(gdata)
eindex <- as.data.frame(read.csv("D:/R/GRP/totindex.csv", row.names=1,header=TRUE))
eindex1 <- as.data.frame(read.csv("D:/R/GRP/priceindex.csv", row.names=1,header=TRUE))
efut <- as.data.frame(read.csv("D:/R/GRP/FutGenratio1.csv", row.names=1,header=TRUE))
rownames(efut)=as.Date(as.numeric(rownames(efut)),origin = "1899-12-30")
cindex <- as.data.frame(read.csv("D:/R/GRP/fut1return-com.csv", row.names=1,header=TRUE))
rownames(cindex)=as.Date(as.numeric(rownames(cindex)),origin = "1899-12-30")
cindex1 <- as.data.frame(read.csv("D:/R/GRP/BCOM.csv", row.names=1,header=TRUE))
emindex <- as.data.frame(read.csv("D:/R/GRP/totindex-em.csv", row.names=1,header=TRUE))
emindex1 <- as.data.frame(read.csv("D:/R/GRP/priceindex-em.csv", row.names=1,header=TRUE))
emfut <- as.data.frame(read.csv("D:/R/GRP/fut1return-em.csv", row.names=1,header=TRUE))
rownames(emfut)=as.Date(as.numeric(rownames(emfut)),origin = "1899-12-30")
eindex1=eindex1[rownames(eindex1)<"2008-01-01",]
ERetp=eindex1[2:nrow(eindex1),]/eindex1[1:(nrow(eindex1)-1),]-1 #p
eindex=eindex[rownames(eindex)<"2008-01-01",]
ERett=eindex[2:nrow(eindex),]/eindex[1:(nrow(eindex)-1),]-1 #
fRet=efut[2:nrow(efut),]/efut[1:(nrow(efut)-1),]-1

bond <- as.data.frame(read.csv("D:/R/GRP/bonds.csv", row.names=1,header=TRUE))
rownames(bond)=as.Date(as.numeric(rownames(bond)),origin = "1899-12-30")
BRet=bond[2:nrow(bond),]/bond[1:(nrow(bond)-1),]-1
if(JGB=="out"){
  BRet=BRet[,1:4]
  bond=bond[,1:4]
}
Bindex=BRet*0

Bindex[1,]=matrix(rep(1,ncol(Bindex)),nrow=1)
for (i in 2:nrow(Bindex)){
  Bindex[i,]=Bindex[(i-1),]*(1+BRet[i,])
}

compRet=ERett

compRet[is.na(compRet)]=ERetp[is.na(compRet)]
ERet1=compRet
ERet=rbind(compRet[rownames(ERet1)<"2008-01-01",],fRet[rownames(fRet)>="2008-01-01",])

CRet1=cindex1[2:nrow(cindex1),]/cindex1[1:(nrow(cindex1)-1),]-1
CRet=cindex[2:nrow(cindex),]/cindex[1:(nrow(cindex)-1),]-1
CRet[is.na(CRet)]=CRet1[is.na(CRet)]

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

ERet[is.na(ERet)]=0
Eindex=ERet*0
Eindex[1,]=matrix(rep(1,ncol(Eindex)),nrow=1)
for (i in 2:nrow(Eindex)){
  Eindex[i,]=Eindex[(i-1),]*(1+ERet[i,])
}

CRet[is.na(CRet)]=0
Cindex=CRet*0
Cindex[1,]=matrix(rep(1,ncol(Cindex)),nrow=1)
for (i in 2:nrow(Cindex)){
  Cindex[i,]=Cindex[(i-1),]*(1+CRet[i,])
}

EMRet[is.na(EMRet)]=0
EMindex=EMRet*0
EMindex[1,]=matrix(rep(1,ncol(EMindex)),nrow=1)
for (i in 2:nrow(EMindex)){
  EMindex[i,]=EMindex[(i-1),]*(1+EMRet[i,])
}


source('D:/R/GRP/all stat final.R')


####create dataframe of returns from longest date
setwd("D:/R/GRP")

stat1=CCA3(RB1=RBP,RB2=RBP,RET=CRet,index=Cindex,CSdesign="vol")
stat2=CPM(RB1=RBP,RB2=RBP,RET=CRet,index=Cindex,CSdesign="vol")
stat3=CSS(RB1=RBP,RB2=RBP,RET=CRet,index=Cindex,CSdesign="vol")
stat4=CVA2(RB1=RBP,RB2=RBP,RET=CRet,index=Cindex,CSdesign="vol")
# statx=CVA3(RB1=RBP,RB2=RBP,RET=CRet,index=Cindex,CSdesign="vol")
stat5=CVO(RB1=RBP,RB2=RBP,RET=CRet,index=Cindex,CSdesign="vol")
stat6=ECA(RB1=RBP,RB2=RBP,RET=ERet,index=Eindex,CSdesign="notional")
stat7=EDY(RB1=RBP,RB2=RBP,RET=ERet,index=Eindex,CSdesign="notional")
stat8=EEM(RB1=RBP,RB2=RBP,RET=ERet,index=Eindex,CSdesign="notional")
stat9=EFX(RB1=RBP,RB2=RBP,RET=ERet,index=Eindex,CSdesign="notional")
stat10=CVA3(RB1=RBP,RB2=RBP,RET=CRet,index=Cindex,CSdesign="vol")
stat11=ELQ(RB1=RBP,RB2=RBP,RET=ERet,index=Eindex,CSdesign="notional")
stat12=EPE(RB1=RBP,RB2=RBP,RET=ERet,index=Eindex,CSdesign="notional")
stat13=EPM(RB1=RBP,RB2=RBP,RET=ERet,index=Eindex,CSdesign="notional")
stat14=EQL(RB1=RBP,RB2=RBP,RET=ERet,index=Eindex,CSdesign="notional")
stat15=ESS(RB1=RBP,RB2=RBP,RET=ERet,index=Eindex,CSdesign="notional")
stat16=EST(RB1=RBP,RB2=RBP,RET=ERet,index=Eindex,CSdesign="notional")
stat17=EVO(RB1=RBP,RB2=RBP,RET=ERet,index=Eindex,CSdesign="notional")
stat18=EMPM(RB1=RBP,RB2=RBP,RET=EMRet,index=EMindex,CSdesign="notional")
stat19=EMCA(RB1=RBP,RB2=RBP,RET=EMRet,index=EMindex,CSdesign="notional")
stat20=EMSS(RB1=RBP,RB2=RBP,RET=EMRet,index=EMindex,CSdesign="notional")
stat21=EMVO(RB1=RBP,RB2=RBP,RET=EMRet,index=EMindex,CSdesign="notional")
stat22=EMDY(RB1=RBP,RB2=RBP,RET=EMRet,index=EMindex,CSdesign="notional")
stat23=EMPE(RB1=RBP,RB2=RBP,RET=EMRet,index=EMindex,CSdesign="notional")
stat24=CVA(RB1=RBP,RB2=RBP,RET=CRet,index=Cindex,CSdesign="vol")

# IRS1=IPM(RB1=RBP,RB2=RBP,RET=BRet,index=Bindex,CSdesign="notional")
# IRS2=ICA(RB1=RBP,RB2=RBP,RET=BRet,index=Bindex,CSdesign="notional")
# IRS3=ISS(RB1=RBP,RB2=RBP,RET=BRet,index=Bindex,CSdesign="notional")
# IRS4=Ieq(RB1=RBP,RB2=RBP,RET=BRet,index=Bindex,CSdesign="notional")
# IRS5=IRV(RB1=RBP,RB2=RBP,RET=BRet,index=Bindex,CSdesign="notional")

IRS1=IPM(RB1=RBP,RB2=RBP,RET=BRet,index=Bindex,CSdesign="vol")
IRS2=ICA(RB1=RBP,RB2=RBP,RET=BRet,index=Bindex,CSdesign="vol")
IRS3=ISS(RB1=RBP,RB2=RBP,RET=BRet,index=Bindex,CSdesign="vol")
IRS4=Ieq(RB1=RBP,RB2=RBP,RET=BRet,index=Bindex,CSdesign="vol")
IRS5=IRV(RB1=RBP,RB2=RBP,RET=BRet,index=Bindex,CSdesign="vol")
IRS6=ICA2(RB1=RBP,RB2=RBP,RET=BRet,index=Bindex,CSdesign="vol")

# stat18=ERA(RB1=RBP,RB2=RBP)
setwd("D:/R/GRP")




allcount=ncol(emindex)+ncol(cindex)+ncol(eindex)+ncol(Bindex)
emstart=ncol(eindex)+ncol(cindex)+1
emend=ncol(emindex)+ncol(cindex)+ncol(eindex)
comstart=ncol(eindex)+1
comend=ncol(eindex)+ncol(cindex)
IRstart=ncol(emindex)+ncol(cindex)+ncol(eindex)+1
Ret=cbind(ERet,ERet,ERet,ERet,ERet)[,1:allcount]*0
colnames(Ret)[comstart:comend]=colnames(CRet)
colnames(Ret)[emstart:emend]=colnames((EMRet))
colnames(Ret)[IRstart:allcount]=colnames((BRet))
Ret[rownames(ERet),colnames(ERet)]=ERet
Ret[rownames(CRet),colnames(CRet)]=CRet
Ret[rownames(EMRet),colnames(EMRet)]=EMRet
Ret[rownames(BRet),colnames(BRet)]=BRet
UNIRET=Ret
# Xindex=Ret*0
# Xindex[1,]=matrix(rep(1,ncol(index)),nrow=1)
# for (i in 2:nrow(Xindex)){
#   Xindex[i,]=Xindex[(i-1),]*(1+Ret[i,])
# }
# Xindex=Xindex[rownames(Xindex)>="1996-01-03",]
# 
# XMOM=XXM(RB1=RBP,RB2=RBP,RET=Ret,index=Xindex,CSdesign="notional")


CSdump=list()
CSdump[[1]]=stat1[[2]]
CSdump[[2]]=stat2[[2]]
CSdump[[3]]=stat3[[2]]
CSdump[[4]]=stat4[[2]]
CSdump[[5]]=stat5[[2]]
CSdump[[6]]=stat6[[2]]
CSdump[[7]]=stat7[[2]]
CSdump[[8]]=stat8[[2]]
CSdump[[9]]=stat9[[2]]
CSdump[[10]]=stat10[[2]]
CSdump[[11]]=stat11[[2]]
CSdump[[12]]=stat12[[2]]
CSdump[[13]]=stat13[[2]]
CSdump[[14]]=stat14[[2]]
CSdump[[15]]=stat15[[2]]
CSdump[[16]]=stat16[[2]]
CSdump[[17]]=stat17[[2]]
CSdump[[18]]=stat18[[2]]
CSdump[[19]]=stat19[[2]]
CSdump[[20]]=stat20[[2]]
CSdump[[21]]=stat21[[2]]
CSdump[[22]]=stat22[[2]]
CSdump[[23]]=stat23[[2]]
CSdump[[24]]=stat24[[2]]

CSdump[[25]]=IRS1[[2]]
CSdump[[26]]=IRS2[[2]]
CSdump[[27]]=IRS3[[2]]
CSdump[[28]]=IRS4[[2]]
CSdump[[29]]=IRS5[[2]]
CSdump[[30]]=IRS6[[2]]

# CSdump[[30]]=XMOM[[2]]


TSdump=list()
TSdump[[1]]=stat1[[1]]
TSdump[[2]]=stat2[[1]]
TSdump[[3]]=stat3[[1]]
TSdump[[4]]=stat4[[1]]
TSdump[[5]]=stat5[[1]]
TSdump[[6]]=stat6[[1]]
TSdump[[7]]=stat7[[1]]
TSdump[[8]]=stat8[[1]]
TSdump[[9]]=stat9[[1]]
TSdump[[10]]=stat10[[1]]
TSdump[[11]]=stat11[[1]]
TSdump[[12]]=stat12[[1]]
TSdump[[13]]=stat13[[1]]
TSdump[[14]]=stat14[[1]]
TSdump[[15]]=stat15[[1]]
TSdump[[16]]=stat16[[1]]
TSdump[[17]]=stat17[[1]]
TSdump[[18]]=stat18[[1]]
TSdump[[19]]=stat19[[1]]
TSdump[[20]]=stat20[[1]]
TSdump[[21]]=stat21[[1]]
TSdump[[22]]=stat22[[1]]
TSdump[[23]]=stat23[[1]]
TSdump[[24]]=stat24[[1]]

TSdump[[25]]=IRS1[[1]]
TSdump[[26]]=IRS2[[1]]
TSdump[[27]]=IRS3[[1]]
TSdump[[28]]=IRS4[[1]]
TSdump[[29]]=IRS5[[1]]
TSdump[[30]]=IRS6[[1]]
#####make all strategies start at same date (start of the longest strategy), replace missing with 0 returns

nstat=29
startdate=today()
listofdates=vector()
for (c in 1:nstat){
  
  startdate=min(startdate,as.Date(rownames(TSdump[[c]])[1]))
  listofdates[c]=as.Date(rownames(TSdump[[c]])[1])
}

dumpTS=list()
dumpCS=list()

emptyE=stat7[[1]]*0
emptyC=cbind(emptyE,emptyE)[,1:ncol(cindex)]
emptyEM=cbind(emptyE)[,1:ncol(emindex)]
emptyI=cbind(emptyE)[,1:ncol(Bindex)]
colnames(emptyC)=colnames(stat1[[1]])
colnames(emptyEM)=colnames(stat22[[1]]) 
colnames(emptyI)=colnames(IRS1[[1]]) 
for (c in 1:nstat){
  if(c<6){dumpTS[[c]]=emptyC
  dumpCS[[c]]=emptyC}
  
  if(c>=6&c<18){dumpTS[[c]]=emptyE
  dumpCS[[c]]=emptyE}
  
  if(c>=18&c<24){dumpTS[[c]]=emptyEM
  dumpCS[[c]]=emptyEM}  
  
  if(c==24){dumpTS[[c]]=emptyC
  dumpCS[[c]]=emptyC}  
  
  if(c>24){dumpTS[[c]]=emptyI
  dumpCS[[c]]=emptyI}  
  
  dumpTS[[c]][rownames(TSdump[[c]]),]=TSdump[[c]]
  dumpCS[[c]][rownames(CSdump[[c]]),]=CSdump[[c]]
}


dumpCS[[30]]=emptyI
dumpCS[[30]][rownames(CSdump[[30]]),]=CSdump[[30]]
dumpTS[[30]]=emptyI
dumpTS[[30]][rownames(TSdump[[30]]),]=TSdump[[30]]
# emptyx=cbind(emptyE,emptyC,emptyEM,emptyI)
# dumpCS[[30]]=emptyx
# dumpCS[[30]][rownames(CSdump[[30]]),]=CSdump[[30]]


####Create 7 Factors: Value, Mom, Risk, Mif, Maf, Carry,Seas [for Com, Eq -> CS and TS]
##TS strategies
Ret=UNIRET
C_Carry_TS=dumpTS[[1]]
C_Mom_TS=dumpTS[[2]]  
C_Seas_TS=dumpTS[[3]]
#C_Value_TS=dumpTS[[4]] 
C_Risk_TS=dumpTS[[5]]
E_Carry_TS=dumpTS[[6]]
E_Seas_TS=dumpTS[[15]]
E_Mom_TS=dumpTS[[13]]
EM_Mom_TS=dumpTS[[18]]
EM_Carry_TS=dumpTS[[19]]
EM_Seas_TS=dumpTS[[20]]
EM_Risk_TS=dumpTS[[21]]
#C_Value1_TS=dumpTS[[24]] 
IR_Mom_TS=dumpTS[[25]]
IR_Carry_TS=TS2=dumpTS[[26]]
IR_Seas_TS=dumpTS[[27]]
IR_Risk_TS=dumpTS[[28]]
IR_Value_TS=dumpTS[[29]]

Ret=UNIRET
fE_Value_TS=TScombine(TS1=dumpTS[[12]],TS2=dumpTS[[7]],WGT1=1,WGT2=1,Ret=Ret,RB1=RBP)
fE_Maf_TS=TScombine(TS1=dumpTS[[9]],TS2=dumpTS[[11]],WGT1=1,WGT2=1,Ret=Ret,RB1=RBP)
fE_Risk_TS=TScombine(TS1=dumpTS[[16]],TS2=dumpTS[[17]],WGT1=0.5,WGT2=1,Ret=Ret,RB1=RBP)
fE_Mif_TS=TScombine(TS1=dumpTS[[14]],TS2=dumpTS[[8]],WGT1=1,WGT2=1,Ret=Ret,RB1=RBP)  ###WGT2 used to be 0.5
fEM_Value_TS=TScombine(TS1=dumpTS[[22]],TS2=dumpTS[[23]],WGT1=1,WGT2=1,Ret=Ret,RB1=RBP)
# fC_Value_TS=TScombine(TS1=dumpTS[[4]],TS2=dumpTS[[24]],WGT1=1,WGT2=0.2,Ret=Ret,RB1=RBP)
# fC_Value_TS=TScombine(TS1=dumpTS[[4]],TS2=dumpTS[[24]],TS3=dumpTS[[10]],WGT1=0.5,WGT2=1,WGT3=0.25,Ret=Ret,RB1=RBP)
fC_Value_TS=TScombine(TS1=dumpTS[[4]],TS2=dumpTS[[10]],WGT1=1,WGT2=1,Ret=Ret,RB1=RBP)
fC_Risk_TS=TScombine(TS1=dumpTS[[5]],TS2=dumpTS[[24]],WGT1=1,WGT2=1,Ret=Ret,RB1=RBP)
C_Risk_TS=fC_Risk_TS[[1]]


E_Value_TS=fE_Value_TS[[1]]
E_Maf_TS=fE_Maf_TS[[1]]
E_Risk_TS=fE_Risk_TS[[1]]
E_Mif_TS=fE_Mif_TS[[1]]  ###WGT2 used to be 0.5
EM_Value_TS=fEM_Value_TS[[1]]
C_Value_TS=fC_Value_TS[[1]]



##CS strategies
C_Carry_CS=dumpCS[[1]]
C_Mom_CS=dumpCS[[2]]
C_Seas_CS=dumpCS[[3]]
#C_Value_CS=dumpCS[[4]] 
C_Risk_CS=dumpCS[[5]] 
E_Carry_CS=dumpCS[[6]]
E_Seas_CS=dumpCS[[15]]
E_Mom_CS=dumpCS[[13]]
EM_Mom_CS=dumpCS[[18]]
EM_Carry_CS=dumpCS[[19]]
EM_Seas_CS=dumpCS[[20]]
EM_Risk_CS=dumpCS[[21]]

IR_Mom_CS=dumpCS[[25]]
#IR_Carry_CS=dumpCS[[26]]

IR_Seas_CS=dumpCS[[27]]
IR_Risk_CS=dumpCS[[28]]
IR_Value_CS=dumpCS[[29]]
# 
# XX_Mom=dumpCS[[30]]

Ret=UNIRET
fE_Value_CS=CScombine(CS1=dumpCS[[12]],CS2=dumpCS[[7]],WGT1=0.5,WGT2=1,Ret=Ret)
fE_Maf_CS=CScombine(CS1=dumpCS[[9]],CS2=dumpCS[[11]],WGT1=1,WGT2=0.5,Ret=Ret)  ##ISI out
fE_Risk_CS=CScombine(CS1=dumpCS[[16]],CS2=dumpCS[[17]],WGT1=1,WGT2=0.2,Ret=Ret)
fE_Mif_CS=CScombine(CS1=dumpCS[[14]],CS2=dumpCS[[8]],WGT1=1,WGT2=1,Ret=Ret)
fEM_Value_CS=CScombine(CS1=dumpCS[[22]],CS2=dumpCS[[23]],WGT1=1,WGT2=1,Ret=Ret)
fIR_Carry_CS=CScombine(CS1=dumpCS[[26]],CS2=dumpCS[[30]],WGT1=1,WGT2=0.9,Ret=Ret)
# fC_Value_CS=CScombine(CS1=dumpCS[[4]],CS2=dumpCS[[24]],WGT1=1,WGT2=0.2,Ret=Ret)

# fC_Value_CS=CScombine(CS1=dumpCS[[4]],CS2=dumpCS[[24]],CS3=dumpCS[[10]],WGT1=0.5,WGT2=1,WGT3=0.25,Ret=Ret)

fC_Value_CS=CScombine(CS1=dumpCS[[4]],CS2=dumpCS[[10]],WGT1=0.5,WGT2=1,Ret=Ret)
fC_Risk_CS=CScombine(CS1=dumpCS[[5]],CS2=dumpCS[[24]],WGT1=1,WGT2=1,Ret=Ret)
C_Risk_CS=fC_Risk_CS[[1]]



E_Value_CS=fE_Value_CS[[1]]

E_Maf_CS=fE_Maf_CS[[1]]
E_Risk_CS=fE_Risk_CS[[1]]
E_Mif_CS=fE_Mif_CS[[1]]
EM_Value_CS=fEM_Value_CS[[1]]
C_Value_CS=fC_Value_CS[[1]]
IR_Carry_CS=fIR_Carry_CS[[1]]
############################################
Ret=UNIRET
#rawstrategies
TS_RAW=cbind(RAWlag(C_Carry_TS,Ret),RAWlag(C_Mom_TS,Ret),RAWlag(C_Seas_TS,Ret),RAWlag(C_Value_TS,Ret),RAWlag(C_Risk_TS,Ret),RAWlag(E_Carry_TS,Ret),
             RAWlag(E_Seas_TS,Ret),RAWlag(E_Mom_TS,Ret),RAWlag(E_Value_TS,Ret),RAWlag(E_Maf_TS,Ret),RAWlag(E_Risk_TS,Ret),RAWlag(E_Mif_TS,Ret),
             RAWlag(EM_Mom_TS,Ret),RAWlag(EM_Carry_TS,Ret),RAWlag(EM_Seas_TS,Ret),RAWlag(EM_Risk_TS,Ret),RAWlag(EM_Value_TS,Ret),RAWlag(IR_Mom_TS,Ret),RAWlag(IR_Carry_TS,Ret),RAWlag(IR_Seas_TS,Ret),RAWlag(IR_Risk_TS,Ret),RAWlag(IR_Value_TS,Ret))
colnames(TS_RAW)=c("C_Carry_TS","C_Mom_TS","C_Seas_TS","C_Value_TS","C_Risk_TS","E_Carry_TS","E_Seas_TS","E_Mom_TS","E_Value_TS","E_Maf_TS","E_Risk_TS","E_Mif_TS","EM_Mom_TS","EM_Carry_TS",
                   "EM_Seas_TS","EM_Risk_TS","EM_Value_TS","IR_Mom_TS","IR_Carry_TS","IR_Seas_TS","IR_Risk_TS","IR_Value_TS")



CS_RAW=cbind(RAWlag(C_Carry_CS,Ret),RAWlag(C_Mom_CS,Ret),RAWlag(C_Seas_CS,Ret),RAWlag(C_Value_CS,Ret),RAWlag(C_Risk_CS,Ret),RAWlag(E_Carry_CS,Ret),
             RAWlag(E_Seas_CS,Ret),RAWlag(E_Mom_CS,Ret),RAWlag(E_Value_CS,Ret),RAWlag(E_Maf_CS,Ret),RAWlag(E_Risk_CS,Ret),RAWlag(E_Mif_CS,Ret),
             RAWlag(EM_Mom_CS,Ret),RAWlag(EM_Carry_CS,Ret),RAWlag(EM_Seas_CS,Ret),RAWlag(EM_Risk_CS,Ret),RAWlag(EM_Value_CS,Ret),RAWlag(IR_Mom_CS,Ret),RAWlag(IR_Carry_CS,Ret),RAWlag(IR_Seas_CS,Ret),RAWlag(IR_Risk_CS,Ret),RAWlag(IR_Value_CS,Ret))
colnames(CS_RAW)=c("C_Carry_CS","C_Mom_CS","C_Seas_CS","C_Value_CS","C_Risk_CS","E_Carry_CS","E_Seas_CS","E_Mom_CS","E_Value_CS","E_Maf_CS","E_Risk_CS","E_Mif_CS","EM_Mom_CS","EM_Carry_CS",
                   "EM_Seas_CS","EM_Risk_CS","EM_Value_CS","IR_Mom_CS","IR_Carry_CS","IR_Seas_CS","IR_Risk_CS","IR_Value_CS")
setwd("D:/R/GRP")

write.csv(TS_RAW[rownames(TS_RAW)>"1995-12-30",],"TS_RAW.csv")

write.csv(CS_RAW[rownames(CS_RAW)>"1995-12-30",],"CS_RAW.csv")

write.csv(cbind(TS_RAW[rownames(TS_RAW)>"1995-12-30",],CS_RAW[rownames(CS_RAW)>"1995-12-30",]),"factors.csv")
# facttime=TS_RAW[rownames(TS_RAW)>"2000-12-30",]+CS_RAW[rownames(CS_RAW)>"2000-12-30",]
# factcum=apply(facttime+1,2,cumprod)
# fx<- as.data.frame(read.csv("D:/R/GRP/fx.csv", row.names=1,header=TRUE))
# factcummon=factcum[rownames(factcum)%in%rownames(fx),]
# factcummon=rbind(rep(1,ncol(factcummon)),factcummon)
# factcummon=factcummon[2:nrow(factcummon),]/factcummon[1:(nrow(factcummon)-1),]-1
# factcummon=factcummon[order(rownames(factcummon),decreasing=TRUE),]
# K=ncol(factcummon)
# M=nrow(factcummon)
# MM=floor(M/12)
# 
# corts=matrix(NA,12,MM*K)
# 
# for (i in 1:MM){
#   corts[,(K*(i-1)+1):(K*(i-1)+K)]=factcummon[(12*(i-1)+1):(12*(i-1)+12),]
# }
# 
# corts1=cor(corts)
# write.csv(corts1,"runningcorrelation.csv")
############################################




###Commodity_CS

Ret=UNIRET
C_CS=C_Carry_CS*1+C_Mom_CS*1+C_Seas_CS*1+C_Value_CS+C_Risk_CS*1
#C_CS=CScombine(CS1=C_CS,CS2=C_CS,WGT1=1,WGT2=1,Ret=Ret)[[1]]
C_TS=C_Carry_TS*0.75+C_Mom_TS*1+C_Seas_TS*0.75+C_Value_TS*0.75+C_Risk_TS

E_CS=E_Carry_CS*0.75+E_Mom_CS*0.75+E_Seas_CS*0.25+E_Value_CS+E_Risk_CS*1+E_Maf_CS+E_Mif_CS
#E_CS=CScombine(CS1=E_CS,CS2=E_CS,WGT1=1,WGT2=1,Ret=Ret)[[1]]
E_TS=E_Carry_TS+E_Mom_TS+E_Seas_TS*0.5+E_Value_TS+E_Risk_TS*0.75+E_Maf_TS+E_Mif_TS*1

EM_CS=EM_Carry_CS*0.75+EM_Mom_CS*0.33+EM_Seas_CS*0.2+EM_Value_CS*1+EM_Risk_CS*1
#EM_CS=CScombine(CS1=EM_CS,CS2=EM_CS,WGT1=1,WGT2=1,Ret=Ret)[[1]]
EM_TS=EM_Carry_TS*1+EM_Mom_TS+EM_Seas_TS*0.5+EM_Value_TS+EM_Risk_TS

IR_CS=IR_Carry_CS*1+IR_Mom_CS*1+IR_Seas_CS*0.75+IR_Value_CS*1+IR_Risk_CS*0.75
#IR_CS=CScombine(CS1=IR_CS,CS2=IR_CS,WGT1=1,WGT2=1,Ret=Ret)[[1]]
IR_TS=IR_Carry_TS*1+IR_Mom_TS*1+IR_Seas_TS*0.5+IR_Value_TS+IR_Risk_TS
assetfactors=cbind(RAW(C_CS,Ret),RAW(C_TS,Ret),RAW(E_CS,Ret),RAW(E_TS,Ret),RAW(EM_CS,Ret),RAW(EM_TS,Ret),RAW(IR_CS,Ret),RAW(IR_TS,Ret))



write.csv(assetfactors,"af.csv")

#print(tail(assetfactors))
####

for (TSnum in 2:2){
  Ret=UNIRET
  setwd("D:/R/GRP")
  
  #fCS=Assetcombine4(Com=C_CS,Eq=E_CS,EM=EM_CS,IR=IR_CS,Ret=Ret,CWGT=1.2,EMWGT=0.5,IRWGT=0.5,RB1=RBP)
  
  fCS=Assetcombine4(Com=C_CS,Eq=E_CS,EM=EM_CS,IR=IR_CS,Ret=Ret,CWGT=0.85,EMWGT=0.67,IRWGT=0.2,RB1=RBP)
  fTS=Assetcombine4(Com=C_TS,Eq=E_TS,EM=EM_TS,IR=IR_TS,Ret=Ret,CWGT=0.85,EMWGT=0.67,IRWGT=0.2,RB1=RBP)
  CS=fCS[[1]]
  TS=fTS[[1]]
  
  
  
  # #lag T markets
  # Ret=UNIRET
  # Ret[2:(nrow(Ret)),colnames(Ret)!=c("NKY","AS51","HSI","CN","KR","TW")]=  UNIRET[1:(nrow(UNIRET)-1),colnames(Ret)!=c("NKY","AS51","HSI","CN","KR","TW")]
  # 
  # CSori=CS
  # TSori=TS
  # CS[2:(nrow(CS)),colnames(CS)!=c("NKY","AS51","HSI","CN","KR","TW")]=  CSori[1:(nrow(CSori)-1),colnames(CSori)!=c("NKY","AS51","HSI","CN","KR","TW")]
  # TS[2:(nrow(TS)),colnames(TS)!=c("NKY","AS51","HSI","CN","KR","TW")]=  TSori[1:(nrow(TSori)-1),colnames(TSori)!=c("NKY","AS51","HSI","CN","KR","TW")]
  
  
  ####################
  ##match TS with CS
  #####################
  
  TSlong=CS*0
  TSlong[rownames(TS),]=TS
  TS=TSlong
  assetfactors1=cbind(RAW(CS,Ret),RAW(TS,Ret))
  write.csv(assetfactors1,"af1.csv")    
  # for (sim in 1:6){
  
  RB1=RBP
  
  
  # if(sim==1){AGG=TS}
  # if(sim==2){AGG=CS}
  # if(sim==3){AGG=0.5*TS+CS}
  # if(sim==4){AGG=TS+CS}
  # if(sim==5){
  #   Ret=UNIRET
  #   CS=Assetcombine(Com=C_CS,Eq=E_CS,EM=EM_CS,Ret=Ret,CWGT=1,EMWGT=0.5,RB1=RBP)
  #   TS=Assetcombine(Com=C_TS,Eq=E_TS,EM=EM_TS,Ret=Ret,CWGT=1,EMWGT=0.5,RB1=RBP)
  #   TSlong=CS*0
  #   TSlong[rownames(TS),]=TS
  #   TS=TSlong
  #   
  #   AGG=TS+CS}
  # 
  # if(sim==6){Ret=UNIRET
  # CS=Assetcombine(Com=C_CS,Eq=E_CS,EM=EM_CS,Ret=Ret,CWGT=2,EMWGT=1,RB1=RBP)
  # TS=Assetcombine(Com=C_TS,Eq=E_TS,EM=EM_TS,Ret=Ret,CWGT=2,EMWGT=1,RB1=RBP)
  # TSlong=CS*0
  # TSlong[rownames(TS),]=TS
  # TS=TSlong
  # 
  # AGG=TS+CS}
  
  
  
  TSSTAT=c(0.33,0.4,0.75)[TSnum]
  # TS[,15:28]=TS[,15:28]*ComTSmod
  # CS[,15:28]=CS[,15:28]*ComCSmod
  # CS[,39:42]=CS[,39:42]*IRCSmod
  # CS[,1:14]=CS[,1:14]*DMCSmod
  # CS[,29:38]=CS[,29:38]*EMCSmod
  # TS[,39:42]=TS[,39:42]*IRTSmod
  # TS[,1:14]=TS[,1:14]*DMTSmod
  # TS[,29:38]=TS[,29:38]*EMTSmod
  AGG=(TSSTAT)*TS+CS
  
  
  
  
  AGG=AGG[rownames(AGG)>"1995-12-30",]
  Ret=UNIRET
  
  ###calculate compounding effects
  fx<- as.data.frame(read.csv("D:/R/GRP/fx.csv", row.names=1,header=TRUE))
  
  portrisk=as.data.frame(AGG[,1])*0
  rownames(portrisk)=rownames(AGG)
  portrisk[1,1]=findwgtewma(Ret,rownames(portrisk)[1],AGG[1,])
  
  
  for(i in 2:(nrow(portrisk))) {
    if((wday(rownames(portrisk)[i])==RB1|rownames(portrisk)[i]%in%rownames(fx))){
      portrisk[i,1]=findwgtewma(Ret,rownames(portrisk)[i],AGG[i,])
    } else{portrisk[i,1]=portrisk[i-1,1]}
  }#i
  
  
  bufriskP=portrisk*0
  bufriskP[1,1]=portrisk[1,1]
  for(k in 2:nrow(portrisk)){
    if(abs(portrisk[k,1]-bufriskP[k-1,1])>Volband*targetvol|rownames(portrisk)[k]%in%rownames(fx)){
      #if(abs(portrisk[k,1]-bufriskP[k-1,1])>Volband*targetvol){
      bufriskP[k,1]=portrisk[k,1]  
    } else{bufriskP[k,1]=bufriskP[k-1,1]}
    
  }
  
  kkkk=targetvol/bufriskP[,1]
  vollev1=as.data.frame(matrix(rep(kkkk,ncol(AGG)),nrow=length(kkkk)))
  rownames(vollev1)=rownames(portrisk)
  
  
  
  
  
  VCAGG=AGG[rownames(AGG)%in%rownames(vollev1),]*vollev1
  
  
  ###compounding effects
  
  # compRet=Ret[rownames(VCAGG),]+1
  # for (comp in 2:nrow(VCAGG)){
  #   if(VCAGG[comp,]==VCAGG[comp-1,]){VCAGG[comp,]=VCAGG[comp-1,]*compRet[comp,]}
  # }
  
  ####leverage constraints
  
  eportlev=rowSums(abs(VCAGG[,1:ncol(eindex)]))
  eportlev1=eportlev[2:length(eportlev)]
  cportlev=rowSums(abs(VCAGG[,comstart:comend]))
  cportlev1=cportlev[2:length(cportlev)]
  emportlev=rowSums(abs(VCAGG[,emstart:emend]))
  emportlev1=emportlev[2:length(emportlev)]
  irportlev=rowSums(abs(VCAGG[,IRstart:ncol(Ret)]))
  irportlev1=irportlev[2:length(irportlev)]
  TO=abs(VCAGG[2:(nrow(VCAGG)),]-VCAGG[1:(nrow(VCAGG)-1),])
  portTO=rowSums(TO)
  # portETO=rowSums(TO[,1:ncol(eindex)])
  # portCTO=rowSums(TO[,comstart:comend])
  # portEMTO=rowSums(TO[,emstart:emend])
  # portIRTO=rowSums(TO[,IRstart:allcount])
  
  
  portETO=rowSums(VCAGG[2:nrow(VCAGG),1:ncol(eindex)])
  portCTO=rowSums(VCAGG[2:nrow(VCAGG),comstart:comend])
  portEMTO=rowSums(VCAGG[2:nrow(VCAGG),emstart:emend])
  portIRTO=rowSums(VCAGG[2:nrow(VCAGG),IRstart:allcount])
  
  
  Ret=Ret[rownames(Ret)%in%rownames(VCAGG),]
  port=Ret[2:nrow(Ret),]*VCAGG[1:(nrow(VCAGG)-1),]
  port1=rowSums(port)
  portlag=rbind(rep(0,ncol(VCAGG)),Ret[3:nrow(Ret),]*VCAGG[1:(nrow(VCAGG)-2),])
  portlag1=rowSums(portlag)
  
  edeleverage=elimit/(eportlev+cportlev+emportlev+irportlev)
  edeleverage[edeleverage>1]=1
  # cdeleverage=elimit/eportlev
  # cdeleverage[cdeleverage>1]=1
  # emdeleverage=elimit/eportlev
  # emdeleverage[emdeleverage>1]=1
  
  deleverage=cbind(matrix(rep(edeleverage,ncol(eindex)),nrow=length(edeleverage)),matrix(rep(edeleverage,ncol(cindex)),nrow=length(edeleverage)),
                   matrix(rep(edeleverage,ncol(emindex)),nrow=length(edeleverage)),matrix(rep(edeleverage,ncol(Bindex)),nrow=length(edeleverage)))
  
  
  
  
  
  ##### apply TSCAP
  TOTnet=rowSums(VCAGG[,c(1:14,29:38)]*deleverage,na.rm=TRUE)
  TSS=-0.16
  TSL=0.16
  TSS=-0.16
  TSL=0.16
  # TSS=-4
  # TSL=4
  TS=TS[rownames(AGG),]
  TS1=TS[,c(1:14,29:38)]
  TSori=TS1[rownames(AGG),]
  TSneg=TS1*(TS1<0)
  TSpos=TS1*(TS1>0)
  TSnet=rowSums(TS1*TSSTAT*kkkk,na.rm=TRUE)*deleverage[,1]
  TSnetneg=rowSums(TSneg*TSSTAT*kkkk,na.rm=TRUE)*deleverage[,1]
  TSnetpos=rowSums(TSpos*TSSTAT*kkkk,na.rm=TRUE)*deleverage[,1]
  TSadjust=0*TSnet+1
  TSadjustneg=0*TSnet+1
  TSadjustpos=0*TSnet+1
  
  # for(jj in 1:length(TSnet)){
  #   if(abs(TOTnet[jj])>0.2){TSadjust[jj]=max(abs(TSnet[jj])/(TSL),1)}
  #   
  # }
  
  
  CSS=-0.04
  CSL=0.04
  # CSS=-4
  # CSL=4
  CSori=CS[rownames(AGG),]
  CS=CS[rownames(AGG),1:38]
  CSnet=rowSums(CS*kkkk*deleverage[,1],na.rm=TRUE)
  
  CSadjust=0*CSnet+1
  
  
  for(jj in 1:length(TSnet)){
    if(abs(TOTnet[jj])<=0.2){TSadjust[jj]=1
    CSadjust[jj]=1} else  {TSadjust[jj]=abs(TOTnet[jj])/0.2}
  }
  
  
  TSadj=TS[,]*0+1 
  
  TSadj[,1:14]=TSadjust*1.4
  
  TSadj[,29:38]=TSadjust*1.4
  #TScap=cbind(TS/TSadj,TSori[,39:42])
  TScap=TS/TSadj*1
  # TScap[,15:28]=TScap[,15:28]*ComTSmod
  write.csv(cbind(TSnet,TSadjust,TSnetpos,TSnetneg,TSadjustpos,TSadjustneg,rowSums(TScap[,c(1:14,29:38)]*TSSTAT*kkkk,na.rm=TRUE)*deleverage[,1],CSnet,CSadjust),"solver.csv")
  
  print(tail(cbind(TSnet,TSadjust,TSnetpos,TSnetneg,TSadjustpos,TSadjustneg,rowSums(TScap[,c(1:14,29:38)]*TSSTAT*kkkk,na.rm=TRUE)*deleverage[,1],CSnet,CSadjust)))
  
  
  
  
  # Comvoladjust=CS*0+1
  # Comvoladjust[,colnames(cindex)]=CSadjust
  # CScap=cbind(CS,CSori[,39:42])
  # CScap[,15:28]=CScap[,15:28]*ComCSmod
  # CScap[,39:42]=CScap[,39:42]*IRCSmod
  # CScap[,1:14]=CScap[,1:14]*EMCSmod
  # CScap[,29:38]=CScap[,29:38]*DMCSmod
  AGG=(TSSTAT)*TScap+CSori[rownames(AGG),]#cap
  VCAGG=AGG[rownames(AGG)%in%rownames(vollev1),]*vollev1
  constraintAGG=VCAGG*deleverage
  
  
  
  
  constraintAGG=VCAGG*deleverage
  
  constraintAGG[,15:28]=constraintAGG[,15:28]*0.8
  constraintAGG[,39:42]=constraintAGG[39:42]*0.8
  
  # constraintAGG[,39:42]=constraintAGG[,39:42]*IRCSmod
  constraintAGGMax=constraintAGG
  constraintAGGMin=constraintAGG
  
  constraintAGGMax[,1:14][constraintAGGMax[,1:14]>0.09]=0.09
  constraintAGGMin[,1:14][constraintAGGMin[,1:14]<(-0.09)]=-0.09
  constraintAGGMax[,29:38][constraintAGGMax[,29:38]>0.065]=0.065
  constraintAGGMin[,29:38][constraintAGGMin[,29:38]<(-0.065)]=-0.065
  constraintAGGMax[,39:42][constraintAGGMax[,39:42]>0.13]=0.13
  constraintAGGMin[,39:42][constraintAGGMin[,39:42]<(-0.13)]=-0.13
  constraintAGGMax[,15:28][constraintAGGMax[,15:28]>0.04]=0.04
  constraintAGGMin[,15:28][constraintAGGMin[,15:28]<(-0.04)]=-0.04
  
  # constraintAGG2=constraintAGG
  # 
  # constraintAGG2[,1:14][constraintAGG2[,1:14]>0.1]=0.1
  # constraintAGG2[,1:14][constraintAGG2[,1:14]<(-0.1)]=-0.1
  # constraintAGG2[,29:38][constraintAGG2[,29:38]>0.075]=0.075
  # constraintAGG2[,29:38][constraintAGG2[,29:38]<(-0.075)]=-0.075
  # constraintAGG2[,15:28][constraintAGG2[,15:28]>0.05]=0.05
  # constraintAGG2[,15:28][constraintAGG2[,15:28]<(-0.05)]=-0.05
  # constraintAGG2[,39:42][constraintAGG2[,39:42]>0.15]=0.15
  # constraintAGG2[,39:42][constraintAGG2[,39:42]<(-0.15)]=-0.15
  # 
  # ADDDM=(rowSums(constraintAGG[,1:14]-constraintAGGMax[,1:14])+rowSums(constraintAGG[,1:14]-constraintAGGMin[,1:14]))/14
  # constraintAGG2[,1:14]=constraintAGG2[,1:14]+ADDDM
  # 
  # ADDEM=(rowSums(constraintAGG[,29:38]-constraintAGGMax[,29:38])+rowSums(constraintAGG[,29:38]-constraintAGGMin[,29:38]))/9
  # constraintAGG2[,29:38]=constraintAGG2[,29:38]+ADDEM
  # constraintAGG2[,34]=constraintAGG2[,34]-ADDEM
  # 
  # ADDBD=(rowSums(constraintAGG[,39:42]-constraintAGGMax[,39:42])+rowSums(constraintAGG[,39:42]-constraintAGGMin[,39:42]))/4
  # constraintAGG2[,39:42]=constraintAGG2[,39:42]+ADDBD
  # 
  # ADDCO=(rowSums(constraintAGG[,15:28]-constraintAGGMax[,15:28])+rowSums(constraintAGG[,15:28]-constraintAGGMin[,15:28]))/14
  # constraintAGG2[,15:28]=constraintAGG2[,15:28]+ADDCO
  # 
  # constraintAGG=constraintAGG2
  
  
  
  
  ADDDM=(rowSums(constraintAGG[,1:14]-constraintAGGMax[,1:14])+rowSums(constraintAGG[,1:14]-constraintAGGMin[,1:14]))/14
  constraintAGG[,1:14][constraintAGG[,1:14]>0.09]=0.09
  constraintAGG[,1:14][constraintAGG[,1:14]<(-0.09)]=-0.09
  constraintAGG[,1:14]=constraintAGG[,1:14]+ADDDM
  
  ADDEM=(rowSums(constraintAGG[,29:38]-constraintAGGMax[,29:38])+rowSums(constraintAGG[,29:38]-constraintAGGMin[,29:38]))/9
  constraintAGG[,29:38][constraintAGG[,29:38]>0.065]=0.065
  constraintAGG[,29:38][constraintAGG[,29:38]<(-0.065)]=-0.065
  constraintAGG[,29:38]=constraintAGG[,29:38]+ADDEM
  constraintAGG[,34]=constraintAGG[,34]-ADDEM
  
  ADDBD=(rowSums(constraintAGG[,39:42]-constraintAGGMax[,39:42])+rowSums(constraintAGG[,39:42]-constraintAGGMin[,39:42]))/4
  constraintAGG[,39:42][constraintAGG[,39:42]>0.13]=0.13
  constraintAGG[,39:42][constraintAGG[,39:42]<(-0.13)]=-0.13
  constraintAGG[,39:42]=constraintAGG[,39:42]+ADDBD
  
  ADDCO=(rowSums(constraintAGG[,15:28]-constraintAGGMax[,15:28])+rowSums(constraintAGG[,15:28]-constraintAGGMin[,15:28]))/14
  constraintAGG[,15:28][constraintAGG[,15:28]>0.04]=0.04
  constraintAGG[,15:28][constraintAGG[,15:28]<(-0.04)]=-0.04
  constraintAGG[,15:28]=constraintAGG[,15:28]+ADDCO
  
  
  ##round2
  
  constraintAGGMax=constraintAGG
  constraintAGGMin=constraintAGG
  
  constraintAGGMax[,1:14][constraintAGGMax[,1:14]>0.1]=0.1
  constraintAGGMin[,1:14][constraintAGGMin[,1:14]<(-0.1)]=-0.1
  constraintAGGMax[,29:38][constraintAGGMax[,29:38]>0.075]=0.075
  constraintAGGMin[,29:38][constraintAGGMin[,29:38]<(-0.075)]=-0.075
  constraintAGGMax[,39:42][constraintAGGMax[,39:42]>0.15]=0.15
  constraintAGGMin[,39:42][constraintAGGMin[,39:42]<(-0.15)]=-0.15
  constraintAGGMax[,15:28][constraintAGGMax[,15:28]>0.05]=0.05
  constraintAGGMin[,15:28][constraintAGGMin[,15:28]<(-0.05)]=-0.05
  
  
  ADDDM=(rowSums(constraintAGG[,1:14]-constraintAGGMax[,1:14])+rowSums(constraintAGG[,1:14]-constraintAGGMin[,1:14]))/12
  constraintAGG[,1:14][constraintAGG[,1:14]>0.1]=0.1
  constraintAGG[,1:14][constraintAGG[,1:14]<(-0.1)]=-0.1
  constraintAGG[,1:14]=constraintAGG[,1:14]+ADDDM
  
  ADDEM=(rowSums(constraintAGG[,29:38]-constraintAGGMax[,29:38])+rowSums(constraintAGG[,29:38]-constraintAGGMin[,29:38]))/8
  constraintAGG[,29:38][constraintAGG[,29:38]>0.075]=0.075
  constraintAGG[,29:38][constraintAGG[,29:38]<(-0.075)]=-0.075
  constraintAGG[,29:38]=constraintAGG[,29:38]+ADDEM
  constraintAGG[,34]=constraintAGG[,34]-ADDEM
  
  ADDBD=(rowSums(constraintAGG[,39:42]-constraintAGGMax[,39:42])+rowSums(constraintAGG[,39:42]-constraintAGGMin[,39:42]))/3
  constraintAGG[,39:42][constraintAGG[,39:42]>0.15]=0.15
  constraintAGG[,39:42][constraintAGG[,39:42]<(-0.15)]=-0.15
  constraintAGG[,39:42]=constraintAGG[,39:42]+ADDBD
  
  ADDCO=(rowSums(constraintAGG[,15:28]-constraintAGGMax[,15:28])+rowSums(constraintAGG[,15:28]-constraintAGGMin[,15:28]))/13
  constraintAGG[,15:28][constraintAGG[,15:28]>0.05]=0.05
  constraintAGG[,15:28][constraintAGG[,15:28]<(-0.05)]=-0.05
  constraintAGG[,15:28]=constraintAGG[,15:28]+ADDCO
  
  
  
  constraintAGG[,1:14][constraintAGG[,1:14]>0.1]=0.1
  constraintAGG[,1:14][constraintAGG[,1:14]<(-0.1)]=-0.1
  constraintAGG[,29:38][constraintAGG[,29:38]>0.075]=0.075
  constraintAGG[,29:38][constraintAGG[,29:38]<(-0.075)]=-0.075
  constraintAGG[,15:28][constraintAGG[,15:28]>0.05]=0.05
  constraintAGG[,15:28][constraintAGG[,15:28]<(-0.05)]=-0.05
  constraintAGG[,39:42][constraintAGG[,39:42]>0.15]=0.15
  constraintAGG[,39:42][constraintAGG[,39:42]<(-0.15)]=-0.15
  
  
  
  if(positions==1){
    setwd("D:/R/GRP/live")
    POS=read.csv("D:/R/GRP/live/MASTERPOS.csv", row.names=1)
    constraintAGG[rownames(POS),]=POS
    
    if (updatepos==1){
      write.csv(constraintAGG,"MASTERPOS.CSV")
    }
    setwd("D:/R/GRP")
  }
  
  
  econstraintlev=rowSums(abs(constraintAGG[,1:ncol(eindex)]))
  econstraintlev1=econstraintlev[2:length(econstraintlev)]
  cconstraintlev=rowSums(abs(constraintAGG[,(1+ncol(eindex)):(ncol(eindex)+ncol(cindex))]))
  cconstraintlev1=cconstraintlev[2:length(cconstraintlev)]
  emconstraintlev=rowSums(abs(constraintAGG[,emstart:emend]))
  emconstraintlev1=emconstraintlev[2:length(emconstraintlev)]
  irconstraintlev=rowSums(abs(constraintAGG[,IRstart:allcount]))
  irconstraintlev1=irconstraintlev[2:length(irconstraintlev)]
  econnet=rowSums((constraintAGG[,1:ncol(eindex)]))
  econnet1=econnet[2:length(econnet)]
  cconnet=rowSums((constraintAGG[,(1+ncol(eindex)):(ncol(eindex)+ncol(cindex))]))
  cconnet1=cconnet[2:length(cconnet)]
  emconnet=rowSums((constraintAGG[,emstart:emend]))
  emconnet1=emconnet[2:length(emconnet)]
  irconnet=rowSums((constraintAGG[,IRstart:allcount]))
  irconnet1=irconnet[2:length(irconnet)]
  
  constraintTO=abs(constraintAGG[2:(nrow(constraintAGG)),]-constraintAGG[1:(nrow(constraintAGG)-1),])
  constraintportTO=rowSums(constraintTO)
  constraintportETO=rowSums(constraintTO[,1:ncol(eindex)])
  constraintportCTO=rowSums(constraintTO[comstart:comend])
  constraintportEMTO=rowSums(constraintTO[emstart:emend])
  constraintportIRTO=rowSums(constraintTO[IRstart:allcount])
  
  constraintport=Ret[2:nrow(Ret),]*constraintAGG[1:(nrow(constraintAGG)-1),]
  constraintport1=rowSums(constraintport)
  constraintportlag=rbind(rep(0,ncol(constraintAGG)),Ret[3:nrow(Ret),]*constraintAGG[1:(nrow(constraintAGG)-2),])
  # constraintportlag5=rowSums(Ret[9:nrow(Ret),]*constraintAGG[1:(nrow(constraintAGG)-8),])
  # write.csv(constraintportlag5,"lag8.csv")
  constraintportlag1=rowSums(constraintportlag)
  equityret=rowSums(constraintport[,1:ncol(eindex)])
  commoret=rowSums(constraintport[,comstart:comend])
  emret=rowSums(constraintport[,emstart:emend])
  irret=rowSums(constraintport[,IRstart:allcount]) 
  contribut=cbind(equityret,commoret,emret,irret)
  
  
  output=cbind(port1,portlag1,portETO,portCTO,portEMTO,portIRTO,eportlev1,cportlev1,emportlev1,irportlev1,
               constraintport1,constraintportlag1,constraintportETO,constraintportCTO,constraintportEMTO,constraintportIRTO,
               econstraintlev1,cconstraintlev1,emconstraintlev1,irconstraintlev1,econnet1,cconnet1,emconnet1,irconnet1,contribut)
  setwd("D:/R/GRP")
  
  write.csv(constraintAGG[rownames(constraintAGG)>"2012-12-31",],"positions.csv")
  write.csv(constraintAGG,"positionsfull.csv")
  write.csv(output,paste("444portfxx",TSnum,".csv",sep=""))
  
  setwd("D:/R/GRP/backupsignals")
  write.csv(output,paste("file-",today(),".csv",sep=""))
  write.csv(constraintAGG,paste("port-",today(),".csv",sep=""))
  setwd("D:/R/GRP")
}



covar1=function(data,date){
  
  sdobs=130
  
  ends=match(date,rownames(data))
  begin=ends-sdobs+1
  
  covar=matrix(NA,ncol(data),ncol(data))
  corr=matrix(NA,ncol(data),ncol(data))
  var=array()
  rownames(covar)=colnames(data)
  colnames(covar)=colnames(data)
  for (i in 1:ncol(data)){
    var[i]=sqrt(var(data[begin:ends,i],data[begin:ends,i],na.rm=TRUE))
  }
  
  for (i in 1:ncol(data)){
    for (j in 1:ncol(data)){
      corr[i,j]=cor(data[begin:ends,i],data[begin:ends,j],"pairwise.complete.obs")
    }
  }
  corrdiag=diag(1,ncol(data),ncol(data))
  
  corr=1*corr+0*corrdiag
  
  
  covar=(var%o%var)*corr
  
  
  covar[is.na(covar[,])]=0
  
  return(covar)
}

corr1=function(data,date){
  
  sdobs=130
  
  ends=match(date,rownames(data))
  begin=ends-sdobs+1
  
  covar=matrix(NA,ncol(data),ncol(data))
  corr=matrix(NA,ncol(data),ncol(data))
  var=array()
  rownames(covar)=colnames(data)
  colnames(covar)=colnames(data)
  for (i in 1:ncol(data)){
    var[i]=sqrt(var(data[begin:ends,i],data[begin:ends,i],na.rm=TRUE))
  }
  
  
  for (i in 1:ncol(data)){
    for (j in 1:ncol(data)){
      corr[i,j]=cor(data[begin:ends,i],data[begin:ends,j],"pairwise.complete.obs")
    }
  }
  corrdiag=diag(1,ncol(data),ncol(data))
  
  corr=1*corr+0*corrdiag
  
  
  
  return(corr)
}

MAT1=covar1(Ret,rownames(constraintAGG)[nrow(constraintAGG)])
colnames(MAT1)=colnames(Ret)
MAT2=covar1(Ret,rownames(constraintAGG)[(nrow(constraintAGG)-1)])
colnames(MAT2)=colnames(Ret)
MAT3=corr1(Ret,rownames(constraintAGG)[nrow(constraintAGG)])
colnames(MAT3)=colnames(Ret)
write.csv(MAT1,"covarianceT.csv")
write.csv(MAT2,"covarianceT-1.csv")
write.csv(MAT3,"correlT.csv")
CSsig=list()
CSsig[[1]]=stat1[[4]]
CSsig[[2]]=stat2[[4]]
CSsig[[3]]=stat3[[4]]
CSsig[[4]]=stat4[[4]]
CSsig[[5]]=stat5[[4]]
CSsig[[6]]=stat6[[4]]
CSsig[[7]]=stat7[[4]]
CSsig[[8]]=stat8[[4]]
CSsig[[9]]=stat9[[4]]
CSsig[[10]]=stat10[[4]]
CSsig[[11]]=stat11[[4]]
CSsig[[12]]=stat12[[4]]
CSsig[[13]]=stat13[[4]]
CSsig[[14]]=stat14[[4]]
CSsig[[15]]=stat15[[4]]
CSsig[[16]]=stat16[[4]]
CSsig[[17]]=stat17[[4]]
CSsig[[18]]=stat18[[4]]
CSsig[[19]]=stat19[[4]]
CSsig[[20]]=stat20[[4]]
CSsig[[21]]=stat21[[4]]
CSsig[[22]]=stat22[[4]]
CSsig[[23]]=stat23[[4]]
CSsig[[24]]=stat24[[4]]
CSsig[[25]]=IRS1[[4]]
CSsig[[26]]=IRS2[[4]]
CSsig[[27]]=IRS3[[4]]
CSsig[[28]]=IRS4[[4]]
CSsig[[29]]=IRS5[[4]]

TSsig=list()
TSsig[[1]]=stat1[[3]]
TSsig[[2]]=stat2[[3]]
TSsig[[3]]=stat3[[3]]
TSsig[[4]]=stat4[[3]]
TSsig[[5]]=stat5[[3]]
TSsig[[6]]=stat6[[3]]
TSsig[[7]]=stat7[[3]]
TSsig[[8]]=stat8[[3]]
TSsig[[9]]=stat9[[3]]
TSsig[[10]]=stat10[[3]]
TSsig[[11]]=stat11[[3]]
TSsig[[12]]=stat12[[3]]
TSsig[[13]]=stat13[[3]]
TSsig[[14]]=stat14[[3]]
TSsig[[15]]=stat15[[3]]
TSsig[[16]]=stat16[[3]]
TSsig[[17]]=stat17[[3]]
TSsig[[18]]=stat18[[3]]
TSsig[[19]]=stat19[[3]]
TSsig[[20]]=stat20[[3]]
TSsig[[21]]=stat21[[3]]
TSsig[[22]]=stat22[[3]]
TSsig[[23]]=stat23[[3]]
TSsig[[24]]=stat24[[3]]
TSsig[[25]]=IRS1[[3]]
TSsig[[26]]=IRS2[[3]]
TSsig[[27]]=IRS3[[3]]
TSsig[[28]]=IRS4[[3]]
TSsig[[29]]=IRS5[[3]]

rundate=rownames(output)[nrow(output)]
rundate1=rownames(output)[nrow(output)-1]

Tposition=as.data.frame(matrix(0,nstat*2,allcount))
colnames(Tposition)=colnames(Ret)
T1position=Tposition
for(sss in 1:nstat){
  Tposition[(2*sss-1),colnames(CSsig[[sss]])]=CSsig[[sss]][rundate,]
  Tposition[(2*sss),colnames(CSsig[[sss]])]=TSsig[[sss]][rundate,]
  
}
for(sss in 1:nstat){
  T1position[(2*sss-1),colnames(CSsig[[sss]])]=CSsig[[sss]][rundate1,]
  T1position[(2*sss),colnames(CSsig[[sss]])]=TSsig[[sss]][rundate1,]
  
}
change=Tposition-T1position
change[is.na(change)]=0
write.csv(cbind(change,Tposition),"sigchange.csv")

Tposition=as.data.frame(matrix(0,nstat*2,allcount))
colnames(Tposition)=colnames(Ret)
T1position=Tposition
for(sss in 1:nstat){
  Tposition[(2*sss-1),colnames(CSsig[[sss]])]=dumpCS[[sss]][rundate,]
  Tposition[(2*sss),colnames(CSsig[[sss]])]=dumpTS[[sss]][rundate,]
  
}
for(sss in 1:nstat){
  T1position[(2*sss-1),colnames(CSsig[[sss]])]=dumpCS[[sss]][rundate1,]
  T1position[(2*sss),colnames(CSsig[[sss]])]=dumpTS[[sss]][rundate1,]
  
}
change=Tposition-T1position
change[is.na(change)]=0
write.csv(cbind(change,Tposition),"weightchange.csv")

Tfactor=as.data.frame(matrix(0,14,allcount))
colnames(Tfactor)=colnames(Ret)
#carry cs
Leverage=as.data.frame(constraintAGG[,])*0
rownames(Leverage)=rownames(constraintAGG)
CScomL=Leverage
CScomL[,colnames(cindex)]=as.data.frame(fCS[[2]][rownames(Leverage),])

TScomL=Leverage
TScomL[,colnames(cindex)]=as.data.frame(fTS[[2]][rownames(Leverage),])
CSdmL=Leverage
CSdmL[,colnames(eindex)]=as.data.frame(fCS[[3]][rownames(Leverage),])
TSdmL=Leverage
TSdmL[,colnames(eindex)]=as.data.frame(fTS[[3]][rownames(Leverage),])
CSemL=Leverage
CSemL[,colnames(emindex)]=as.data.frame(fCS[[4]][rownames(Leverage),])
TSemL=Leverage
TSemL[,colnames(emindex)]=as.data.frame(fTS[[4]][rownames(Leverage),])
CSirL=Leverage
CSirL[,colnames(Bindex)]=as.data.frame(fCS[[5]][rownames(Leverage),])
TSirL=Leverage
TSirL[,colnames(Bindex)]=as.data.frame(fTS[[5]][rownames(Leverage),])
CSL=Leverage
CSL[,1:ncol(Leverage)]=as.data.frame(fCS[[6]][rownames(Leverage),])
TSL=Leverage
TSL[,1:ncol(Leverage)]=as.data.frame(fTS[[6]][rownames(Leverage),])

CSEvalueL=Leverage
CSEvalueL[,colnames(eindex)]=as.data.frame(fE_Value_CS[[2]][rownames(Leverage),])
CSEmafL=Leverage
CSEmafL[,colnames(eindex)]=as.data.frame(fE_Maf_CS[[2]][rownames(Leverage),])
CSEriskL=Leverage
CSEriskL[,colnames(eindex)]=as.data.frame(fE_Risk_CS[[2]][rownames(Leverage),])
CSEmifL=Leverage
CSEmifL[,colnames(eindex)]=as.data.frame(fE_Mif_CS[[2]][rownames(Leverage),])
CSEMvalueL=Leverage
CSEMvalueL[,colnames(emindex)]=as.data.frame(fEM_Value_CS[[2]][rownames(Leverage),])

TSEvalueL=Leverage
TSEvalueL[,colnames(eindex)]=as.data.frame(fE_Value_TS[[2]][rownames(Leverage),])
TSEmafL=Leverage
TSEmafL[,colnames(eindex)]=as.data.frame(fE_Maf_TS[[2]][rownames(Leverage),])
TSEriskL=Leverage
TSEriskL[,colnames(eindex)]=as.data.frame(fE_Risk_TS[[2]][rownames(Leverage),])
TSEmifL=Leverage
TSEmifL[,colnames(eindex)]=as.data.frame(fE_Mif_TS[[2]][rownames(Leverage),])
TSEMvalueL=Leverage
TSEMvalueL[,colnames(emindex)]=as.data.frame(fEM_Value_TS[[2]][rownames(Leverage),])

TSadjust=as.data.frame(TSadjust)
TSadjustL=Leverage
TSadjustL[,colnames(Leverage)]=1/as.data.frame(TSadj[rownames(Leverage),])

CSadjust=as.data.frame(CSadjust)
CSadjustL=Leverage
CSadjustL[,colnames(Leverage)]=1/as.data.frame(CSadjust[rownames(Leverage),])



portL=vollev1[rownames(constraintAGG),]



#TSstats


C_Carry_TS_C=C_Carry_TS[rownames(Leverage),]*(TScomL*0.75*TSL*portL*deleverage*TSadjustL)[,colnames(cindex)]*0.5*0.85*ComTSmod
C_Mom_TS_C=C_Mom_TS[rownames(Leverage),]*(TScomL*1*TSL*portL*deleverage*TSadjustL)[,colnames(cindex)]*0.5*0.85*ComTSmod
C_Seas_TS_C=C_Seas_TS[rownames(Leverage),]*(TScomL*0.75*TSL*portL*deleverage*TSadjustL)[,colnames(cindex)]*0.5*0.85*ComTSmod
C_Value_TS_C=C_Value_TS[rownames(Leverage),]*(TScomL*0.75*TSL*portL*deleverage*TSadjustL)[,colnames(cindex)]*0.5*0.85*ComTSmod
C_Risk_TS_C=C_Risk_TS[rownames(Leverage),]*(TScomL*1*TSL*portL*deleverage*TSadjustL)[,colnames(cindex)]*0.5*0.85*ComTSmod
E_Carry_TS_C=E_Carry_TS[rownames(Leverage),]*(TSdmL*1*TSL*portL*deleverage*TSadjustL)[,colnames(eindex)]*0.5*DMTSmod
E_Seas_TS_C=E_Seas_TS[rownames(Leverage),]*(TSdmL*0.5*TSL*portL*deleverage*TSadjustL)[,colnames(eindex)]*0.5*DMTSmod
E_Mom_TS_C=E_Mom_TS[rownames(Leverage),]*(TSdmL*1*TSL*portL*deleverage*TSadjustL)[,colnames(eindex)]*0.5*DMTSmod
EM_Mom_TS_C=EM_Mom_TS[rownames(Leverage),]*(TSemL*1*TSL*portL*deleverage*TSadjustL)[,colnames(emindex)]*0.5*0.67*EMTSmod
EM_Carry_TS_C=EM_Carry_TS[rownames(Leverage),]*(TSemL*1*TSL*portL*deleverage*TSadjustL)[,colnames(emindex)]*0.5*0.67*EMTSmod
EM_Seas_TS_C=EM_Seas_TS[rownames(Leverage),]*(TSemL*0.5*TSL*portL*deleverage*TSadjustL)[,colnames(emindex)]*0.5*0.67*EMTSmod
EM_Risk_TS_C=EM_Risk_TS[rownames(Leverage),]*(TSemL*0.75*TSL*portL*deleverage*TSadjustL)[,colnames(emindex)]*0.5*0.67*EMTSmod
E_Value_TS_C=E_Value_TS[rownames(Leverage),]*(TSdmL*1*TSL*portL*deleverage*TSadjustL)[,colnames(eindex)]*0.5*DMTSmod
E_Maf_TS_C=E_Maf_TS[rownames(Leverage),]*(TSdmL*1*TSL*portL*deleverage*TSadjustL)[,colnames(eindex)]*0.5*DMTSmod
E_Risk_TS_C=E_Risk_TS[rownames(Leverage),]*(TSdmL*0.75*TSL*portL*deleverage*TSadjustL)[,colnames(eindex)]*0.5*DMTSmod
E_Mif_TS_C=E_Mif_TS[rownames(Leverage),]*(TSdmL*1*TSL*portL*deleverage*TSadjustL)[,colnames(eindex)]*0.5*DMTSmod
EM_Value_TS_C=EM_Value_TS[rownames(Leverage),]*(TSemL*1*TSL*portL*deleverage*TSadjustL)[,colnames(emindex)]*0.5*0.67*EMTSmod

IR_Carry_TS_C=IR_Carry_TS[rownames(Leverage),]*(TSirL*1*TSL*portL*deleverage*TSadjustL)[,colnames(Bindex)]*0.5*0.2*IRTSmod
IR_Mom_TS_C=IR_Mom_TS[rownames(Leverage),]*(TSirL*1*TSL*portL*deleverage*TSadjustL)[,colnames(Bindex)]*0.5*0.2*IRTSmod
IR_Seas_TS_C=IR_Seas_TS[rownames(Leverage),]*(TSirL*0.5*TSL*portL*deleverage*TSadjustL)[,colnames(Bindex)]*0.5*0.2*IRTSmod
IR_Value_TS_C=IR_Value_TS[rownames(Leverage),]*(TSirL*1*TSL*portL*deleverage*TSadjustL)[,colnames(Bindex)]*0.5*0.2*IRTSmod
IR_Risk_TS_C=IR_Risk_TS[rownames(Leverage),]*(TSirL*1*TSL*portL*deleverage*TSadjustL)[,colnames(Bindex)]*0.5*0.2*IRTSmod

##CS strategies
C_Carry_CS_C=C_Carry_CS[rownames(Leverage),]*(CScomL*1*CSL*portL*deleverage*CSadjustL)[,colnames(cindex)]*1*0.85*ComCSmod
C_Mom_CS_C=C_Mom_CS[rownames(Leverage),]*(CScomL*1*CSL*portL*deleverage*CSadjustL)[,colnames(cindex)]*1*0.85*ComCSmod
C_Seas_CS_C=C_Seas_CS[rownames(Leverage),]*(CScomL*1*CSL*portL*deleverage*CSadjustL)[,colnames(cindex)]*1*0.85*ComCSmod
C_Value_CS_C=C_Value_CS[rownames(Leverage),]*(CScomL*1*CSL*portL*deleverage*CSadjustL)[,colnames(cindex)] *1*0.85*ComCSmod
C_Risk_CS_C=C_Risk_CS[rownames(Leverage),]*(CScomL*1*CSL*portL*deleverage*CSadjustL)[,colnames(cindex)]*1*0.85*ComCSmod
E_Carry_CS_C=E_Carry_CS[rownames(Leverage),]*(CSdmL*0.75*CSL*portL*deleverage)[,colnames(eindex)]*DMCSmod
E_Seas_CS_C=E_Seas_CS[rownames(Leverage),]*(CSdmL*0.25*CSL*portL*deleverage)[,colnames(eindex)]*DMCSmod
E_Mom_CS_C=E_Mom_CS[rownames(Leverage),]*(CSdmL*0.75*CSL*portL*deleverage)[,colnames(eindex)]*DMCSmod
EM_Mom_CS_C=EM_Mom_CS[rownames(Leverage),]*(CSemL*0.33*CSL*portL*deleverage)[,colnames(emindex)]*1*0.67*EMCSmod
EM_Carry_CS_C=EM_Carry_CS[rownames(Leverage),]*(CSemL*0.75*CSL*portL*deleverage)[,colnames(emindex)]*1*0.67*EMCSmod
EM_Seas_CS_C=EM_Seas_CS[rownames(Leverage),]*(CSemL*0.2*CSL*portL*deleverage)[,colnames(emindex)]*1*0.67*EMCSmod
EM_Risk_CS_C=EM_Risk_CS[rownames(Leverage),]*(CSemL*1*CSL*portL*deleverage)[,colnames(emindex)]*1*0.67*EMCSmod

E_Value_CS_C=E_Value_CS[rownames(Leverage),]*(CSdmL*1*CSL*portL*deleverage)[,colnames(eindex)]*DMCSmod
E_Maf_CS_C=E_Maf_CS[rownames(Leverage),]*(CSdmL*1*CSL*portL*deleverage)[,colnames(eindex)]*DMCSmod
E_Risk_CS_C=E_Risk_CS[rownames(Leverage),]*(CSdmL*1*CSL*portL*deleverage)[,colnames(eindex)]*DMCSmod
E_Mif_CS_C=E_Mif_CS[rownames(Leverage),]*(CSdmL*1*CSL*portL*deleverage)[,colnames(eindex)]*DMCSmod
EM_Value_CS_C=EM_Value_CS[rownames(Leverage),]*(CSemL*1*CSL*portL*deleverage)[,colnames(emindex)]*1*0.67*EMCSmod


IR_Carry_CS_C=IR_Carry_CS[rownames(Leverage),]*(CSirL*1*CSL*portL*deleverage)[,colnames(Bindex)]*0.2*IRCSmod
IR_Mom_CS_C=IR_Mom_CS[rownames(Leverage),]*(CSirL*1*CSL*portL*deleverage)[,colnames(Bindex)]*0.2*IRCSmod
IR_Seas_CS_C=IR_Seas_CS[rownames(Leverage),]*(CSirL*0.5*CSL*portL*deleverage)[,colnames(Bindex)]*0.2*IRCSmod
IR_Value_CS_C=IR_Value_CS[rownames(Leverage),]*(CSirL*1*CSL*portL*deleverage)[,colnames(Bindex)]*0.2*IRCSmod
IR_Risk_CS_C=IR_Risk_CS[rownames(Leverage),]*(CSirL*0.75*CSL*portL*deleverage)[,colnames(Bindex)]*0.2*IRCSmod*0.75


TRealwgt=as.data.frame(matrix(0,7*2,allcount))
colnames(TRealwgt)=colnames(Ret)
rownames(TRealwgt)=c("Carry_TS","Mom_TS","Seas_TS","Value_TS","Risk_TS","Maf_TS","Mif_TS","Carry_CS","Mom_CS","Seas_CS","Value_CS","Risk_CS","Maf_CS","Mif_CS")

T1Realwgt=TRealwgt

TRealwgt[1,]=cbind(E_Carry_TS_C[rundate,],C_Carry_TS_C[rundate,],EM_Carry_TS_C[rundate,],IR_Carry_TS_C[rundate,])
TRealwgt[2,]=cbind(E_Mom_TS_C[rundate,],C_Mom_TS_C[rundate,],EM_Mom_TS_C[rundate,],IR_Mom_TS_C[rundate,])
TRealwgt[3,]=cbind(E_Seas_TS_C[rundate,],C_Seas_TS_C[rundate,],EM_Seas_TS_C[rundate,],IR_Seas_TS_C[rundate,])
TRealwgt[4,]=cbind(E_Value_TS_C[rundate,],C_Value_TS_C[rundate,],EM_Value_TS_C[rundate,],IR_Value_TS_C[rundate,])
TRealwgt[5,]=cbind(E_Risk_TS_C[rundate,],C_Risk_TS_C[rundate,],EM_Risk_TS_C[rundate,],IR_Risk_TS_C[rundate,])
TRealwgt[6,colnames(eindex)]=E_Maf_TS_C[rundate,]
TRealwgt[7,colnames(eindex)]=E_Mif_TS_C[rundate,]
TRealwgt[8,]=cbind(E_Carry_CS_C[rundate,],C_Carry_CS_C[rundate,],EM_Carry_CS_C[rundate,],IR_Carry_CS_C[rundate,])
TRealwgt[9,]=cbind(E_Mom_CS_C[rundate,],C_Mom_CS_C[rundate,],EM_Mom_CS_C[rundate,],IR_Mom_CS_C[rundate,])
TRealwgt[10,]=cbind(E_Seas_CS_C[rundate,],C_Seas_CS_C[rundate,],EM_Seas_CS_C[rundate,],IR_Seas_CS_C[rundate,])
TRealwgt[11,]=cbind(E_Value_CS_C[rundate,],C_Value_CS_C[rundate,],EM_Value_CS_C[rundate,],IR_Value_CS_C[rundate,])
TRealwgt[12,]=cbind(E_Risk_CS_C[rundate,],C_Risk_CS_C[rundate,],EM_Risk_CS_C[rundate,],IR_Risk_CS_C[rundate,])
TRealwgt[13,colnames(eindex)]=E_Maf_CS_C[rundate,]
TRealwgt[14,colnames(eindex)]=E_Mif_CS_C[rundate,]

T1Realwgt[1,]=cbind(E_Carry_TS_C[rundate1,],C_Carry_TS_C[rundate1,],EM_Carry_TS_C[rundate1,],IR_Carry_TS_C[rundate1,])
T1Realwgt[2,]=cbind(E_Mom_TS_C[rundate1,],C_Mom_TS_C[rundate1,],EM_Mom_TS_C[rundate1,],IR_Mom_TS_C[rundate1,])
T1Realwgt[3,]=cbind(E_Seas_TS_C[rundate1,],C_Seas_TS_C[rundate1,],EM_Seas_TS_C[rundate1,],IR_Seas_TS_C[rundate1,])
T1Realwgt[4,]=cbind(E_Value_TS_C[rundate1,],C_Value_TS_C[rundate1,],EM_Value_TS_C[rundate1,],IR_Value_TS_C[rundate1,])
T1Realwgt[5,]=cbind(E_Risk_TS_C[rundate1,],C_Risk_TS_C[rundate1,],EM_Risk_TS_C[rundate1,],IR_Risk_TS_C[rundate1,])
T1Realwgt[6,colnames(eindex)]=E_Maf_TS_C[rundate1,]
T1Realwgt[7,colnames(eindex)]=E_Mif_TS_C[rundate1,]
T1Realwgt[8,]=cbind(E_Carry_CS_C[rundate1,],C_Carry_CS_C[rundate1,],EM_Carry_CS_C[rundate1,],IR_Carry_CS_C[rundate1,])
T1Realwgt[9,]=cbind(E_Mom_CS_C[rundate1,],C_Mom_CS_C[rundate1,],EM_Mom_CS_C[rundate1,],IR_Mom_CS_C[rundate1,])
T1Realwgt[10,]=cbind(E_Seas_CS_C[rundate1,],C_Seas_CS_C[rundate1,],EM_Seas_CS_C[rundate1,],IR_Seas_CS_C[rundate1,])
T1Realwgt[11,]=cbind(E_Value_CS_C[rundate1,],C_Value_CS_C[rundate1,],EM_Value_CS_C[rundate1,],IR_Value_CS_C[rundate1,])
T1Realwgt[12,]=cbind(E_Risk_CS_C[rundate1,],C_Risk_CS_C[rundate1,],EM_Risk_CS_C[rundate1,],IR_Risk_CS_C[rundate1,])
T1Realwgt[13,colnames(eindex)]=E_Maf_CS_C[rundate1,]
T1Realwgt[14,colnames(eindex)]=E_Mif_CS_C[rundate1,]


TCRealwgt=TRealwgt-T1Realwgt

write.csv(rbind(TRealwgt,TCRealwgt),"FactorRealWeight.csv")



positionbutc=C_Carry_TS_C+C_Mom_TS_C+C_Seas_TS_C+C_Value_TS_C+C_Risk_TS_C
positionbucc=C_Carry_CS_C+C_Mom_CS_C+C_Seas_CS_C+C_Value_CS_C+C_Risk_CS_C
positionbutdm=E_Carry_TS_C+E_Seas_TS_C+E_Mom_TS_C+E_Value_TS_C+E_Maf_TS_C+E_Risk_TS_C+E_Mif_TS_C
positionbucdm=E_Carry_CS_C+E_Seas_CS_C+E_Mom_CS_C+E_Value_CS_C+E_Maf_CS_C+E_Risk_CS_C+E_Mif_CS_C
positionbutem=EM_Mom_TS_C+EM_Carry_TS_C+EM_Seas_TS_C+EM_Risk_TS_C+EM_Value_TS_C
positionbucem=EM_Mom_CS_C+EM_Carry_CS_C+EM_Seas_CS_C+EM_Risk_CS_C+EM_Value_CS_C
positionbutir=IR_Mom_TS_C+IR_Carry_TS_C+IR_Seas_TS_C+IR_Risk_TS_C+IR_Value_TS_C
positionbucir=IR_Mom_CS_C+IR_Carry_CS_C+IR_Seas_CS_C+IR_Risk_CS_C+IR_Value_CS_C


positionbut=cbind(positionbutdm,positionbutc,positionbutem,positionbutir)
positionbuc=cbind(positionbucdm,positionbucc,positionbucem,positionbucir)
positionbu=positionbut+positionbuc
Ret=Ret[rownames(Ret)%in%rownames(Leverage),]
C_Carry_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Carry_TS_C)]*C_Carry_TS_C[1:(nrow(Leverage)-2),]))
C_Mom_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Mom_TS_C)]*C_Mom_TS_C[1:(nrow(Leverage)-2),]))
C_Seas_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Seas_TS_C)]*C_Seas_TS_C[1:(nrow(Leverage)-2),]))
C_Value_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Value_TS_C)]*C_Value_TS_C[1:(nrow(Leverage)-2),]))
C_Risk_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Risk_TS_C)]*C_Risk_TS_C[1:(nrow(Leverage)-2),]))
C_Carry_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Carry_CS_C)]*C_Carry_CS_C[1:(nrow(Leverage)-2),]))
C_Mom_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Mom_CS_C)]*C_Mom_CS_C[1:(nrow(Leverage)-2),]))
C_Seas_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Seas_CS_C)]*C_Seas_CS_C[1:(nrow(Leverage)-2),]))
C_Value_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Value_CS_C)]*C_Value_CS_C[1:(nrow(Leverage)-2),]))
C_Risk_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Risk_CS_C)]*C_Risk_CS_C[1:(nrow(Leverage)-2),]))

E_Carry_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Carry_TS_C)]*E_Carry_TS_C[1:(nrow(Leverage)-2),]))
E_Mom_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Mom_TS_C)]*E_Mom_TS_C[1:(nrow(Leverage)-2),]))
E_Seas_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Seas_TS_C)]*E_Seas_TS_C[1:(nrow(Leverage)-2),]))
E_Value_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Value_TS_C)]*E_Value_TS_C[1:(nrow(Leverage)-2),]))
E_Risk_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Risk_TS_C)]*E_Risk_TS_C[1:(nrow(Leverage)-2),]))
E_Maf_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Maf_TS_C)]*E_Maf_TS_C[1:(nrow(Leverage)-2),]))
E_Mif_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Mif_TS_C)]*E_Mif_TS_C[1:(nrow(Leverage)-2),]))
E_Carry_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Carry_CS_C)]*E_Carry_CS_C[1:(nrow(Leverage)-2),]))
E_Mom_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Mom_CS_C)]*E_Mom_CS_C[1:(nrow(Leverage)-2),]))
E_Seas_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Seas_CS_C)]*E_Seas_CS_C[1:(nrow(Leverage)-2),]))
E_Value_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Value_CS_C)]*E_Value_CS_C[1:(nrow(Leverage)-2),]))
E_Risk_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Risk_CS_C)]*E_Risk_CS_C[1:(nrow(Leverage)-2),]))
E_Maf_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Maf_CS_C)]*E_Maf_CS_C[1:(nrow(Leverage)-2),]))
E_Mif_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Mif_CS_C)]*E_Mif_CS_C[1:(nrow(Leverage)-2),]))

EM_Carry_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Carry_TS_C)]*EM_Carry_TS_C[1:(nrow(Leverage)-2),]))
EM_Mom_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Mom_TS_C)]*EM_Mom_TS_C[1:(nrow(Leverage)-2),]))
EM_Seas_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Seas_TS_C)]*EM_Seas_TS_C[1:(nrow(Leverage)-2),]))
EM_Value_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Value_TS_C)]*EM_Value_TS_C[1:(nrow(Leverage)-2),]))
EM_Risk_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Risk_TS_C)]*EM_Risk_TS_C[1:(nrow(Leverage)-2),]))
EM_Carry_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Carry_CS_C)]*EM_Carry_CS_C[1:(nrow(Leverage)-2),]))
EM_Mom_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Mom_CS_C)]*EM_Mom_CS_C[1:(nrow(Leverage)-2),]))
EM_Seas_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Seas_CS_C)]*EM_Seas_CS_C[1:(nrow(Leverage)-2),]))
EM_Value_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Value_CS_C)]*EM_Value_CS_C[1:(nrow(Leverage)-2),]))
EM_Risk_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Risk_CS_C)]*EM_Risk_CS_C[1:(nrow(Leverage)-2),]))


IR_Carry_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Carry_TS_C)]*IR_Carry_TS_C[1:(nrow(Leverage)-2),]))
IR_Mom_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Mom_TS_C)]*IR_Mom_TS_C[1:(nrow(Leverage)-2),]))
IR_Seas_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Seas_TS_C)]*IR_Seas_TS_C[1:(nrow(Leverage)-2),]))
IR_Value_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Value_TS_C)]*IR_Value_TS_C[1:(nrow(Leverage)-2),]))
IR_Risk_TS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Risk_TS_C)]*IR_Risk_TS_C[1:(nrow(Leverage)-2),]))
IR_Carry_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Carry_CS_C)]*IR_Carry_CS_C[1:(nrow(Leverage)-2),]))
IR_Mom_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Mom_CS_C)]*IR_Mom_CS_C[1:(nrow(Leverage)-2),]))
IR_Seas_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Seas_CS_C)]*IR_Seas_CS_C[1:(nrow(Leverage)-2),]))
IR_Value_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Value_CS_C)]*IR_Value_CS_C[1:(nrow(Leverage)-2),]))
IR_Risk_CS_C1=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Risk_CS_C)]*IR_Risk_CS_C[1:(nrow(Leverage)-2),]))



# commodity=cbind(C_Carry_TS_C1+C_Carry_CS_C1,C_Mom_TS_C1+C_Mom_CS_C1,C_Seas_TS_C1+C_Seas_CS_C1,C_Value_TS_C1+C_Value_CS_C1,C_Risk_TS_C1+C_Risk_CS_C1)
# dm=cbind(E_Carry_TS_C1+E_Carry_CS_C1,E_Mom_TS_C1+E_Mom_CS_C1,E_Seas_TS_C1+E_Seas_CS_C1,E_Value_TS_C1+E_Value_CS_C1,E_Risk_TS_C1+E_Risk_CS_C1,E_Maf_TS_C1+E_Maf_CS_C1,E_Mif_TS_C1+E_Mif_CS_C1)
# em=cbind(EM_Carry_TS_C1+EM_Carry_CS_C1,EM_Mom_TS_C1+EM_Mom_CS_C1,EM_Seas_TS_C1+EM_Seas_CS_C1,EM_Value_TS_C1+EM_Value_CS_C1,EM_Risk_TS_C1+EM_Risk_CS_C1)
commodity=cbind(C_Carry_TS_C1,C_Mom_TS_C1,C_Seas_TS_C1,C_Value_TS_C1,C_Risk_TS_C1,C_Carry_CS_C1,C_Mom_CS_C1,C_Seas_CS_C1,C_Value_CS_C1,C_Risk_CS_C1)
dm=cbind(E_Carry_TS_C1,E_Mom_TS_C1,E_Seas_TS_C1,E_Value_TS_C1,E_Risk_TS_C1,E_Maf_TS_C1,E_Mif_TS_C1,E_Carry_CS_C1,E_Mom_CS_C1,E_Seas_CS_C1,E_Value_CS_C1,E_Risk_CS_C1,E_Maf_CS_C1,E_Mif_CS_C1)
em=cbind(EM_Carry_TS_C1,EM_Mom_TS_C1,EM_Seas_TS_C1,EM_Value_TS_C1,EM_Risk_TS_C1,EM_Carry_CS_C1,EM_Mom_CS_C1,EM_Seas_CS_C1,EM_Value_CS_C1,EM_Risk_CS_C1)
IR=cbind(IR_Carry_TS_C1,IR_Mom_TS_C1,IR_Seas_TS_C1,IR_Value_TS_C1,IR_Risk_TS_C1,IR_Carry_CS_C1,IR_Mom_CS_C1,IR_Seas_CS_C1,IR_Value_CS_C1,IR_Risk_CS_C1)
cumret=cbind(commodity,dm,em,IR)+1
cumret[is.na(cumret)]=1
MASTERcum=as.data.frame(cumret[2:nrow(cumret),])


if(positions==1){
  setwd("D:/R/GRP/live")
  MASTERCON=as.data.frame(read.csv("D:/R/GRP/live/MASTERcon.csv", row.names=1))
  MASTERcum[rownames(MASTERCON),]=MASTERCON
  
  if (updatepos==1){
    write.csv(MASTERcum,"MASTERcon.CSV")
  }
  setwd("D:/R/GRP")
}
setwd("D:/R/GRP")
MASTERcum1=apply(MASTERcum,2,cumprod)[2:nrow(MASTERcum),]
#write.csv(MASTERcum1,"contributionfull.csv")

MASTERcontribution=as.data.frame(MASTERcum1[rownames(MASTERcum1)>"2012-12-31",])
write.csv(MASTERcontribution,"contribution.csv")




digital=function(data2){
  data1=data2
  data1[data2>0]=1
  data1[data2<0]=-1
  return(data1)
}

LONG=function(data2){
  data1=data2
  data1[data2>0]=1
  data1[data2<0]=0
  return(data1)
}

SHORT=function(data2){
  data1=data2
  data1[data2<0]=1
  data1[data2>0]=0
  return(data1)
}

C_Carry_CS_C2=digital(C_Carry_CS_C)/14
C_Mom_CS_C2=digital(C_Mom_CS_C)/14
C_Seas_CS_C2=digital(C_Seas_CS_C)/14
C_Value_CS_C2=digital(C_Value_CS_C)/14
C_Risk_CS_C2=digital(C_Risk_CS_C)/14
E_Carry_CS_C2=digital(E_Carry_CS_C)/14
E_Seas_CS_C2=digital(E_Seas_CS_C)/14
E_Mom_CS_C2=digital(E_Mom_CS_C)/14
EM_Mom_CS_C2=digital(EM_Mom_CS_C)/11
EM_Carry_CS_C2=digital(EM_Carry_CS_C)/11
EM_Seas_CS_C2=digital(EM_Seas_CS_C)/11
EM_Risk_CS_C2=digital(EM_Risk_CS_C)/11
E_Value_CS_C2=digital(E_Value_CS_C)/14
E_Maf_CS_C2=digital(E_Maf_CS_C)/14
E_Risk_CS_C2=digital(E_Risk_CS_C)/14
E_Mif_CS_C2=digital(E_Mif_CS_C)/14
EM_Value_CS_C2=digital(EM_Value_CS_C)/11
IR_Carry_CS_C2=digital(IR_Carry_CS_C)/5
IR_Mom_CS_C2=digital(IR_Mom_CS_C)/5
IR_Seas_CS_C2=digital(IR_Seas_CS_C)/5
IR_Value_CS_C2=digital(IR_Value_CS_C)/5
IR_Risk_CS_C2=digital(IR_Risk_CS_C)/5

C_Carry_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Carry_CS_C2)]*C_Carry_CS_C2[1:(nrow(Leverage)-2),]))
C_Mom_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Mom_CS_C2)]*C_Mom_CS_C2[1:(nrow(Leverage)-2),]))
C_Seas_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Seas_CS_C2)]*C_Seas_CS_C2[1:(nrow(Leverage)-2),]))
C_Value_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Value_CS_C2)]*C_Value_CS_C2[1:(nrow(Leverage)-2),]))
C_Risk_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Risk_CS_C2)]*C_Risk_CS_C2[1:(nrow(Leverage)-2),]))
E_Carry_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Carry_CS_C2)]*E_Carry_CS_C2[1:(nrow(Leverage)-2),]))
E_Seas_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Seas_CS_C2)]*E_Seas_CS_C2[1:(nrow(Leverage)-2),]))
E_Mom_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Mom_CS_C2)]*E_Mom_CS_C2[1:(nrow(Leverage)-2),]))
EM_Mom_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Mom_CS_C2)]*EM_Mom_CS_C2[1:(nrow(Leverage)-2),]))
EM_Carry_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Carry_CS_C2)]*EM_Carry_CS_C2[1:(nrow(Leverage)-2),]))
EM_Seas_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Seas_CS_C2)]*EM_Seas_CS_C2[1:(nrow(Leverage)-2),]))
EM_Risk_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Risk_CS_C2)]*EM_Risk_CS_C2[1:(nrow(Leverage)-2),]))
E_Value_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Value_CS_C2)]*E_Value_CS_C2[1:(nrow(Leverage)-2),]))
E_Maf_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Maf_CS_C2)]*E_Maf_CS_C2[1:(nrow(Leverage)-2),]))
E_Risk_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Risk_CS_C2)]*E_Risk_CS_C2[1:(nrow(Leverage)-2),]))
E_Mif_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Mif_CS_C2)]*E_Mif_CS_C2[1:(nrow(Leverage)-2),]))
EM_Value_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Value_CS_C2)]*EM_Value_CS_C2[1:(nrow(Leverage)-2),]))
IR_Carry_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Carry_CS_C2)]*IR_Carry_CS_C2[1:(nrow(Leverage)-2),]))
IR_Mom_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Mom_CS_C2)]*IR_Mom_CS_C2[1:(nrow(Leverage)-2),]))
IR_Seas_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Seas_CS_C2)]*IR_Seas_CS_C2[1:(nrow(Leverage)-2),]))
IR_Value_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Value_CS_C2)]*IR_Value_CS_C2[1:(nrow(Leverage)-2),]))
IR_Risk_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Risk_CS_C2)]*IR_Risk_CS_C2[1:(nrow(Leverage)-2),]))



factorindex=cbind(E_Carry_CS_C3,E_Seas_CS_C3,E_Mom_CS_C3,E_Value_CS_C3,E_Maf_CS_C3,E_Risk_CS_C3,E_Mif_CS_C3,EM_Carry_CS_C3,EM_Seas_CS_C3,EM_Mom_CS_C3,EM_Value_CS_C3,EM_Risk_CS_C3,C_Carry_CS_C3,C_Seas_CS_C3,C_Mom_CS_C3,C_Value_CS_C3,C_Risk_CS_C3,IR_Carry_CS_C3,IR_Seas_CS_C3,IR_Mom_CS_C3,IR_Value_CS_C3,IR_Risk_CS_C3  )
factorindex2=factorindex[2:nrow(factorindex),]
factorindex2[is.na(factorindex2)]=0
#charts.PerformanceSummary(factorindex2)
DIGITALINDEX=factorindex2
###longversion
C_Carry_CS_C2=LONG(C_Carry_CS_C)/14
C_Mom_CS_C2=LONG(C_Mom_CS_C)/14
C_Seas_CS_C2=LONG(C_Seas_CS_C)/14
C_Value_CS_C2=LONG(C_Value_CS_C)/14
C_Risk_CS_C2=LONG(C_Risk_CS_C)/14
E_Carry_CS_C2=LONG(E_Carry_CS_C)/14
E_Seas_CS_C2=LONG(E_Seas_CS_C)/14
E_Mom_CS_C2=LONG(E_Mom_CS_C)/14
EM_Mom_CS_C2=LONG(EM_Mom_CS_C)/11
EM_Carry_CS_C2=LONG(EM_Carry_CS_C)/11
EM_Seas_CS_C2=LONG(EM_Seas_CS_C)/11
EM_Risk_CS_C2=LONG(EM_Risk_CS_C)/11
E_Value_CS_C2=LONG(E_Value_CS_C)/14
E_Maf_CS_C2=LONG(E_Maf_CS_C)/14
E_Risk_CS_C2=LONG(E_Risk_CS_C)/14
E_Mif_CS_C2=LONG(E_Mif_CS_C)/14
EM_Value_CS_C2=LONG(EM_Value_CS_C)/11
IR_Carry_CS_C2=LONG(IR_Carry_CS_C)/5
IR_Mom_CS_C2=LONG(IR_Mom_CS_C)/5
IR_Seas_CS_C2=LONG(IR_Seas_CS_C)/5
IR_Value_CS_C2=LONG(IR_Value_CS_C)/5
IR_Risk_CS_C2=LONG(IR_Risk_CS_C)/5



C_Carry_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Carry_CS_C2)]*C_Carry_CS_C2[1:(nrow(Leverage)-2),]))
C_Mom_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Mom_CS_C2)]*C_Mom_CS_C2[1:(nrow(Leverage)-2),]))
C_Seas_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Seas_CS_C2)]*C_Seas_CS_C2[1:(nrow(Leverage)-2),]))
C_Value_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Value_CS_C2)]*C_Value_CS_C2[1:(nrow(Leverage)-2),]))
C_Risk_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Risk_CS_C2)]*C_Risk_CS_C2[1:(nrow(Leverage)-2),]))
E_Carry_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Carry_CS_C2)]*E_Carry_CS_C2[1:(nrow(Leverage)-2),]))
E_Seas_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Seas_CS_C2)]*E_Seas_CS_C2[1:(nrow(Leverage)-2),]))
E_Mom_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Mom_CS_C2)]*E_Mom_CS_C2[1:(nrow(Leverage)-2),]))
EM_Mom_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Mom_CS_C2)]*EM_Mom_CS_C2[1:(nrow(Leverage)-2),]))
EM_Carry_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Carry_CS_C2)]*EM_Carry_CS_C2[1:(nrow(Leverage)-2),]))
EM_Seas_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Seas_CS_C2)]*EM_Seas_CS_C2[1:(nrow(Leverage)-2),]))
EM_Risk_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Risk_CS_C2)]*EM_Risk_CS_C2[1:(nrow(Leverage)-2),]))
E_Value_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Value_CS_C2)]*E_Value_CS_C2[1:(nrow(Leverage)-2),]))
E_Maf_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Maf_CS_C2)]*E_Maf_CS_C2[1:(nrow(Leverage)-2),]))
E_Risk_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Risk_CS_C2)]*E_Risk_CS_C2[1:(nrow(Leverage)-2),]))
E_Mif_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Mif_CS_C2)]*E_Mif_CS_C2[1:(nrow(Leverage)-2),]))
EM_Value_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Value_CS_C2)]*EM_Value_CS_C2[1:(nrow(Leverage)-2),]))
IR_Carry_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Carry_CS_C2)]*IR_Carry_CS_C2[1:(nrow(Leverage)-2),]))
IR_Mom_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Mom_CS_C2)]*IR_Mom_CS_C2[1:(nrow(Leverage)-2),]))
IR_Seas_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Seas_CS_C2)]*IR_Seas_CS_C2[1:(nrow(Leverage)-2),]))
IR_Value_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Value_CS_C2)]*IR_Value_CS_C2[1:(nrow(Leverage)-2),]))
IR_Risk_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Risk_CS_C2)]*IR_Risk_CS_C2[1:(nrow(Leverage)-2),]))


factorindex=cbind(E_Carry_CS_C3,E_Seas_CS_C3,E_Mom_CS_C3,E_Value_CS_C3,E_Maf_CS_C3,E_Risk_CS_C3,E_Mif_CS_C3,EM_Carry_CS_C3,EM_Seas_CS_C3,EM_Mom_CS_C3,EM_Value_CS_C3,EM_Risk_CS_C3,C_Carry_CS_C3,C_Seas_CS_C3,C_Mom_CS_C3,C_Value_CS_C3,C_Risk_CS_C3,IR_Carry_CS_C3,IR_Seas_CS_C3,IR_Mom_CS_C3,IR_Value_CS_C3,IR_Risk_CS_C3 )
factorindex2=factorindex[2:nrow(factorindex),]
factorindex2[is.na(factorindex2)]=0
LONGINDEX=factorindex2


###longversion
C_Carry_CS_C2=SHORT(C_Carry_CS_C)/14
C_Mom_CS_C2=SHORT(C_Mom_CS_C)/14
C_Seas_CS_C2=SHORT(C_Seas_CS_C)/14
C_Value_CS_C2=SHORT(C_Value_CS_C)/14
C_Risk_CS_C2=SHORT(C_Risk_CS_C)/14
E_Carry_CS_C2=SHORT(E_Carry_CS_C)/14
E_Seas_CS_C2=SHORT(E_Seas_CS_C)/14
E_Mom_CS_C2=SHORT(E_Mom_CS_C)/14
EM_Mom_CS_C2=SHORT(EM_Mom_CS_C)/11
EM_Carry_CS_C2=SHORT(EM_Carry_CS_C)/11
EM_Seas_CS_C2=SHORT(EM_Seas_CS_C)/11
EM_Risk_CS_C2=SHORT(EM_Risk_CS_C)/11
E_Value_CS_C2=SHORT(E_Value_CS_C)/14
E_Maf_CS_C2=SHORT(E_Maf_CS_C)/14
E_Risk_CS_C2=SHORT(E_Risk_CS_C)/14
E_Mif_CS_C2=SHORT(E_Mif_CS_C)/14
EM_Value_CS_C2=SHORT(EM_Value_CS_C)/11
IR_Carry_CS_C2=SHORT(IR_Carry_CS_C)/5
IR_Mom_CS_C2=SHORT(IR_Mom_CS_C)/5
IR_Seas_CS_C2=SHORT(IR_Seas_CS_C)/5
IR_Value_CS_C2=SHORT(IR_Value_CS_C)/5
IR_Risk_CS_C2=SHORT(IR_Risk_CS_C)/5

C_Carry_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Carry_CS_C2)]*C_Carry_CS_C2[1:(nrow(Leverage)-2),]))
C_Mom_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Mom_CS_C2)]*C_Mom_CS_C2[1:(nrow(Leverage)-2),]))
C_Seas_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Seas_CS_C2)]*C_Seas_CS_C2[1:(nrow(Leverage)-2),]))
C_Value_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Value_CS_C2)]*C_Value_CS_C2[1:(nrow(Leverage)-2),]))
C_Risk_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Risk_CS_C2)]*C_Risk_CS_C2[1:(nrow(Leverage)-2),]))
E_Carry_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Carry_CS_C2)]*E_Carry_CS_C2[1:(nrow(Leverage)-2),]))
E_Seas_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Seas_CS_C2)]*E_Seas_CS_C2[1:(nrow(Leverage)-2),]))
E_Mom_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Mom_CS_C2)]*E_Mom_CS_C2[1:(nrow(Leverage)-2),]))
EM_Mom_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Mom_CS_C2)]*EM_Mom_CS_C2[1:(nrow(Leverage)-2),]))
EM_Carry_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Carry_CS_C2)]*EM_Carry_CS_C2[1:(nrow(Leverage)-2),]))
EM_Seas_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Seas_CS_C2)]*EM_Seas_CS_C2[1:(nrow(Leverage)-2),]))
EM_Risk_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Risk_CS_C2)]*EM_Risk_CS_C2[1:(nrow(Leverage)-2),]))
E_Value_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Value_CS_C2)]*E_Value_CS_C2[1:(nrow(Leverage)-2),]))
E_Maf_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Maf_CS_C2)]*E_Maf_CS_C2[1:(nrow(Leverage)-2),]))
E_Risk_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Risk_CS_C2)]*E_Risk_CS_C2[1:(nrow(Leverage)-2),]))
E_Mif_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Mif_CS_C2)]*E_Mif_CS_C2[1:(nrow(Leverage)-2),]))
EM_Value_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Value_CS_C2)]*EM_Value_CS_C2[1:(nrow(Leverage)-2),]))
IR_Carry_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Carry_CS_C2)]*IR_Carry_CS_C2[1:(nrow(Leverage)-2),]))
IR_Mom_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Mom_CS_C2)]*IR_Mom_CS_C2[1:(nrow(Leverage)-2),]))
IR_Seas_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Seas_CS_C2)]*IR_Seas_CS_C2[1:(nrow(Leverage)-2),]))
IR_Value_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Value_CS_C2)]*IR_Value_CS_C2[1:(nrow(Leverage)-2),]))
IR_Risk_CS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Risk_CS_C2)]*IR_Risk_CS_C2[1:(nrow(Leverage)-2),]))


factorindex=cbind(E_Carry_CS_C3,E_Seas_CS_C3,E_Mom_CS_C3,E_Value_CS_C3,E_Maf_CS_C3,E_Risk_CS_C3,E_Mif_CS_C3,EM_Carry_CS_C3,EM_Seas_CS_C3,EM_Mom_CS_C3,EM_Value_CS_C3,EM_Risk_CS_C3,C_Carry_CS_C3,C_Seas_CS_C3,C_Mom_CS_C3,C_Value_CS_C3,C_Risk_CS_C3,IR_Carry_CS_C3,IR_Seas_CS_C3,IR_Mom_CS_C3,IR_Value_CS_C3,IR_Risk_CS_C3 )
factorindex2=factorindex[2:nrow(factorindex),]
factorindex2[is.na(factorindex2)]=0
SHORTINDEX=factorindex2

DAILYINDEX=cbind(DIGITALINDEX,LONGINDEX,SHORTINDEX)
###shorter verison

DAILYINDEX=DAILYINDEX[rownames(DAILYINDEX)>="2013-12-31",]
###

SORTEDINDEX=DAILYINDEX*0
colnames(DAILYINDEX)=gsub("_C3","",colnames(DAILYINDEX))

for (i in 1:22){
  SORTEDINDEX[,c(3*(i-1)+1,3*(i-1)+2,3*(i-1)+3)]=DAILYINDEX[,c(i,i+22,i+44)]
  colnames(SORTEDINDEX)[c(3*(i-1)+1,3*(i-1)+2,3*(i-1)+3)]=paste(colnames(DAILYINDEX)[c(i,i+22,i+44)],c("L-S","L","S"))
}
SORTEDINDEX=SORTEDINDEX+1
SORTEDINDEX[is.na(SORTEDINDEX)]=1
SORTEDINDEXCUMCS=apply(SORTEDINDEX,2,cumprod)[2:nrow(SORTEDINDEX),]

###Start TS


C_Carry_TS_C2=digital(C_Carry_TS_C)/14
C_Mom_TS_C2=digital(C_Mom_TS_C)/14
C_Seas_TS_C2=digital(C_Seas_TS_C)/14
C_Value_TS_C2=digital(C_Value_TS_C)/14
C_Risk_TS_C2=digital(C_Risk_TS_C)/14
E_Carry_TS_C2=digital(E_Carry_TS_C)/14
E_Seas_TS_C2=digital(E_Seas_TS_C)/14
E_Mom_TS_C2=digital(E_Mom_TS_C)/14
EM_Mom_TS_C2=digital(EM_Mom_TS_C)/11
EM_Carry_TS_C2=digital(EM_Carry_TS_C)/11
EM_Seas_TS_C2=digital(EM_Seas_TS_C)/11
EM_Risk_TS_C2=digital(EM_Risk_TS_C)/11
E_Value_TS_C2=digital(E_Value_TS_C)/14
E_Maf_TS_C2=digital(E_Maf_TS_C)/14
E_Risk_TS_C2=digital(E_Risk_TS_C)/14
E_Mif_TS_C2=digital(E_Mif_TS_C)/14
EM_Value_TS_C2=digital(EM_Value_TS_C)/11
IR_Carry_TS_C2=digital(IR_Carry_TS_C)/5
IR_Mom_TS_C2=digital(IR_Mom_TS_C)/5
IR_Seas_TS_C2=digital(IR_Seas_TS_C)/5
IR_Value_TS_C2=digital(IR_Value_TS_C)/5
IR_Risk_TS_C2=digital(IR_Risk_TS_C)/5

C_Carry_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Carry_TS_C2)]*C_Carry_TS_C2[1:(nrow(Leverage)-2),]))
C_Mom_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Mom_TS_C2)]*C_Mom_TS_C2[1:(nrow(Leverage)-2),]))
C_Seas_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Seas_TS_C2)]*C_Seas_TS_C2[1:(nrow(Leverage)-2),]))
C_Value_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Value_TS_C2)]*C_Value_TS_C2[1:(nrow(Leverage)-2),]))
C_Risk_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Risk_TS_C2)]*C_Risk_TS_C2[1:(nrow(Leverage)-2),]))
E_Carry_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Carry_TS_C2)]*E_Carry_TS_C2[1:(nrow(Leverage)-2),]))
E_Seas_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Seas_TS_C2)]*E_Seas_TS_C2[1:(nrow(Leverage)-2),]))
E_Mom_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Mom_TS_C2)]*E_Mom_TS_C2[1:(nrow(Leverage)-2),]))
EM_Mom_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Mom_TS_C2)]*EM_Mom_TS_C2[1:(nrow(Leverage)-2),]))
EM_Carry_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Carry_TS_C2)]*EM_Carry_TS_C2[1:(nrow(Leverage)-2),]))
EM_Seas_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Seas_TS_C2)]*EM_Seas_TS_C2[1:(nrow(Leverage)-2),]))
EM_Risk_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Risk_TS_C2)]*EM_Risk_TS_C2[1:(nrow(Leverage)-2),]))
E_Value_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Value_TS_C2)]*E_Value_TS_C2[1:(nrow(Leverage)-2),]))
E_Maf_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Maf_TS_C2)]*E_Maf_TS_C2[1:(nrow(Leverage)-2),]))
E_Risk_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Risk_TS_C2)]*E_Risk_TS_C2[1:(nrow(Leverage)-2),]))
E_Mif_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Mif_TS_C2)]*E_Mif_TS_C2[1:(nrow(Leverage)-2),]))
EM_Value_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Value_TS_C2)]*EM_Value_TS_C2[1:(nrow(Leverage)-2),]))
IR_Carry_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Carry_TS_C2)]*IR_Carry_TS_C2[1:(nrow(Leverage)-2),]))
IR_Mom_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Mom_TS_C2)]*IR_Mom_TS_C2[1:(nrow(Leverage)-2),]))
IR_Seas_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Seas_TS_C2)]*IR_Seas_TS_C2[1:(nrow(Leverage)-2),]))
IR_Value_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Value_TS_C2)]*IR_Value_TS_C2[1:(nrow(Leverage)-2),]))
IR_Risk_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Risk_TS_C2)]*IR_Risk_TS_C2[1:(nrow(Leverage)-2),]))

factorindex=cbind(E_Carry_TS_C3,E_Seas_TS_C3,E_Mom_TS_C3,E_Value_TS_C3,E_Maf_TS_C3,E_Risk_TS_C3,E_Mif_TS_C3,EM_Carry_TS_C3,EM_Seas_TS_C3,EM_Mom_TS_C3,EM_Value_TS_C3,EM_Risk_TS_C3,C_Carry_TS_C3,C_Seas_TS_C3,C_Mom_TS_C3,C_Value_TS_C3,C_Risk_TS_C3,IR_Carry_TS_C3,IR_Seas_TS_C3,IR_Mom_TS_C3,IR_Value_TS_C3,IR_Risk_TS_C3 )
factorindex2=factorindex[2:nrow(factorindex),]
factorindex2[is.na(factorindex2)]=0
#charts.PerformanceSummary(factorindex2)
DIGITALINDEX=factorindex2
###longversion
C_Carry_TS_C2=LONG(C_Carry_TS_C)/14
C_Mom_TS_C2=LONG(C_Mom_TS_C)/14
C_Seas_TS_C2=LONG(C_Seas_TS_C)/14
C_Value_TS_C2=LONG(C_Value_TS_C)/14
C_Risk_TS_C2=LONG(C_Risk_TS_C)/14
E_Carry_TS_C2=LONG(E_Carry_TS_C)/14
E_Seas_TS_C2=LONG(E_Seas_TS_C)/14
E_Mom_TS_C2=LONG(E_Mom_TS_C)/14
EM_Mom_TS_C2=LONG(EM_Mom_TS_C)/11
EM_Carry_TS_C2=LONG(EM_Carry_TS_C)/11
EM_Seas_TS_C2=LONG(EM_Seas_TS_C)/11
EM_Risk_TS_C2=LONG(EM_Risk_TS_C)/11
E_Value_TS_C2=LONG(E_Value_TS_C)/14
E_Maf_TS_C2=LONG(E_Maf_TS_C)/14
E_Risk_TS_C2=LONG(E_Risk_TS_C)/14
E_Mif_TS_C2=LONG(E_Mif_TS_C)/14
EM_Value_TS_C2=LONG(EM_Value_TS_C)/11
IR_Carry_TS_C2=LONG(IR_Carry_TS_C)/5
IR_Mom_TS_C2=LONG(IR_Mom_TS_C)/5
IR_Seas_TS_C2=LONG(IR_Seas_TS_C)/5
IR_Value_TS_C2=LONG(IR_Value_TS_C)/5
IR_Risk_TS_C2=LONG(IR_Risk_TS_C)/5

C_Carry_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Carry_TS_C2)]*C_Carry_TS_C2[1:(nrow(Leverage)-2),]))
C_Mom_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Mom_TS_C2)]*C_Mom_TS_C2[1:(nrow(Leverage)-2),]))
C_Seas_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Seas_TS_C2)]*C_Seas_TS_C2[1:(nrow(Leverage)-2),]))
C_Value_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Value_TS_C2)]*C_Value_TS_C2[1:(nrow(Leverage)-2),]))
C_Risk_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Risk_TS_C2)]*C_Risk_TS_C2[1:(nrow(Leverage)-2),]))
E_Carry_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Carry_TS_C2)]*E_Carry_TS_C2[1:(nrow(Leverage)-2),]))
E_Seas_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Seas_TS_C2)]*E_Seas_TS_C2[1:(nrow(Leverage)-2),]))
E_Mom_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Mom_TS_C2)]*E_Mom_TS_C2[1:(nrow(Leverage)-2),]))
EM_Mom_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Mom_TS_C2)]*EM_Mom_TS_C2[1:(nrow(Leverage)-2),]))
EM_Carry_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Carry_TS_C2)]*EM_Carry_TS_C2[1:(nrow(Leverage)-2),]))
EM_Seas_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Seas_TS_C2)]*EM_Seas_TS_C2[1:(nrow(Leverage)-2),]))
EM_Risk_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Risk_TS_C2)]*EM_Risk_TS_C2[1:(nrow(Leverage)-2),]))
E_Value_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Value_TS_C2)]*E_Value_TS_C2[1:(nrow(Leverage)-2),]))
E_Maf_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Maf_TS_C2)]*E_Maf_TS_C2[1:(nrow(Leverage)-2),]))
E_Risk_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Risk_TS_C2)]*E_Risk_TS_C2[1:(nrow(Leverage)-2),]))
E_Mif_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Mif_TS_C2)]*E_Mif_TS_C2[1:(nrow(Leverage)-2),]))
EM_Value_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Value_TS_C2)]*EM_Value_TS_C2[1:(nrow(Leverage)-2),]))
IR_Carry_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Carry_TS_C2)]*IR_Carry_TS_C2[1:(nrow(Leverage)-2),]))
IR_Mom_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Mom_TS_C2)]*IR_Mom_TS_C2[1:(nrow(Leverage)-2),]))
IR_Seas_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Seas_TS_C2)]*IR_Seas_TS_C2[1:(nrow(Leverage)-2),]))
IR_Value_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Value_TS_C2)]*IR_Value_TS_C2[1:(nrow(Leverage)-2),]))
IR_Risk_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Risk_TS_C2)]*IR_Risk_TS_C2[1:(nrow(Leverage)-2),]))

factorindex=cbind(E_Carry_TS_C3,E_Seas_TS_C3,E_Mom_TS_C3,E_Value_TS_C3,E_Maf_TS_C3,E_Risk_TS_C3,E_Mif_TS_C3,EM_Carry_TS_C3,EM_Seas_TS_C3,EM_Mom_TS_C3,EM_Value_TS_C3,EM_Risk_TS_C3,C_Carry_TS_C3,C_Seas_TS_C3,C_Mom_TS_C3,C_Value_TS_C3,C_Risk_TS_C3,IR_Carry_TS_C3,IR_Seas_TS_C3,IR_Mom_TS_C3,IR_Value_TS_C3,IR_Risk_TS_C3 )
factorindex2=factorindex[2:nrow(factorindex),]
factorindex2[is.na(factorindex2)]=0
LONGINDEX=factorindex2


###longversion
C_Carry_TS_C2=SHORT(C_Carry_TS_C)/14
C_Mom_TS_C2=SHORT(C_Mom_TS_C)/14
C_Seas_TS_C2=SHORT(C_Seas_TS_C)/14
C_Value_TS_C2=SHORT(C_Value_TS_C)/14
C_Risk_TS_C2=SHORT(C_Risk_TS_C)/14
E_Carry_TS_C2=SHORT(E_Carry_TS_C)/14
E_Seas_TS_C2=SHORT(E_Seas_TS_C)/14
E_Mom_TS_C2=SHORT(E_Mom_TS_C)/14
EM_Mom_TS_C2=SHORT(EM_Mom_TS_C)/11
EM_Carry_TS_C2=SHORT(EM_Carry_TS_C)/11
EM_Seas_TS_C2=SHORT(EM_Seas_TS_C)/11
EM_Risk_TS_C2=SHORT(EM_Risk_TS_C)/11
E_Value_TS_C2=SHORT(E_Value_TS_C)/14
E_Maf_TS_C2=SHORT(E_Maf_TS_C)/14
E_Risk_TS_C2=SHORT(E_Risk_TS_C)/14
E_Mif_TS_C2=SHORT(E_Mif_TS_C)/14
EM_Value_TS_C2=SHORT(EM_Value_TS_C)/11
IR_Carry_TS_C2=SHORT(IR_Carry_TS_C)/5
IR_Mom_TS_C2=SHORT(IR_Mom_TS_C)/5
IR_Seas_TS_C2=SHORT(IR_Seas_TS_C)/5
IR_Value_TS_C2=SHORT(IR_Value_TS_C)/5
IR_Risk_TS_C2=SHORT(IR_Risk_TS_C)/5

C_Carry_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Carry_TS_C2)]*C_Carry_TS_C2[1:(nrow(Leverage)-2),]))
C_Mom_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Mom_TS_C2)]*C_Mom_TS_C2[1:(nrow(Leverage)-2),]))
C_Seas_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Seas_TS_C2)]*C_Seas_TS_C2[1:(nrow(Leverage)-2),]))
C_Value_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Value_TS_C2)]*C_Value_TS_C2[1:(nrow(Leverage)-2),]))
C_Risk_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(C_Risk_TS_C2)]*C_Risk_TS_C2[1:(nrow(Leverage)-2),]))
E_Carry_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Carry_TS_C2)]*E_Carry_TS_C2[1:(nrow(Leverage)-2),]))
E_Seas_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Seas_TS_C2)]*E_Seas_TS_C2[1:(nrow(Leverage)-2),]))
E_Mom_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Mom_TS_C2)]*E_Mom_TS_C2[1:(nrow(Leverage)-2),]))
EM_Mom_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Mom_TS_C2)]*EM_Mom_TS_C2[1:(nrow(Leverage)-2),]))
EM_Carry_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Carry_TS_C2)]*EM_Carry_TS_C2[1:(nrow(Leverage)-2),]))
EM_Seas_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Seas_TS_C2)]*EM_Seas_TS_C2[1:(nrow(Leverage)-2),]))
EM_Risk_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Risk_TS_C2)]*EM_Risk_TS_C2[1:(nrow(Leverage)-2),]))
E_Value_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Value_TS_C2)]*E_Value_TS_C2[1:(nrow(Leverage)-2),]))
E_Maf_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Maf_TS_C2)]*E_Maf_TS_C2[1:(nrow(Leverage)-2),]))
E_Risk_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Risk_TS_C2)]*E_Risk_TS_C2[1:(nrow(Leverage)-2),]))
E_Mif_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(E_Mif_TS_C2)]*E_Mif_TS_C2[1:(nrow(Leverage)-2),]))
EM_Value_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(EM_Value_TS_C2)]*EM_Value_TS_C2[1:(nrow(Leverage)-2),]))
IR_Carry_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Carry_TS_C2)]*IR_Carry_TS_C2[1:(nrow(Leverage)-2),]))
IR_Mom_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Mom_TS_C2)]*IR_Mom_TS_C2[1:(nrow(Leverage)-2),]))
IR_Seas_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Seas_TS_C2)]*IR_Seas_TS_C2[1:(nrow(Leverage)-2),]))
IR_Value_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Value_TS_C2)]*IR_Value_TS_C2[1:(nrow(Leverage)-2),]))
IR_Risk_TS_C3=rowSums(rbind(rep(0,ncol(Leverage)),Ret[3:nrow(Ret),colnames(IR_Risk_TS_C2)]*IR_Risk_TS_C2[1:(nrow(Leverage)-2),]))

factorindex=cbind(E_Carry_TS_C3,E_Seas_TS_C3,E_Mom_TS_C3,E_Value_TS_C3,E_Maf_TS_C3,E_Risk_TS_C3,E_Mif_TS_C3,EM_Carry_TS_C3,EM_Seas_TS_C3,EM_Mom_TS_C3,EM_Value_TS_C3,EM_Risk_TS_C3,C_Carry_TS_C3,C_Seas_TS_C3,C_Mom_TS_C3,C_Value_TS_C3,C_Risk_TS_C3,IR_Carry_TS_C3,IR_Seas_TS_C3,IR_Mom_TS_C3,IR_Value_TS_C3,IR_Risk_TS_C3 )
factorindex2=factorindex[2:nrow(factorindex),]
factorindex2[is.na(factorindex2)]=0
SHORTINDEX=factorindex2
portperf=as.data.frame(constraintportlag1[2:length(constraintportlag1)])
DAILYINDEX=cbind(DIGITALINDEX,LONGINDEX,SHORTINDEX,portperf)
colnames(DAILYINDEX)[ncol(DAILYINDEX)]="NET LAGGED"
###shorter verison

DAILYINDEX=DAILYINDEX[rownames(DAILYINDEX)>="2013-12-31",]
###

SORTEDINDEX=DAILYINDEX*0
colnames(DAILYINDEX)=gsub("_C3","",colnames(DAILYINDEX))

for (i in 1:22){
  SORTEDINDEX[,c(3*(i-1)+1,3*(i-1)+2,3*(i-1)+3)]=DAILYINDEX[,c(i,i+22,i+44)]
  colnames(SORTEDINDEX)[c(3*(i-1)+1,3*(i-1)+2,3*(i-1)+3)]=paste(colnames(DAILYINDEX)[c(i,i+22,i+44)],c("L-S","L","S"))
}
SORTEDINDEX[,ncol(SORTEDINDEX)]=DAILYINDEX[,ncol(DAILYINDEX)]
SORTEDINDEX=SORTEDINDEX+1
SORTEDINDEX[is.na(SORTEDINDEX)]=1
SORTEDINDEXCUMTS=apply(SORTEDINDEX,2,cumprod)[2:nrow(SORTEDINDEX),]

dailyreport=cbind(cbind(SORTEDINDEXCUMCS,SORTEDINDEXCUMTS)[,seq(1,132,3)],SORTEDINDEXCUMTS[,ncol(SORTEDINDEXCUMTS)])

write.csv(dailyreport,"index.csv")

Dailyret=Ret[rownames(Ret)>="2012-12-31",]
write.csv(Dailyret,"dailyret.csv")
Dailyret=Dailyret+1
retCUM=apply(Dailyret,2,cumprod)[2:nrow(Dailyret),]
write.csv(retCUM,"dailyretcum.csv")

assetcontribution=Ret[3:nrow(Ret),]*constraintAGG[1:(nrow(constraintAGG)-2),]+1

assetcontribution=cbind(assetcontribution,(portperf+1))
write.csv(assetcontribution,"assetretcum2.csv")
assetcontribution[is.na(assetcontribution)]=1
assetCUM=apply(assetcontribution,2,cumprod)[2:nrow(assetcontribution),]
write.csv(assetCUM[rownames(assetCUM)>="2012-12-31",],"assetretcum.csv")
###

if(test!=1){
  files=list.files("D:/R/GRP/live")
  path=paste("D:/R/GRP/backupsignals/",today(),sep="")
  dir.create(path)
  
  files1=paste("D:/R/GRP/live/",files,sep="")
  file.copy(from=files1, to=path,overwrite=TRUE)
}

print("GRP done, working on NH")
#source('D:/R/GRP/public/nh.R')