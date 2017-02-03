rm(list=ls(all=TRUE)) 
#Load the library
library(RMySQL)
library(xtable)
library(data.table)

options(warn=-1)
myhost <- "cemoptions.cloudapp.net"
options(stringsAsFactors = FALSE)

#Check if the file exists, it not use a constant. 
if(file.exists("/home/cem/portakal/futures.csv"))
{
  futuresblob <- read.csv("/home/cem/portakal/futures.csv", header=FALSE)  # read csv file 
  stocktickervector <- sort(as.vector(futuresblob[,1]))
  stocktickervector <- stocktickervector[! stocktickervector %in% c("ZB")] #Remove ZB there is a but in R about the decimals in ZB.
  stocktickervector <- c(stocktickervector,"ZB") #Remove ZB there is a but in R about the decimals in ZB. 
} else
{
  #stocktickervector <- sort(c("ZB","NG","ES","6J","6A","6B","CL","SB","6E","GC","SI"))
  stocktickervector <- sort(c("HE"))
}

#Loop on stock tickers
for(stockticker in stocktickervector)
{
  #Processing now
  print(stockticker)
  
  #Connection settings and the dataset
  mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)
  rs = dbSendQuery(mydb, paste("CALL GetMonthlySD('",stockticker,"')",sep=""))
  stockdata = fetch(rs, n=-1)
}

#Close the database connections
killDbConnections <- function () {
  all_cons <- dbListConnections(MySQL())
  print(all_cons)
  
  for(con in all_cons)
    +  dbDisconnect(con)
  print(paste(length(all_cons), " connections killed."))
}

killDbConnections()

gap_down = 0
gap_up=0

results = data.frame(MONTH = NA, AVE_GAP_D =NA, STD_GAP_D = NA, AVE_GAP_U=NA, STD_GAP_U=NA)

for (j in 1:12){
  for(i in 1:floor(dim(stockdata)[1]/12)-1 ){
    gap_down[i] = (stockdata$MCLOSE[i*12+j-2]-min(stockdata$MLOW[i*12+j-1],stockdata$MLOW[i*12+j]))/stockdata$MCLOSE[i*12+j-2]
    
    gap_up[i] = (-stockdata$MCLOSE[i*12+j-2]+max(stockdata$MHIGH[i*12+j-1],stockdata$MHIGH[i*12+j]))/stockdata$MCLOSE[i*12+j-2]
  }
  
  print(length(gap_down))
  temp_results = data.frame(MONTH = j, AVE_GAP_D =mean(gap_down[which(gap_down>0)]), STD_GAP_D = sd(gap_down[which(gap_down>0)]),AVE_GAP_U =mean(gap_up[which(gap_up>0)]), STD_GAP_U = sd(gap_up[which(gap_up>0)]))
  results = rbind(temp_results, results)
}

#Connection settings and the dataset
mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)
rs = dbSendQuery(mydb, paste("SELECT F.OPEN, F.LAST AS CLOSE, F.LOW AS DAYLOW, F.HIGH AS DAYHIGH, F.SNAPSHOTDATE, S.SETTLE, (SELECT P.LAST FROM futures P WHERE F.FUTURE = P.FUTURE AND F.SNAPSHOTDATE > P.SNAPSHOTDATE ORDER BY P.SNAPSHOTDATE DESC LIMIT 1) AS PCLOSE, F.VOLUME FROM futures F INNER JOIN futuresspot S ON F.FUTURE = S.FUTURE WHERE F.FUTURE = '",stockticker,"' ORDER BY F.SNAPSHOTDATE DESC",sep=""))
stockdata_cont = fetch(rs, n=-1)

#Conversion factor between continuos future data and current data based on close of current vs close of continous.
conversionfactor <- stockdata_cont[1,6]/stockdata_cont[1,2]



#Example Usage
print("For February entry prices are: Mean, Mean+1SD, Mean +2SD")
print(stockdata$MCLOSE[192]*(1-results$AVE_GAP_D[11]-0*results$STD_GAP_D[11])*conversionfactor )
print(stockdata$MCLOSE[192]*(1-results$AVE_GAP_D[11]-1*results$STD_GAP_D[11])*conversionfactor  )
print(stockdata$MCLOSE[192]*(1-results$AVE_GAP_D[11]-2*results$STD_GAP_D[11])*conversionfactor )
print("Short targets")
print(stockdata$MCLOSE[192]*(1+results$AVE_GAP_U[11]+0*results$STD_GAP_U[11])*conversionfactor)
print(stockdata$MCLOSE[192]*(1+results$AVE_GAP_U[11]+1*results$STD_GAP_U[11])*conversionfactor)
print(stockdata$MCLOSE[192]*(1+results$AVE_GAP_U[11]+2*results$STD_GAP_U[11])*conversionfactor)