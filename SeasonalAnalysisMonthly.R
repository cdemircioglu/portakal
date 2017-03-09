rm(list=ls(all=TRUE)) 
#Load the library
library(RMySQL)
library(xtable)
library(data.table)

options(warn=-1)
myhost <- "cemoptions.cloudapp.net"
options(stringsAsFactors = FALSE)

#Format the numbers
DecimalPlaces <- function(x) {
  if ((x %% 1) != 0) {
    nchar(strsplit(sub('0+$', '', as.character(x)), ".", fixed=TRUE)[[1]][[2]])
  } else {
    return(0)
  }
}

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
  stocktickervector <- sort(c("6M"))
}

#Create the resulting data frame
resultstable <- data.frame(FUTURE=NA,BUY0=0.0,BUY1=0.0,SELL0=0.0,SELL1=0.0)

#Loop on stock tickers
for(stockticker in stocktickervector)
{
  #Processing now
  print(stockticker)
  
  #Connection settings and the dataset
  mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)
  rs = dbSendQuery(mydb, paste("CALL GetMonthlySD('",stockticker,"')",sep=""))
  stockdata = fetch(rs, n=-1)
  
  #Set the parameters
  gap_down = 0
  gap_up=0
  
  #Create the resulting data frame
  results = data.frame(MONTH = NA, AVE_GAP_D =NA, STD_GAP_D = NA, AVE_GAP_U=NA, STD_GAP_U=NA)
  
  #This is the logic for two months. Buy in Jan, sell in Feb. [Referene point is the end of December]
  for (j in 1:12){
    for(i in 1:floor((dim(stockdata)[1]-2)/12) ){
      gap_down[i] = (stockdata$MCLOSE[i*12+j-2]-min(stockdata$MLOW[i*12+j-1],stockdata$MLOW[i*12+j]))/stockdata$MCLOSE[i*12+j-2]
      gap_up[i] = (-stockdata$MCLOSE[i*12+j-2]+max(stockdata$MHIGH[i*12+j-1],stockdata$MHIGH[i*12+j]))/stockdata$MCLOSE[i*12+j-2]
    }
    
    #Calculate the long mean/sd, short mean/sd
    temp_results = data.frame(MONTH = j, 
                              AVE_GAP_D =mean(gap_down[which(gap_down>0)]), 
                              STD_GAP_D = sd(gap_down[which(gap_down>0)]),
                              AVE_GAP_U =mean(gap_up[which(gap_up>0)]), 
                              STD_GAP_U = sd(gap_up[which(gap_up>0)])
    )
    #Add them to the resulting data frame
    results = rbind(temp_results, results)  
  }
  
  #Find the complete rows
  results <- results[complete.cases(results),]
  
  #Conversion factor between continuos future data and current data based on close of current vs close of continous.
  conversionfactor <- stockdata$CONVERSIONFACTOR[1];
  
  #The close of the month
  mclose <- tail(stockdata$MCLOSE,1)
  
  #Get the current month number
  currentmonth <- (month(Sys.Date())+1) %% 12 #Always use the forward month
  
  #Find the decimal places  
  dec <- DecimalPlaces(tail(stockdata$SETTLE,1))
  
  #Find base 
  buy0 <- format(round(mclose*(1-results$AVE_GAP_D[currentmonth]-0*results$STD_GAP_D[currentmonth])*conversionfactor, dec), nsmall = dec)
  sell0 <- format(round(mclose*(1+results$AVE_GAP_U[currentmonth]+0*results$STD_GAP_U[currentmonth])*conversionfactor, dec), nsmall = dec)  
  
  #Find the 1SD 2SD figures
  buy1 <- format(round(mclose*(1-results$AVE_GAP_D[currentmonth]-1*results$STD_GAP_D[currentmonth])*conversionfactor, dec), nsmall = dec)
  buy2 <- format(round(mclose*(1-results$AVE_GAP_D[currentmonth]-2*results$STD_GAP_D[currentmonth])*conversionfactor, dec), nsmall = dec)
  sell1 <- format(round(mclose*(1+results$AVE_GAP_U[currentmonth]+1*results$STD_GAP_U[currentmonth])*conversionfactor, dec), nsmall = dec)
  sell2 <- format(round(mclose*(1+results$AVE_GAP_U[currentmonth]+2*results$STD_GAP_U[currentmonth])*conversionfactor, dec), nsmall = dec)
  
  #Futures predict table entry. The period will be 30 for monthly ones. At this point 
  #we can't calculate as of RSI values, hence -1. Another MySQL bug, can not call a proc and insert, delete from the same connection. 
  #Create a new connection.
  mydb2 = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)
  
  #First delete already existing record
  query <- "DELETE FROM futurespredict WHERE period = 30 AND FUTURE = 'CCC' AND SNAPSHOTDATE = (SELECT * FROM (SELECT MAX(SNAPSHOTDATE) FROM futurespredict) A );"
  query <- gsub("CCC", stockticker, query)
  dbSendQuery(mydb2,query)
  
  #Second insert the new record
  query <- "INSERT INTO futurespredict SELECT DISTINCT FUTURE,`CLOSE`,DDD,SNAPSHOTDATE,30 AS PERIOD,-1 AS BUYRSI1,-1 AS BUYRSI2,-1 AS SELLRSI1,-1 AS SELLRSI2,RSI,MFI,MVA FROM futurespredict WHERE FUTURE = 'CCC' AND SNAPSHOTDATE = (SELECT MAX(SNAPSHOTDATE) FROM futurespredict);"
  query <- gsub("CCC", stockticker, query)
  query <- gsub("DDD", paste(buy0,sell0,buy1,sell1,buy2,sell2,sep=","), query)
  dbSendQuery(mydb2,query)
  
  #Close the database
  dbDisconnect((mydb2))
  
  #Create a table
  resultstable <- rbind(resultstable, c(stockticker,buy0,buy1,sell0,sell1))  
  
  #Close the database
  dbDisconnect((mydb))
  
}

#The data of this script is stored at the database and retreived by seasonalanalysis email script. 

#Create the table for output
#resultstable <- resultstable[complete.cases(resultstable),]
#finaldt <- as.data.table(resultstable)
#print(xtable(as.data.frame.matrix(finaldt)), type='html', file="/home/cem/emailcontent_seasonality_monthly.html")