library(dplyr)
library(lubridate)
library(Quandl)
library(RMySQL)
library(xtable)
library(data.table)
library(formattable)

#Set the server
myhost <- "cemoptions.cloudapp.net"
options(stringsAsFactors=F)

#Set the decimal places
DecimalPlaces <- function(x,dec) {
  return (format(round(x, dec), nsmall=dec))
}

#Connection settings and the dataset
mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)

#Truncate the table
dbSendQuery(mydb, "TRUNCATE TABLE cboemetric;")

#Get the skew figures from the quandl
myfuture <- Quandl("CBOE/SKEW", api_key="zK6coAV1K5eyxuaPvWJm")

#Loop on the rows
for (j in 1:nrow(myfuture)){
  
  #Create the query
  query <- "INSERT INTO cboemetric VALUES ('SKEW','AAA',BBB)"
  query <- gsub("AAA", myfuture[j,1], query)
  query <- gsub("BBB", as.character(myfuture[j,2]), query)
  
  #Execute the query
  dbSendQuery(mydb, query)   
}

#Get the VIX figures from the quandl
myfuture <- Quandl("CBOE/VIX", start_date="1999-12-31", api_key="zK6coAV1K5eyxuaPvWJm")

#Loop on the rows
for (j in 1:nrow(myfuture)){
  
  #Create the query
  query <- "INSERT INTO cboemetric VALUES ('VIX','AAA',BBB)"
  query <- gsub("AAA", myfuture[j,1], query)
  query <- gsub("BBB", as.character(myfuture[j,2]), query)
  
  #Execute the query
  dbSendQuery(mydb, query)   
}

#Close the database
dbDisconnect((mydb))

#Connection settings and the dataset
mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)

#Get the data from database, snapshotdate, skew, vix, left join on vix. 
rs = dbSendQuery(mydb, "SELECT S.SNAPSHOTDATE, S.VALUE AS SKEW, V.VALUE AS VIX FROM (SELECT * FROM cboemetric where metric = 'SKEW') S LEFT JOIN (SELECT * FROM cboemetric where metric = 'VIX') V ON S.SNAPSHOTDATE = V.SNAPSHOTDATE ORDER BY S.SNAPSHOTDATE DESC")
myfuture = fetch(rs, n=-1)

#Calculate the SKEW/VIX ratio
myfuture$'SKEW/VIX' <- myfuture$SKEW/myfuture$VIX 

#Calculate skew percetile rank
myfuture <-
  myfuture %>%
  mutate('SKEW RANK'=rank(SKEW)/length(SKEW))

#Calculate skew/vix percetile rank
myfuture <-
  myfuture[!is.na(myfuture$VIX),] %>%
  mutate('SKEW/VIX RANK'=rank(SKEW/VIX)/length(SKEW/VIX))

#Create the resulting data frame
resultstable <- data.frame(COL0=NA,COL1=NA,COL2=NA,COL3=NA,COL4=NA,COL5=NA)

#Show the last record of VIX / SKEW
resultstable <- rbind(
  data.frame(
    COL0="0",
    COL1=paste(colnames(myfuture)[2],":",myfuture[1,2],sep=""),
    COL2=paste(colnames(myfuture)[3],":",myfuture[1,3],sep=""),
    COL3=paste(colnames(myfuture)[4],":",DecimalPlaces(myfuture[1,4],2),sep=""),
    COL4=paste(colnames(myfuture)[5],":",DecimalPlaces(myfuture[1,5],4),sep=""),
    COL5=paste(colnames(myfuture)[6],":",DecimalPlaces(myfuture[1,6],4),sep="")
  ),resultstable
)

#Close the database
dbDisconnect((mydb))

#Connection settings and the dataset
mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)

#Get the market volume data
rs = dbSendQuery(mydb, "CALL GetMarketVolume('SPY')")
stockdata = fetch(rs, n=-1)

#Close the database
dbDisconnect((mydb))

#Show the last record
resultstable <- rbind(
  data.frame(
    COL0="1",
    COL1="VOL RANK:SPY",
    COL2=paste("DAY:",stockdata[1,1],sep=""),
    COL3=paste("TWO WEEKS:",stockdata[1,2],sep=""),
    COL4=paste("MONTH:",stockdata[1,3],sep=""),
    COL5=" "
  ),resultstable
)

#Connection settings and the dataset
mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)

#Get the market volume data
rs = dbSendQuery(mydb, "CALL GetMarketVolume('QQQ')")
stockdata = fetch(rs, n=-1)

#Close the database
dbDisconnect((mydb))

#Show the last record
resultstable <- rbind(
  data.frame(
    COL0="2",
    COL1="VOL RANK:QQQ",
    COL2=paste("DAY:",stockdata[1,1],sep=""),
    COL3=paste("TWO WEEKS:",stockdata[1,2],sep=""),
    COL4=paste("MONTH:",stockdata[1,3],sep=""),
    COL5=" "
  ),resultstable
)  


#Last step remove the NA row. 
resultstable <- resultstable[!is.na(resultstable$COL0),]

#Order the data frame
resultstable <- resultstable[order(resultstable$COL0),c(2:6)]

#Create a table
finaldt <- as.data.table(resultstable)
print(xtable(as.data.frame.matrix(finaldt)), type='html', file="/home/cem/emailcontent_stats.html",include.colnames=FALSE,,include.rownames=FALSE)



