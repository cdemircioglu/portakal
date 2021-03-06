library(dplyr)
library(lubridate)
library(Quandl)
library(RMySQL)

#Set the server
myhost <- "cemoptions.cloudapp.net"

#Set the date for the future
mydate <- format(Sys.time(), "%Y-%m-%d %H:%M:%d")

#Check if the file exists, it not use a constant. 
if(file.exists("/home/cem/portakal/futures.csv"))
{
  futuresblob <- read.csv("/home/cem/portakal/futures.csv", header=FALSE,  colClasses = "character")  # read csv file 
  stocktickervector <- futuresblob[order(futuresblob$V1),c(1,2)]
} else
{
  #stocktickervector <- sort(c("ZB","NG","ES","6J","6A","6B","CL","SB","6E","GC","SI"))
  #stocktickervector <- sort(c("GC","NG"))
  futuresblob <- read.csv("c:\\temp\\futures.csv", header=FALSE,  colClasses = "character")  # read csv file 
  stocktickervector <- futuresblob[order(futuresblob$V1),c(1,2)]
}

#Truncate the table first
#Connection settings and the dataset
mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)

query <- "TRUNCATE TABLE futuresdata;"
dbSendQuery(mydb,query)

#Close the database
dbDisconnect((mydb))

#Write to the database
for(i in 1:nrow(stocktickervector)) 
{
  #Connection settings and the dataset
  mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)
  
  #Get the ticker
  stockticker <- stocktickervector[i,1] #Get the symbol
  
  #Print the ticket
  print(stockticker)
  
  #Get the futures
  myfuture <- Quandl(paste("CHRIS/",stocktickervector[i,2],sep=""), start_date="2000-12-31", api_key="zK6coAV1K5eyxuaPvWJm")

  #Reduce the myfuture
  if(!grepl("ICE",stocktickervector[i,2])) #CME futures
  {
    myfuture <- myfuture[,c(1,3,4,5)]
  } else
  {
    myfuture <- myfuture[,c(1,3,4,5)]
  }
  
  #Only consider the complete cases
  myfuture <- myfuture[complete.cases(myfuture),]
  
  #Loop on the values pulled  
  for(x in 1:nrow(myfuture)) 
  {
    if(!grepl("ICE",stocktickervector[i,2])) #CME futures
    { 
      if(!is.null(myfuture$Last[x]) && !is.na(myfuture$Last[x]) && myfuture$Last[x] != "NA")
      {
        query <- paste("INSERT INTO futuresdata VALUES('",stockticker,"','",myfuture$Date[x],"',",myfuture$Last[x],",",myfuture$High[x],",",myfuture$Low[x],")",sep="")
        dbSendQuery(mydb,query)
      }
    } else { #ICE futures
      if(!is.null(myfuture$Settle[x]) && !is.na(myfuture$Settle[x]) && myfuture$Settle[x] != "NA")
      {
        query <- paste("INSERT INTO futuresdata VALUES('",stockticker,"','",myfuture$Date[x],"',",myfuture$Settle[x],",",myfuture$High[x],",",myfuture$Low[x],")",sep="")
        dbSendQuery(mydb,query)
      }
    }
    
  }
  
  #Close the database
  dbDisconnect((mydb))
}


#Loop on the futures
for(i in 1:nrow(stocktickervector))
{
  #Get the ticket
  stockticker <- stocktickervector[i,1] #Get the symbol
  print(stockticker) #For debugging
  
  #Get the futures
  #myfuture <- Quandl(paste("CHRIS/",stocktickervector[i,2],sep=""), api_key="zK6coAV1K5eyxuaPvWJm")
  #Connection settings and the dataset
  mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)
  rs = dbSendQuery(mydb, paste("SELECT P.* FROM futuresdata P WHERE P.FUTURE = '",stockticker,"'",sep=""))
  myfuture = fetch(rs, n=-1)
  
  #Find the end of months
  mymonth <- myfuture %>% 
    group_by(strftime(SNAPSHOTDATE, "%Y-%m")) %>% #Groups by the yearmonths
    filter(SNAPSHOTDATE == max(SNAPSHOTDATE)) %>% #Take the last date of each group
    .$SNAPSHOTDATE  
  
  #Filter the dataset to just end of month data. 
  myfuture <- myfuture[myfuture$SNAPSHOTDATE %in% mymonth,]
  myfuture$SNAPSHOTDATE <- as.Date(myfuture$SNAPSHOTDATE)
  
  #Filter the data to jan start
  myfuture <- myfuture[myfuture$SNAPSHOTDATE > as.Date("2000-12-31"),]
  myfuture <- myfuture[2:nrow(myfuture),] #Leave the current month out
  
  #Dummy vector
  b <- as.vector(0)
  
  #Loop on the rows
  for(i in 1:nrow(myfuture)-1)
  { b[i] <- myfuture$LAST[i+1] }
  
  #Add the last values as a column
  myfuture$PREVLAST <- c(b,NA)
  
  #Remove the last row with na
  myfuture <- myfuture[1:nrow(myfuture)-1,] 
  
  #Remove the last row with na
  myfuture$DIFF <- (myfuture[,3] - myfuture[,6])/myfuture[,6]*100
  
  #Add the months
  myfuture$MONTH <- month(myfuture$SNAPSHOTDATE)
  
  #Monthly mean
  pchange <- aggregate(myfuture$DIFF,list(myfuture$MONTH),mean)
  
  #Find the positive negative counts
  posnegcount <- myfuture %>% group_by(MONTH) %>%  
    summarize(pos = sum(DIFF>0), neg = sum(DIFF<0))
  
  #Insert the values to the database
  dbSendQuery(mydb,paste("INSERT INTO futuresseasonality VALUES ('",stockticker,"','",mydate,"','MEAN',",noquote(paste(format(round(pchange$x, 2), nsmall = 2),collapse=",")),")",sep=""))
  dbSendQuery(mydb,paste("INSERT INTO futuresseasonality VALUES ('",stockticker,"','",mydate,"','POS',",noquote(paste(format(round(posnegcount$pos, 2), nsmall = 2),collapse=",")),")",sep=""))
  dbSendQuery(mydb,paste("INSERT INTO futuresseasonality VALUES ('",stockticker,"','",mydate,"','NEG',",noquote(paste(format(round(posnegcount$neg, 2), nsmall = 2),collapse=",")),")",sep=""))
  
  #Close the database
  dbDisconnect((mydb))
  
}





