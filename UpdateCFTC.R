library(dplyr)
library(lubridate)
library(Quandl)
library(RMySQL)
library(xtable)
library(data.table)

#Set the server
myhost <- "cemoptions.cloudapp.net"
options(stringsAsFactors=F)

#Connection settings and truncate the table
mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)
dbSendQuery(mydb, "TRUNCATE TABLE futurescftc;")

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



#Loop on the futures
for(i in 1:nrow(stocktickervector))
{
  #Get the ticket
  stockticker <- stocktickervector[i,1] #Get the symbol
  print(stockticker) #For debugging
  
  #Get CME address
  stocktickervector[i,3] <- gsub('[0-9]+', '', (strsplit(stocktickervector[i,2],"_"))[[1]][2])
  
  #Exceptions
  if (stockticker == "BZ")
    stocktickervector[i,3] <- stockticker
  if (stockticker == "ZW")
    stocktickervector[i,3] <- "MW"
  
  #Get the futures
  myfuture <- Quandl(paste("CFTC/",stocktickervector[i,3],"_FO_ALL",sep=""), api_key="zK6coAV1K5eyxuaPvWJm")

  #Remove the extra dealer spread column if it is a currency
  if(startsWith(stockticker, "6"))
    myfuture <- subset(myfuture, select = -c(5) )
  
  #Loop on the rows
  for (j in 1:nrow(myfuture)){

    #Create the query
    query <- "INSERT INTO futurescftc VALUES ('AAA','BBB',CCC)"
    query <- gsub("AAA", stockticker, query)
    query <- gsub("BBB", as.character(myfuture[j,1]), query)
    query <- gsub("CCC", noquote(paste(myfuture[j,c(2:length(myfuture))],collapse=",")), query) 
    
    #Execute the query
    dbSendQuery(mydb, query)   
  }
  
}

#Close the database
dbDisconnect((mydb))
