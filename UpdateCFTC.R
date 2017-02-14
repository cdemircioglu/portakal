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

#Rank function
perc.rank <- function(x, xo)  length(x[x <= xo])/length(x)*100

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
  if(startsWith(stockticker, "6") || stockticker == "DX" || stockticker == "ES" || stockticker == "NQ" || stockticker == "YM" || stockticker == "ZB"|| stockticker == "TF")
    myfuture <- subset(myfuture, select = -c(5) )
  
  #Initialize the vector
  vlength <- 30
  prank <- vector('numeric', length=vlength)
  
  #Calculate the percent rank
  for (p in 1:vlength)
  {
    prankvector <- (myfuture[(1+p-1):(320+p-1),8]-myfuture[(1+p-1):(320+p-1),9]+myfuture[(1+p-1):(320+p-1),11]-myfuture[(1+p-1):(320+p-1),12])/myfuture[(1+p-1):(320+p-1),2]
    prank[p] <- perc.rank(prankvector,prankvector[1])
  }
  
  #Loop on the rows
  for (j in 1:nrow(myfuture)){

    #Create the query
    query <- "INSERT INTO futurescftc VALUES ('AAA','BBB',CCC,DDD)"
    query <- gsub("AAA", stockticker, query)
    query <- gsub("BBB", as.character(myfuture[j,1]), query)
    query <- gsub("CCC", noquote(paste(myfuture[j,c(2:length(myfuture))],collapse=",")), query) 
    
    #Calculate the prank value
    if(j<vlength)
      query <- gsub("DDD", 
                    format(round(as.numeric(prank[j]), 2), nsmall = 2)
                    ,query) 
    else
      query <- gsub("DDD", 0,query) 
    
    #Execute the query
    dbSendQuery(mydb, query)   
  }
  
}

#Close the database
dbDisconnect((mydb))
