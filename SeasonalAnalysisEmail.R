library(dplyr)
library(lubridate)
library(Quandl)
library(RMySQL)
library(xtable)
library(data.table)

#Set the server
myhost <- "cemoptions.cloudapp.net"
options(stringsAsFactors=F)

#Set the date for the future
mydate <- format(Sys.time(), "%Y-%m-%d %H:%M:%d")

#Result set
result <- data.frame(Col1=character(),Col2=character(),Col3=character(),Col4=character(),Col5=character(),Col6=character(),Col7=character(),Col8=character(),Col9=character(),Col10=character(),Col11=character(),Col12=character(),Col13=character(),Col14=character(),Col15=character(),Col16=character(),Col17=character(),Col18=character(),Col19=character(), stringsAsFactors=FALSE)

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


#Write to the database
for(i in 1:nrow(stocktickervector)) 
{
  #Connection settings and the dataset
  mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)
  
  #Get the ticker
  stockticker <- stocktickervector[i,1] #Get the symbol

  #Get the three lines per future 
  rs = dbSendQuery(mydb, paste("select * FROM futuresseasonality WHERE FUTURE = '",stockticker,"' AND SNAPSHOTDATE = (SELECT MAX(SNAPSHOTDATE) FROM futuresseasonality WHERE FUTURE = '",stockticker,"')",sep=""))
  myfuture = fetch(rs, n=-1)
  
  #Print the ticket
  print(stockticker)

  #Create the vector
  my <- c(myfuture[1,1]) #Future symbol
  
  #Create the months
  for(i in 4:15)
  {
    if (myfuture[1,i] > 0)
      my <- c(my,paste(myfuture[1,i]," (",myfuture[2,i],"/",myfuture[3,i]+myfuture[2,i],")",sep=""))
    else 
      my <- c(my,paste(myfuture[1,i]," (",myfuture[3,i],"/",myfuture[3,i]+myfuture[2,i],")",sep=""))
  }
  
  #Add the snapshotdate
  my <- c(my,as.character(as.Date(myfuture[1,2]))) #Snapshotdate
  
  #Get the monthly notification figures
  query <- "SELECT BUY05 AS BUY1, BUY1 AS BUY2, SELL05 AS SELL1, SELL1 AS SELL2 FROM futurespredict WHERE future = 'CCC' AND PERIOD = 30 AND SNAPSHOTDATE = 'DDD'"
  query <- gsub("CCC", stockticker, query)
  query <- gsub("DDD", as.Date(myfuture[1,2]), query)
  rs2 = dbSendQuery(mydb, query)
  myfuture2 = fetch(rs2, n=-1)
  
  #The currency conversion
  if(head(strsplit(stockticker,'')[[1]],1) == 6)
    myfuture2 <- 100*myfuture2
  
  #Add the buy sell figures from the monthly notification figures
  my <- c(my,myfuture2)
  
  #Let's work on the COT report
  query <- "SELECT GROUP_CONCAT(FLOOR(PERCENTILERANKLARGESPEC) SEPARATOR ', ') AS PERCENTILERANKLARGESPEC FROM (SELECT PERCENTILERANKLARGESPEC FROM futurescftc WHERE future = 'CCC' ORDER BY SNAPSHOTDATE DESC LIMIT 3) A;"
  query <- gsub("CCC", stockticker, query)
  rs2 = dbSendQuery(mydb, query)
  myfuture2 = fetch(rs2, n=-1)
  
  #Add the buy sell figures from the monthly notification figures
  my <- c(my,myfuture2$PERCENTILERANKLARGESPEC[1])
  
  #Rename the columns
  names(result) <- c("FUTURE","JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC","SNAPSHOTDATE","BUY1","BUY2","SELL1","SELL2","PRNKLRG")
  names(my) <- c("FUTURE","JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC","SNAPSHOTDATE","BUY1","BUY2","SELL1","SELL2","PRNKLRG")
  
  #Add the vector to results data frame
  result <- rbind(result,my)
  
  #Close the database
  dbDisconnect((mydb))
  
}

#Rename the columns
names(result) <- c("FUTURE","JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC","SNAPSHOTDATE","BUY1","BUY2","SELL1","SELL2","PRNKLRG")

#Find the current three months
currentmonth <- month(myfuture[1,2]) ##Current month
currentmonth1 <- ((currentmonth+1) %% 12)+1 
currentmonth2 <- ((currentmonth+2) %% 12)+1
currentmonth3 <- ((currentmonth+3) %% 12)+1

#Create a table
finaldt <- as.data.table(result[,c(1,currentmonth+1,currentmonth1,currentmonth2,currentmonth3,15:19,14)])
#print(xtable(as.data.frame.matrix(finaldt),digits=c(4,4,4,4,4,4,4,4,4,4,4,4,4,4,4)), type="html", file="/home/cem/emailcontent_seasonality.html")
print(xtable(as.data.frame.matrix(finaldt)), type="html", file="/home/cem/emailcontent_seasonality.html")


