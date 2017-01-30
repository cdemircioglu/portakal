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
result <- data.frame(Col1=character(),Col2=character(),Col3=character(),Col4=character(),Col5=character(),Col6=character(),Col7=character(),Col8=character(),Col9=character(),Col10=character(),Col11=character(),Col12=character(),Col13=character(),Col14=character(), stringsAsFactors=FALSE)

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
  
  #Add the vector to results data frame
  result <- rbind(result,my)
  
  #Close the database
  dbDisconnect((mydb))
  
}

#Rename the columns
names(result) <- c("FUTURE","JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC","SNAPSHOTDATE")

#Create a table
finaldt <- as.data.table(result)
print(xtable(as.data.frame.matrix(finaldt),digits=c(4,4,4,4,4,4,4,4,4,4,4,4,4,4,4)), type="html", file="/home/cem/emailcontent_seasonality.html")

