# This is a test comment
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
  stocktickervector <- sort(c("CL","SI"))
}

for(stockticker in stocktickervector)
{
  #Processing now
  print(stockticker)
  
  #Connection settings and the dataset
  mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)
  rs = dbSendQuery(mydb, paste("CALL GetMonthlySD('",stockticker,"')",sep=""))
  stockdata = fetch(rs, n=-1)
  
}
