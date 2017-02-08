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
  stocktickervector <- sort(c("HE"))
}

#Create the resulting data frame
resultstable <- data.frame(FUTURE=NA,BUY1=0.0,BUY2=0.0,SELL1=0.0,SELL2=0.0)

#Loop on stock tickers
for(stockticker in stocktickervector)
{
  #Processing now
  print(stockticker)
  
  #Connection settings and the dataset
  mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)
  rs = dbSendQuery(mydb, paste("CALL GetWeeklySD('",stockticker,"')",sep=""))
  stockdata = fetch(rs, n=-1)
  
  #We have missing values in CLOSE prices. Let us replace these NA values by the arithmetical mean of HIGH and LOW
  for(i in 1:dim(stockdata)[1] ){
    if(is.na(stockdata$MCLOSE[i])){
      stockdata$MCLOSE[i] = (stockdata$MLOW[i]+stockdata$MHIGH[i])/2
    }
  }

  
  #Create the resulting data frame for weekly percent changes
  results = data.frame(WEEK = NA, NUMBER_UP =NA, NUMBER_DOWN = NA, UP = NA, DOWN = NA)
  
  #Loop through different weeks
  for (j in 1:49){
    week_current = subset(stockdata, WK %% 51 == j)
    week_next = subset(stockdata, WK %% 51 == (j+1))
    change = (week_next$MCLOSE - week_current$MCLOSE)/week_current$MCLOSE
    temp_results =  data.frame(WEEK = j+1, NUMBER_UP = sum(change>0), NUMBER_DOWN = sum(change<0), UP = mean(change[which(change>0)]), DOWN = mean(change[which(change<0)])  )
    results = rbind(temp_results, results) 
  }
 
  #Find the complete rows
  results <- results[complete.cases(results),]
  
  
  
  #Close the database
  dbDisconnect((mydb))
  
}

#Create the table for output
#resultstable <- resultstable[complete.cases(resultstable),]
#finaldt <- as.data.table(resultstable)
#print(xtable(as.data.frame.matrix(finaldt)), type='html', file="/home/cem/emailcontent_seasonality_monthly.html")