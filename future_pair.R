#Load the library
library(RMySQL)
library(xtable)
library(data.table)
library(reshape)
library(ggplot2)
options(warn=-1)
myhost <- "cemoptions.cloudapp.net"
options(stringsAsFactors = FALSE)

#Parameters
time_window <- 89

#Periods
myperiod <- c(1,5,10)

#Check if the file exists, it not use a constant. 
if(file.exists("/home/cem/portakal/futures_pair.txt"))
{
  futuresblob <- read.csv("/home/cem/portakal/futures_pair.txt", header=FALSE)  # read csv file 
} else
{
  V0 <- as.vector(unlist(strsplit("ESZB,ES|ZB,ES+10*ZB,1ES|1ZB,CME|CME,2",",",fixed=TRUE)))
  V1 <- V0[1]
  V2 <- V0[2]
  V3 <- V0[3]
  futuresblob <- data.frame(V1,V2,V3)
}

futures <- function(time_window,standarddeviation,stockdata,period)
{
  
  # For our long equity strategy, we need "close" and "lo" columns from our dataframe.
  open <- stockdata$OPEN
  close <- stockdata$CLOSE
  lo <- stockdata$DAYLOW
  hi <- stockdata$DAYHIGH
  date <- stockdata$SNAPSHOTDATE
  pclose <- stockdata$PCLOSE
  time <- nrow(stockdata)
  gap_percentage_long = NULL
  gap_percentage_short = NULL
  
  
  # Moving SD
  movsd <- function(series,lag)
  {
    msd <- vector(mode="numeric")
    for (i in lag:length(series))
    {
      msd[i] <- sd(series[(i-lag+1):i])
    }
    msd
  }
  
  # Moving mean
  movmean <- function(series,lag)
  {
    msd <- vector(mode="numeric")
    for (i in lag:length(series))
    {
      msd[i] <- mean(series[(i-lag+1):i])
    }
    msd
  }
  
  #Implement the stats
  for (i in period:time ) {
    gap_percentage_long[i] = (close[i+1] - min(lo[(i-period+1):i]))/close[i+1]
    gap_percentage_short[i] = (max(hi[(i-period+1):i]) - close[i+1])/close[i+1]
  }
  
  #Remove less than zero percent gaps
  gap_percentage_long_pos = gap_percentage_long[gap_percentage_long>0]
  gap_percentage_short_pos = gap_percentage_short[gap_percentage_short>0]
  
  #Remove the NA, last record
  gap_percentage_long_pos = gap_percentage_long_pos[!is.na(gap_percentage_long_pos)]
  gap_percentage_short_pos = gap_percentage_short_pos[!is.na(gap_percentage_short_pos)]
  
  #Calculate gap percentages
  runmean_gap_prctg_long = movmean(rev(gap_percentage_long_pos), time_window)
  runsd_gap_prctg_long = movsd(rev(gap_percentage_long_pos), time_window)
  
  runmean_gap_prctg_short = movmean(rev(gap_percentage_short_pos), time_window)
  runsd_gap_prctg_short = movsd(rev(gap_percentage_short_pos), time_window)
  
  #Remove the NA values, last n records based on time window
  runmean_gap_prctg_long = runmean_gap_prctg_long[!is.na(runmean_gap_prctg_long)]
  runsd_gap_prctg_long = runsd_gap_prctg_long[!is.na(runsd_gap_prctg_long)]
  
  runmean_gap_prctg_short = runmean_gap_prctg_short[!is.na(runmean_gap_prctg_short)]
  runsd_gap_prctg_short = runsd_gap_prctg_short[!is.na(runsd_gap_prctg_short)]
  
  #Reverse the order
  runmean_gap_prctg_short <- rev(runmean_gap_prctg_short)
  runsd_gap_prctg_short <- rev(runsd_gap_prctg_short)
  
  runmean_gap_prctg_long <- rev(runmean_gap_prctg_long)
  runsd_gap_prctg_long <- rev(runsd_gap_prctg_long)
  
  #Find the buy and sell prices
  buy_price = close*(1-rev(runmean_gap_prctg_long)-standarddeviation*rev(runsd_gap_prctg_long))
  sell_price =  close*(1+rev(runmean_gap_prctg_short)+standarddeviation*rev(runsd_gap_prctg_short))
  
  #Create a data frame
  #strategy.data = head(data.frame(buy_price,sell_price),1)
  mov_mean_long <- head(runmean_gap_prctg_long,1)
  mov_sd_long <- head(runsd_gap_prctg_long,1)
  mov_mean_short <- head(runmean_gap_prctg_short,1)
  mov_sd_short <- head(runsd_gap_prctg_short,1)
  run_sd <- standarddeviation
  
  strategy.data <- data.frame(mov_mean_long, mov_sd_long, mov_mean_short, mov_sd_short, run_sd, period, stringsAsFactors=FALSE)
  
  #Return the data
  strategy.data
}

getRawData <- function(stockticker)
{
  mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)
  rs = dbSendQuery(mydb, paste("SELECT F.OPEN, F.LAST AS CLOSE, F.LOW AS DAYLOW, F.HIGH AS DAYHIGH, F.SNAPSHOTDATE, S.SETTLE, (SELECT P.LAST FROM futures P WHERE F.FUTURE = P.FUTURE AND F.SNAPSHOTDATE > P.SNAPSHOTDATE ORDER BY P.SNAPSHOTDATE DESC LIMIT 1) AS PCLOSE FROM futures F INNER JOIN futuresspot S ON F.FUTURE = S.FUTURE WHERE F.FUTURE = '",stockticker,"' ORDER BY F.SNAPSHOTDATE DESC",sep=""))
  stockdata = fetch(rs, n=-1)
  
  #Close the database
  dbDisconnect((mydb))
  
  #Return
  stockdata
}

getDataset <- function(stocks,stocksformula)
{
  #Set the values
  stocks <- as.vector(unlist(strsplit(stocks,'|',fixed=TRUE)))
  #stocksformula <- as.vector(unlist(strsplit(stockstring,"|",fixed=TRUE)))[2]
  
  #Find the length of data matrix
  mlength <- c()
  
  #Store the conversion factor
  mconversionfactor <- c()
  #Loop through the futures
  for(i in 1:length(stocks)){
    tempdata <- getRawData(stocks[i]) #Assign the data frame to temp, will be used to retreive snapshot dates
    mlength <- c(mlength,nrow(tempdata)) #Capture the length of the data frame
    assign(stocks[i],data.matrix(tempdata),envir = .GlobalEnv)  #Dynamic assignment of a future
  }  
  
  #Resize the data matrix to be the minimum
  for(i in 1:length(stocks)){
    assign(stocks[i] ,eval(parse(text=stocks[i]))[1:min(mlength),],envir = .GlobalEnv)
  }  
  
  #Resize the temp data as well
  tempdata <- tempdata[1:min(mlength),]
  
  #Evaluate the formula and return a result
  stockdata <- as.data.frame(eval(parse(text=stocksformula)))
  stockdata$SNAPSHOTDATE <- tempdata$SNAPSHOTDATE
  
  stockdata
}


for(i in 1:nrow(futuresblob))
{
  
  #Get data
  stockdata <- getDataset(futuresblob[i,2],futuresblob[i,3])
  future <- futuresblob[i,1]
  
  #Result set
  result <- data.frame(mov_mean_long=numeric(),mov_sd_long=numeric(),mov_mean_short=numeric(),mov_sd_short=numeric(),run_sd=numeric(),period=numeric())
  
  #Conversion factor between continuos future data and current data based on close of current vs close of continous.
  conversionfactor <- stockdata[1,6]/stockdata[1,2]
  
  # Loop on standard deviation
  for (i in 0:2)
  {
    for(period in myperiod)
    {
      #Call the function
      result <- rbind(result,futures(time_window,i,stockdata,period))
    }
  }
  
  #Convert the numbers to actuals
  result$pclose <- stockdata[1,7]*conversionfactor
  result$close <- stockdata[1,6]
  result$future <- future
  result$buy <- result$close*(1-result$mov_mean_long-result$mov_sd_long*result$run_sd)
  result$sell <- result$close*(1+result$mov_mean_short+result$mov_sd_short*result$run_sd)
  
  #Create the final results table
  if(!exists("finalresulttable"))
  {
    finalresulttable <- result
  } else {
    finalresulttable <- rbind(finalresulttable,result)
  }
  
}

#Result table
resulttable <- finalresulttable[finalresulttable$run_sd == 0,c(9,8,6,10:11)]
resulttable$buy1 <- finalresulttable[finalresulttable$run_sd == 1,10]
resulttable$sell1 <- finalresulttable[finalresulttable$run_sd == 1,11]
resulttable$buy2 <- finalresulttable[finalresulttable$run_sd == 2,10]
resulttable$sell2 <- finalresulttable[finalresulttable$run_sd == 2,11]

#Rename the columns
names(resulttable) <- c("FUTURE","CLOSE","PERIOD","BUY","SELL","BUY1","SELL1","BUY2","SELL2")

#Connection settings and the dataset
mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)

#Add the date column to the final result table
resulttable$SNAPSHOTDATE <- as.character(Sys.Date())

#Delete already existing records for pairs only. 
query <- paste("DELETE FROM futurespredict WHERE LENGTH(FUTURE) > 2 AND SNAPSHOTDATE = '",as.character(Sys.Date()),"'",sep="",collapse = ',')
dbSendQuery(mydb,query)

#Loop through the data frame
for (i in 1:nrow(resulttable) ) {
  
  #dbWriteTable(mydb, value = finalresulttable, name = "futurespredict" , overwrite=FALSE, append = TRUE, row.names = NA )
  val0 <- paste("'",resulttable[i,1],"',",sep="",collapse = '')
  val1 <- noquote(paste(resulttable[i,c(2,4:9)],collapse = ','))
  query <- paste("INSERT INTO futurespredict VALUES(",noquote(paste(val0,val1,paste(",'",resulttable[i,10],"'",sep="",collapse = ''),",",resulttable[i,3],sep="",collapse = ',')),")",sep="",collapse = ',')
  
  dbSendQuery(mydb,query)
}

#Close the database
dbDisconnect((mydb))

#Create a table
finaldt <- as.data.table(resulttable)
print(xtable(as.data.frame.matrix(finaldt),digits=c(0,1,4,0,4,4,4,4,4,4,4)), type='html', file="/home/cem/emailcontent_pair.html")
