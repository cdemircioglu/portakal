#Load the library
library(RMySQL)
library(xtable)
library(data.table)
library(reshape)
library(ggplot2)
library(TTR)
options(warn=-1)
myhost <- "cemoptions.cloudapp.net"
options(stringsAsFactors = FALSE)


ConvertZB32 <- function(price)
{
  wholenumber <- floor(price)
  fraction <- ((price - wholenumber)*0.32) + wholenumber
  formatC(fraction,digits=2, format="f")
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

#Parameters
time_window <- 89

#Check if the file exists, it not use a constant. 
if(file.exists("/home/cem/portakal/futures.csv"))
{
  futuresblob <- read.csv("/home/cem/portakal/futures.csv", header=FALSE)  # read csv file 
  stocktickervector <- sort(as.vector(futuresblob[,1]))
} else
{
  stocktickervector <- sort(c("ZB","NG","ES","6J","6A","6B","CL","SB","6E","GC","SI"))
  #stocktickervector <- sort(c("NG"))
}

myperiod <- c(1,5,10)



for(stockticker in stocktickervector)
{
  print(stockticker)
  #Connection settings and the dataset
  mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)
  rs = dbSendQuery(mydb, paste("SELECT F.OPEN, F.LAST AS CLOSE, F.LOW AS DAYLOW, F.HIGH AS DAYHIGH, F.SNAPSHOTDATE, S.SETTLE, (SELECT P.LAST FROM futures P WHERE F.FUTURE = P.FUTURE AND F.SNAPSHOTDATE > P.SNAPSHOTDATE ORDER BY P.SNAPSHOTDATE DESC LIMIT 1) AS PCLOSE FROM futures F INNER JOIN futuresspot S ON F.FUTURE = S.FUTURE WHERE F.FUTURE = '",stockticker,"' ORDER BY F.SNAPSHOTDATE DESC",sep=""))
  stockdata = fetch(rs, n=-1)
  
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
  
  #Close the database
  dbDisconnect((mydb))
  
  #Convert the numbers to actuals
  result$pclose <- stockdata[1,7]*conversionfactor
  result$close <- stockdata[1,6]
  result$future <- stockticker
  result$buy <- result$close*(1-result$mov_mean_long-result$mov_sd_long*result$run_sd)
  result$sell <- result$close*(1+result$mov_mean_short+result$mov_sd_short*result$run_sd)
  result$rsi <- format(round(rev(RSI(rev(stockdata$CLOSE), n=14))[1], 0), nsmall = 0)
  
  #Create the final results table
  if(!exists("finalresulttable"))
  {
    finalresulttable <- result
  } else {
    finalresulttable <- rbind(finalresulttable,result)
  }
  
  #Update the ZB to ticks
  finalresulttable[finalresulttable$future == "ZB",11] <- as.numeric(ConvertZB32(finalresulttable[finalresulttable$future == "ZB",11])) #Sell
  finalresulttable[finalresulttable$future == "ZB",10] <- as.numeric(ConvertZB32(finalresulttable[finalresulttable$future == "ZB",10])) #Buy
  finalresulttable[finalresulttable$future == "ZB",8] <- as.numeric(ConvertZB32(finalresulttable[finalresulttable$future == "ZB",8])) #Close
  
}




#Result table
resulttable <- finalresulttable[finalresulttable$run_sd == 0,c(9,8,6,10:11)]
resulttable$buy1 <- finalresulttable[finalresulttable$run_sd == 1,10]
resulttable$sell1 <- finalresulttable[finalresulttable$run_sd == 1,11]
resulttable$buy2 <- finalresulttable[finalresulttable$run_sd == 2,10]
resulttable$sell2 <- finalresulttable[finalresulttable$run_sd == 2,11]
resulttable$rsi <- finalresulttable[finalresulttable$run_sd == 2,12]

#Rename the columns
names(resulttable) <- c("FUTURE","CLOSE","PERIOD","BUY","SELL","BUY1","SELL1","BUY2","SELL2","RSI")

#Connection settings and the dataset
mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)

#Add the date column to the final result table
resulttable$SNAPSHOTDATE <- as.character(Sys.Date())

#Delete already existing current day data
query <- paste("DELETE FROM futurespredict WHERE SNAPSHOTDATE = '",as.character(Sys.Date()),"'",sep="")
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

#Update for 6J
resulttable$CLOSE[resulttable$FUTURE == '6J'] <- resulttable$CLOSE[resulttable$FUTURE == '6J']*100
resulttable$BUY[resulttable$FUTURE == '6J'] <- resulttable$BUY[resulttable$FUTURE == '6J']*100
resulttable$SELL[resulttable$FUTURE == '6J'] <- resulttable$SELL[resulttable$FUTURE == '6J']*100
resulttable$BUY1[resulttable$FUTURE == '6J'] <- resulttable$BUY1[resulttable$FUTURE == '6J']*100
resulttable$SELL1[resulttable$FUTURE == '6J'] <- resulttable$SELL1[resulttable$FUTURE == '6J']*100
resulttable$BUY2[resulttable$FUTURE == '6J'] <- resulttable$BUY2[resulttable$FUTURE == '6J']*100
resulttable$SELL2[resulttable$FUTURE == '6J'] <- resulttable$SELL2[resulttable$FUTURE == '6J']*100


#Create a table
finaldt <- as.data.table(resulttable)
print(xtable(as.data.frame.matrix(finaldt),digits=c(0,1,4,0,4,4,4,4,4,4,4,2)), type='html', file="/home/cem/emailcontent.html")
