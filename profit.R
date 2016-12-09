#Load the library
library(RMySQL)
library(xtable)
library(data.table)
myhost <- "cemoptions.cloudapp.net"
options(stringsAsFactors = FALSE)

futuresblob <- read.csv("futures.csv", header=FALSE)  # read csv file 
#stocktickervector <- sort(c("ZB","NG","ES","6J","6A","6B","CL","SB","6E","GC","SI"))
stocktickervector <- sort(as.vector(futuresblob[,1]))
#stocktickervector <- sort(c("CL"))
myperiod <- c(5,10)
time_window <- 89

#Result set
expected_benefit <<- data.frame(stockticker=character(), sd=numeric(),period=numeric(),
                                sell_suggested_price=numeric(),
                                buy_suggested_price=numeric(),sell_mean_profit=numeric(),buy_mean_profit=numeric(),sell_sd_profit=numeric(),buy_sd_profit=numeric(),sell_low_profit=numeric(),sell_high_profit=numeric(),buy_low_profit=numeric(),buy_high_profit=numeric(),sell_count=numeric(),buy_count=numeric())

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
  
  #Vector of lengths
  numberlines <- min(c(length(runmean_gap_prctg_long),length(runsd_gap_prctg_long),length(runmean_gap_prctg_short),length(runsd_gap_prctg_short)))
  
  #Create a data frame
  #strategy.data = head(data.frame(buy_price,sell_price),1)
  mov_mean_long <- head(runmean_gap_prctg_long,numberlines)
  mov_sd_long <- head(runsd_gap_prctg_long,numberlines)
  mov_mean_short <- head(runmean_gap_prctg_short,numberlines)
  mov_sd_short <- head(runsd_gap_prctg_short,numberlines)
  run_sd <- standarddeviation
  
  #Add the originals to the data frame
  strategy.data <- data.frame(mov_mean_long, mov_sd_long, mov_mean_short, mov_sd_short, run_sd, period, stringsAsFactors=FALSE)
  
  #Now add the additional fields
  strategy.data["pclose"] <- head(stockdata$PCLOSE,numberlines)
  strategy.data["lo"] <- head(stockdata$DAYLOW,numberlines)
  strategy.data["hi"] <- head(stockdata$DAYHIGH,numberlines)
  strategy.data["buy_price"] <- strategy.data$pclose*(1-strategy.data$mov_mean_long-strategy.data$mov_sd_long*strategy.data$run_sd)
  strategy.data["sell_price"] <- strategy.data$pclose*(1+strategy.data$mov_mean_short+strategy.data$mov_sd_short*strategy.data$run_sd)
  strategy.data["buy_actual"] <- 0
  strategy.data["sell_actual"] <- 0
  strategy.data["pprofit_buy"] <- -100
  strategy.data["pprofit_sell"] <- -100
  strategy.data["pprofit_buy_expected"] <- ""
  strategy.data["pprofit_sell_expected"] <- ""
  strategy.data["sell_count"] <- 0
  strategy.data["buy_count"] <- 0
  strategy.data["close"] <- head(stockdata$CLOSE,numberlines)
  
  
  #Return the data
  strategy.data
}

for(stockticker in stocktickervector)
{
  #Connection settings and the dataset
  mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)
  rs = dbSendQuery(mydb, paste("SELECT F.OPEN, F.LAST AS CLOSE, F.LOW AS DAYLOW, F.HIGH AS DAYHIGH, F.SNAPSHOTDATE, S.SETTLE, (SELECT P.LAST FROM futures P WHERE F.FUTURE = P.FUTURE AND F.SNAPSHOTDATE > P.SNAPSHOTDATE ORDER BY P.SNAPSHOTDATE DESC LIMIT 1) AS PCLOSE FROM futures F INNER JOIN futuresspot S ON F.FUTURE = S.FUTURE WHERE F.FUTURE = '",stockticker,"' ORDER BY F.SNAPSHOTDATE DESC",sep=""))
  stockdata = fetch(rs, n=-1)

  rs = dbSendQuery(mydb,paste("SELECT F.OPEN, F.LAST AS CLOSE, F.LOW AS DAYLOW, F.HIGH AS DAYHIGH, F.SNAPSHOTDATE, S.SETTLE, (SELECT P.LAST FROM futures P WHERE F.FUTURE = P.FUTURE AND F.SNAPSHOTDATE > P.SNAPSHOTDATE ORDER BY P.SNAPSHOTDATE DESC LIMIT 1) AS PCLOSE FROM futures F INNER JOIN futuresspot S ON F.FUTURE = S.FUTURE WHERE F.FUTURE = '",stockticker,"' ORDER BY F.SNAPSHOTDATE DESC LIMIT 1;",sep=""))
  stockconvert = fetch(rs, n=-1)
  
  #Conversion factor between continuos future data and current data based on close of current vs close of continous.
  conversionfactor <- stockconvert[1,6]/stockconvert[1,2]
    
  #Close the database
  dbDisconnect((mydb))
  
  # Loop on standard deviation
  for (sd in 1:2)
  {
    for(period in myperiod)
    {
      #Result set
      result <- data.frame(mov_mean_long=numeric(),mov_sd_long=numeric(),mov_mean_short=numeric(),mov_sd_short=numeric(),run_sd=numeric(),period=numeric(),close=numeric(),lo=numeric(),hi=numeric(),buy_price=numeric(),sell_price=numeric(),buy_actual=numeric(),sell_actual=numeric(),pprofit_buy=numeric(),pprofit_sell=numeric(),pprofit_buy_expected=character(),pprofit_sell_expected=character(),sell_count=numeric(),buy_count=numeric(),close=numeric())
      
      #Call the function
      result <- rbind(result,futures(time_window,sd,stockdata,period))
      
      #Set the ones we sold or bought
      result[result$hi > result$sell_price,13] <- 1
      result[result$lo < result$buy_price,12] <- 1
      
      #Loop through the results to complete the transactio, if bought time to sell, if sold, time to buy.
      #We check the close of the next days
      for(i in 1:nrow(result))
      {
        if(result[i,13] > 0) #Buy low, sell high case
        {
          if (i>period)
          {
            result[i,13] <- min(result[(i-period+1):(i-1),20])
          } else {
            result[i,13] <- min(result[1:(i-1),20])
          }
          result["sell_count"] <- result["sell_count"] + 1
        } #Set the max of period
        
        if(result[i,12] > 0) #Sell high, buy low case
        {
          if(i>period)
          {
            result[i,12] <- max(result[(i-period+1):(i-1),20])
          } else {
            result[i,12] <- max(result[1:(i-1),20])
          }
          result["buy_count"] <- result["buy_count"] + 1
        } #Set the min of period
      }
      
      #Calculate the percent profit for each
      for(i in 1:nrow(result))
      {
        #print(result[i,13])
        if(!is.na(result[i,13]) &&  result[i,13] > 0 ) #Buy low, sell high case
        {
          result[i,15] <- (result[i,11] - result[i,13])/result[i,11]
        }
        if(!is.na(result[i,12]) && result[i,12] > 0) #Sell high, buy low case
        {
          result[i,14] <- (result[i,12] - result[i,10])/result[i,10] #Set the min of period
        }
      }
      
      sell_count <- max(result["sell_count"])
      buy_count <- max(result["buy_count"])
      
      #Find the means of values
      sell_mean_profit <- mean(result[result$pprofit_sell != -100,15])
      buy_mean_profit <- mean(result[result$pprofit_buy != -100,14])
      
      #Find sd of profits
      sell_sd_profit <- sd(result[result$pprofit_sell != -100,15])
      if(is.na(sell_sd_profit)) #if the number of observations is 1, sd fails
        sell_sd_profit <- 0
      
      buy_sd_profit <- sd(result[result$pprofit_buy != -100,14])
      if(is.na(buy_sd_profit)) #if the number of observations is 1, sd fails
        buy_sd_profit <- 0
      
      
      #set the ranges
      sell_low_profit <- signif((sell_mean_profit-sell_sd_profit)*100,digits=3)
      sell_high_profit <- signif((sell_mean_profit+sell_sd_profit)*100,digits=3)
      
      buy_low_profit <- round(signif((buy_mean_profit-buy_sd_profit)*100,digits=3),digits=3)
      buy_high_profit <- signif((buy_mean_profit+buy_sd_profit)*100,digits=3)
      
      #Find the suggested sell price
      sell_suggested_price <- result[1,11]*(1-sell_mean_profit)*conversionfactor
      buy_suggested_price <- result[1,10]*(1+buy_mean_profit)*conversionfactor
      
      
      #Result set
      expected_benefit <<- rbind(expected_benefit,data.frame(stockticker,sd,period,
                                                             sell_suggested_price,
                                                             buy_suggested_price,
                                                             sell_mean_profit,buy_mean_profit,sell_sd_profit,buy_sd_profit,sell_low_profit,sell_high_profit,buy_low_profit,buy_high_profit,sell_count,buy_count))
      
      
      #Write the debug file to csv
      #write.csv(result, file = paste("CLsd",sd,"period",period,".csv",sep=""), row.names = TRUE)
      
    }
  }
}

#Delete row when sell count and buy count are zero
expected_benefit <- expected_benefit[!(expected_benefit$sell_count==0 & expected_benefit$buy_count==0),]

#Print out the data set
#finaldt <- as.data.table(expected_benefit[,c(1:5,8:13)])
finaldt <- as.data.table(expected_benefit[,c(1:5)])
print(xtable(as.data.frame.matrix(finaldt),digits=c(0,0,0,4,4)), type='html', file="emailcontent_profit.html")