rm(list=ls(all=TRUE)) 
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
  stocktickervector <- sort(c("HE"))
}

#Loop on stock tickers
for(stockticker in stocktickervector)
{
  #Processing now
  print(stockticker)
  
  #Connection settings and the dataset
  mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)
  rs = dbSendQuery(mydb, paste("CALL GetMonthlySD('",stockticker,"')",sep=""))
  stockdata = fetch(rs, n=-1)

  #Set the parameters
  gap_down = 0
  gap_up=0

  #Create the resulting data frame
  results = data.frame(MONTH = NA, AVE_GAP_D =NA, STD_GAP_D = NA, AVE_GAP_U=NA, STD_GAP_U=NA)

  #This is the logic for two months. Buy in Jan, sell in Feb. 
  for (j in 1:12){
    for(i in 1:floor(dim(stockdata)[1]/12)-1 ){
      gap_down[i] = (stockdata$MCLOSE[i*12+j-2]-min(stockdata$MLOW[i*12+j-1],stockdata$MLOW[i*12+j]))/stockdata$MCLOSE[i*12+j-2]
      gap_up[i] = (-stockdata$MCLOSE[i*12+j-2]+max(stockdata$MHIGH[i*12+j-1],stockdata$MHIGH[i*12+j]))/stockdata$MCLOSE[i*12+j-2]
    }
    
    #Calculate the long mean/sd, short mean/sd
    temp_results = data.frame(MONTH = j, 
                              AVE_GAP_D =mean(gap_down[which(gap_down>0)]), 
                              STD_GAP_D = sd(gap_down[which(gap_down>0)]),
                              AVE_GAP_U =mean(gap_up[which(gap_up>0)]), 
                              STD_GAP_U = sd(gap_up[which(gap_up>0)])
    )
    #Add them to the resulting data frame
    results = rbind(temp_results, results)  
  }
  
  #Find the complete rows
  results <- results[complete.cases(results),]
  
  #Conversion factor between continuos future data and current data based on close of current vs close of continous.
  conversionfactor <- stockdata$CONVERSIONFACTOR[1];
  
  #The close of the month
  mclose <- tail(stockdata$MCLOSE,1)
  
  #Get the current month number
  currentmonth <- month(Sys.Date())
  
  #THIS SHOULD BE FEBRUARY 1SD GAP DOWN
  print(mclose*(1-results$AVE_GAP_D[currentmonth]-1*results$STD_GAP_D[currentmonth])*conversionfactor)
  
  #Example Usage
  print("For February entry prices are: Mean, Mean+1SD, Mean +2SD")
  print(stockdata$MCLOSE[192]*(1-results$AVE_GAP_D[11]-0*results$STD_GAP_D[11])*conversionfactor )
  print(stockdata$MCLOSE[192]*(1-results$AVE_GAP_D[11]-1*results$STD_GAP_D[11])*conversionfactor  )
  print(stockdata$MCLOSE[192]*(1-results$AVE_GAP_D[11]-2*results$STD_GAP_D[11])*conversionfactor )
  print("Short targets")
  print(stockdata$MCLOSE[192]*(1+results$AVE_GAP_U[11]+0*results$STD_GAP_U[11])*conversionfactor)
  print(stockdata$MCLOSE[192]*(1+results$AVE_GAP_U[11]+1*results$STD_GAP_U[11])*conversionfactor)
  print(stockdata$MCLOSE[192]*(1+results$AVE_GAP_U[11]+2*results$STD_GAP_U[11])*conversionfactor)  
  
  #Close the database
  dbDisconnect((mydb))
  
}

