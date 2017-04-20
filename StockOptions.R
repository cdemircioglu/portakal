library(dplyr)
library(lubridate)
library(Quandl)
library(RMySQL)
library(xtable)
library(data.table)
library(quantmod) 

#This script is to capture stock options data. It runs daily. It pull a handfull of stock options. 

#Set the server
myhost <- "cemoptions.cloudapp.net"
options(stringsAsFactors=F)

#Connection settings and the dataset
mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)

#Set the date for the future
mydate <- format(Sys.time(), "%Y-%m-%d %H:%M:%d")

#Delete the current date
dbSendQuery(mydb, paste("DELETE FROM stockoption WHERE SNAPSHOTDATE ='",format(Sys.time(), "%Y-%m-%d"),"'",sep=''))

##This script is to download stock options data. 
stocktickervector <- c("QQQ","SPY","XLE","XLF","TSLA","AAPL","AMZN","NFLX")

#Loop on stock tickers
for(stockticker in stocktickervector)
{
    optionchain <- getOptionChain(stockticker,NULL) 
    STOCKPRICE <- getQuote(stockticker)$Last #Last stock price
    STRIKELEVEL <- 0 #Assume everything is at the money
    
    
    for (optionlist in optionchain)
    {
      
      for (option in optionlist)
      {
      
          for (p in 1:nrow(option))
          {
            STOCKOPTIONTYPE <- substr(gsub(stockticker,"20",rownames(option)[p]),9,9) #Put or Call
            STRIKE <- option$Strike[p] #Strike
            if (STOCKOPTIONTYPE == 'C') { 
              if (STRIKE<=STOCKPRICE) { 
                STRIKELEVELCALL <- p
              }
            } 
            if (STOCKOPTIONTYPE == 'P') { 
              if (STRIKE<=STOCKPRICE) { 
                STRIKELEVELPUT <- p
              }
            }
          }
          
          for (p in 1:nrow(option))
          {
            STOCK <- stockticker #Stock name
            STOCKPRICE #Stock price
            STOCKOPTIONTYPE <- substr(gsub(stockticker,"20",rownames(option)[p]),9,9) #Put or Call
            PRICE <- option$Last[p] #Last price
            BID <- option$Bid[p] #Bid price
            ASK <- option$Ask[p] #Ask price
            VOLUME <- option$Vol[p] #Volume
            OPENINT <- option$OI[p] #Open interest
            STRIKE <- option$Strike[p] #Strike
            EXPIRY <- substr(gsub(stockticker,"20",rownames(option)[p]),1,8) #Expiration
            SNAPSHOTDATE <- today()  #Snapshotdate
              if (STOCKOPTIONTYPE == 'C') { 
                  STRIKEDISTANCE <- abs(STRIKELEVELCALL - p)
              } else {
                  STRIKEDISTANCE <- abs(p - STRIKELEVELPUT)
              }
            
            #Query string
            query <- "INSERT INTO stockoption VALUES ('AAA','BBB','CCC','DDD','EEE','FFF','GGG','HHH','III','JJJ','KKK','LLL')"
            query <- gsub("AAA",STOCK,query)
            query <- gsub("'BBB'",STOCKPRICE,query)
            query <- gsub("CCC",STOCKOPTIONTYPE,query)
            query <- gsub("'DDD'",PRICE,query)
            query <- gsub("'EEE'",BID,query)
            query <- gsub("'FFF'",ASK,query)
            query <- gsub("'GGG'",VOLUME,query)
            query <- gsub("'HHH'",OPENINT,query)
            query <- gsub("'III'",STRIKE,query)
            query <- gsub("JJJ",EXPIRY,query)
            query <- gsub("KKK",STRIKEDISTANCE,query)
            query <- gsub("LLL",SNAPSHOTDATE,query)
            
            #Insert the option
            dbSendQuery(mydb, query)
          }
        }
    } 
    
}

#Close the database
dbDisconnect((mydb))




