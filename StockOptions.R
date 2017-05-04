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

##Define the option chain2 
`getOptionChain2` <-
  function(Symbols, Exp=NULL, src="yahoo", ...) {
    Call <- paste("getOptionChain",src,sep=".")
    if(missing(Exp)) {
      do.call(Call, list(Symbols=Symbols, ...))
    } else {
      do.call(Call, list(Symbols=Symbols, Exp=Exp, ...))
    }
  }

getOptionChain.yahoo <- function(Symbols, Exp, ...)
{
  if(!requireNamespace("jsonlite", quietly=TRUE))
    stop("package:",dQuote("jsonlite"),"cannot be loaded.")
  
  NewToOld <- function(x) {
    if(is.null(x))
      return(x)
    # clean up colnames, in case there's weirdness in the JSON
    names(x) <- tolower(gsub("[[:space:]]", "", names(x)))
    # set cleaned up colnames to current output colnames
    d <- with(x, data.frame(Strike=strike, Last=lastprice, Chg=change,
                            Bid=bid, Ask=ask, Vol=volume, OI=openinterest, IV=impliedvolatility,
                            row.names=contractsymbol, stringsAsFactors=FALSE))
    # remove commas from the numeric data
    d[] <- lapply(d, gsub, pattern=",", replacement="", fixed=TRUE)
    d[] <- lapply(d, type.convert, as.is=TRUE)
    d
  }
  
  # Don't check the expiry date if we're looping over dates we just scraped
  checkExp <- !hasArg(".expiry.known") || !match.call(expand.dots=TRUE)$.expiry.known
  # Construct URL
  urlExp <- paste0("https://query2.finance.yahoo.com/v7/finance/options/", Symbols[1])
  # Add expiry date to URL
  if(!checkExp)
    urlExp <- paste0(urlExp, "?&date=", Exp)

  # Fetch data (jsonlite::fromJSON will handle connection)
  tbl <- jsonlite::fromJSON(urlExp)
  
  
  # Only return nearest expiry (default served by Yahoo Finance), unless the user specified Exp
  if(!missing(Exp) && checkExp) {
    all.expiries <- tbl$optionChain$result$expirationDates[[1]]
    all.expiries.posix <- .POSIXct(as.numeric(all.expiries), tz="UTC")
    
    if(is.null(Exp)) {
      # Return all expiries if Exp = NULL
      out <- lapply(all.expiries, getOptionChain.yahoo, Symbols=Symbols, .expiry.known=TRUE)
      # Expiry format was "%b %Y", but that's not unique with weeklies. Change
      # format to "%b.%d.%Y" ("%Y-%m-%d wouldn't be good, since names should
      # start with a letter or dot--naming things is hard).
      return(setNames(out, format(all.expiries.posix, "%b.%d.%Y")))
    } else {
      # Ensure data exist for user-provided expiry date(s)
      if(inherits(Exp, "Date"))
        valid.expiries <- as.Date(all.expiries.posix) %in% Exp
      else if(inherits(Exp, "POSIXt"))
        valid.expiries <- all.expiries.posix %in% Exp
      else if(is.character(Exp)) {
        expiry.range <- range(unlist(lapply(Exp, .parseISO8601, tz="UTC")))
        valid.expiries <- all.expiries.posix >= expiry.range[1] &
          all.expiries.posix <= expiry.range[2]
      }
      if(all(!valid.expiries))
        stop("Provided expiry date(s) not found. Available dates are: ",
             paste(as.Date(all.expiries.posix), collapse=", "))
      
      expiry.subset <- all.expiries[valid.expiries]
      if(length(expiry.subset) == 1)
        return(getOptionChain.yahoo(Symbols, expiry.subset, .expiry.known=TRUE))
      else {
        out <- lapply(expiry.subset, getOptionChain.yahoo, Symbols=Symbols, .expiry.known=TRUE)
        # See comment above regarding the output names
        return(setNames(out, format(all.expiries.posix[valid.expiries], "%b.%d.%Y")))
      }
    }
  }
  
  dftables <- lapply(tbl$optionChain$result$options[[1]][,c("calls","puts")], `[[`, 1L)
  dftables <- head(mapply(NewToOld, x=dftables, SIMPLIFY=FALSE))
  dftables
}




#Loop on stock tickers
for(stockticker in stocktickervector)
{
    optionchain <- try(getOptionChain2(stockticker,paste(format(Sys.Date(), "%Y"),"/",format(Sys.Date()+720, "%Y"),sep=""))) 
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
            SNAPSHOTDATE <- today()  #Snapshotdate #Server time is ahead of 
              if (STOCKOPTIONTYPE == 'C') { 
                  STRIKEDISTANCE <- abs(STRIKELEVELCALL - p)
              } else {
                  STRIKEDISTANCE <- abs(p - STRIKELEVELPUT)
              }
            IV <- option$IV[p] #Strike
            #Query string
            query <- "INSERT INTO stockoption VALUES ('AAA','BBB','CCC','DDD','EEE','FFF','GGG','HHH','III','JJJ','KKK','LLL','MMM')"
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
            query <- gsub("MMM",IV,query)
            #Insert the option
            dbSendQuery(mydb, query)
          }
        }
    } 
    
}

#Close the database
dbDisconnect((mydb))







