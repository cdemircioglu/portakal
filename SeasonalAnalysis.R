#Get the library 
library("DBI")
library("dplyr")
library("methods")
library("data.table")
library("XML")
library("RMySQL")
library("ggplot2")

#Read csv file
blob <- read.csv("c:\\temp\\CHRIS-CME_NG11.csv", header=TRUE)  

#Set the ticker
stockticker <- "NG"

#Connection settings and the dataset
mydb <- dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host=myhost)
rs <- dbSendQuery(mydb, paste("SELECT F.SNAPSHOTDATE AS 'Date', F.OPEN AS 'Open', F.High, F.Low, F.LAST AS 'Last', F.CHANGE AS 'Change', F.Settle FROM futures F WHERE F.Future = '",stockticker,"' ORDER BY F.SNAPSHOTDATE DESC",sep=""))
blob <- fetch(rs, n=-1)

blob$Date <- as.POSIXct(blob$Date)

#Group them based on their dates, 1st of month
pdates <- blob[,c(1,2)] %>% 
  group_by(month(as.POSIXlt(blob$Date)),year(as.POSIXlt(blob$Date))) %>% 
  slice(which.min(as.numeric(as.POSIXlt(Date))))

#Get the dates with the first
pdates <- as.list(pdates[with(pdates, order(as.character(as.POSIXlt(Date)))), 1])
pdates <- as.vector(as.character(as.POSIXlt(pdates$Date)))

#These are the line numbers
nline <- as.numeric(rownames(blob[as.character(blob$Date) %in% pdates, ]))

#Create a temp data frame
mdata <- data.frame(
    SnapshotDate = character(),
    Open = numeric(),
    PSettle = numeric()
    )

#Loop on lines
for(i in 1:length(nline))
{
    mdata <- rbind(mdata, data.frame(SnapshotDate = blob[nline[i],1],
             Open = blob[nline[i],2],
             PSettle = if(i==1) blob[nline[i],2] else blob[nline[i-1],7] )
  )
}

#Temp vector
v <- c()

#Calculate difference 
for(i in 1:nrow(mdata)-1)
{
  v <- c(v,(mdata[i,3]-mdata[i+1,2])/mdata[i+1,2]) #Month open vs month settle
}

#Add the differences 
mdata$PDifference <- c(NA,v)

#Monthly differences are at PDifference
head(mdata)


