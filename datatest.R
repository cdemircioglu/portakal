library(RMySQL)
library(xtable)
library(data.table)

#Connection string
mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host="cemoptions.cloudapp.net")
rs = dbSendQuery(mydb, "SELECT FUTURE,MAX(SNAPSHOTDATE) AS SNAPSHOTDATE FROM futures WHERE VOLUME IS NOT NULL GROUP BY FUTURE;")
stockdata = fetch(rs, n=-1)
                             
#Disconnect from the database
dbDisconnect(mydb)

#Print out the data set
finaldt <- as.data.table(stockdata)
print(xtable(as.data.frame.matrix(finaldt),digits=c(0,0,0)), type='html', file="emailcontent_data.html")