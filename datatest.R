library(RMySQL)
library(xtable)
library(data.table)

#Connection string
mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host="cemoptions.cloudapp.net")
rs = dbSendQuery(mydb, "CALL GetDataQuality();")
stockdata = fetch(rs, n=-1)
                             
#Disconnect from the database
dbDisconnect(mydb)

#Print out the data set
finaldt <- as.data.table(stockdata)
print(xtable(as.data.frame.matrix(finaldt)), type='html', file="/home/cem/emailcontent_testdata.html")
