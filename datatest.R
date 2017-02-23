library(RMySQL)
library(xtable)
library(data.table)

#Connection string
mydb = dbConnect(MySQL(), user='borsacanavari', password='opsiyoncanavari1', dbname='myoptions', host="cemoptions.cloudapp.net")
rs = dbSendQuery(mydb, "SELECT A.FUTURE,MAX(CASE WHEN A.VOLUME IS NULL THEN '2015-01-01' ELSE A.SNAPSHOTDATE END) AS CONTINUOUS, MAX(A.SNAPSHOTDATE) AS CURRENT,
(SELECT MAX(B.SNAPSHOTDATE) FROM futurescftc B WHERE B.FUTURE = A.FUTURE) AS CFTC
                 FROM futures A GROUP BY A.FUTURE;
                 ")
stockdata = fetch(rs, n=-1)
                             
#Disconnect from the database
dbDisconnect(mydb)

#Print out the data set
finaldt <- as.data.table(stockdata)
print(xtable(as.data.frame.matrix(finaldt)), type='html', file="/home/cem/emailcontent_testdata.html")
