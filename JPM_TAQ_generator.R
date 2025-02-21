# R code for processing Refinitiv tick-level data
# combines the trades and quotes data to form a TAQ style file

library(xts)
library(highfrequency)

Sys.setenv(TZ = "GMT")  
options(digits.secs=3) # keep millisecond timestamps

# Datafile obtained from Thompson Reuters aka Refinitiv
rawdata <- read.csv("JPM_Jan_13_2021_EXCH.csv", header = TRUE)
#rawdata <- read.csv("JPM_Jan_13_2021.csv", header = TRUE)
head(rawdata)

#View(rawdata)

class(rawdata)

names(rawdata)
#summary(rawdata)
length(rawdata$Price) #439,230 entries (trades+quotes)

length(rawdata$Type=="Trade")
length(rawdata$Type=="Quote")

head(rawdata)

tdata <- subset(rawdata, Type=="Trade")
qdata <- subset(rawdata, Type=="Quote")

View()


length(tdata$Price) #129,393 trades
length(qdata$Price) #309,812 quotes

tdata.small <- data.frame(TIME = gsub("T", " ", tdata$Date.Time, perl=TRUE),
                          SYMBOL = "JPM",
                          PRICE = tdata$Price,
                          SIZE = tdata$Volume,
                          EX = tdata$Ex.Cntrb.ID)



head(tdata.small,10)

# filter the quotes for each product 

qdata.small <- data.frame(TIME = gsub("T", " ", qdata$Date.Time, perl=TRUE),
                          SYMBOL = "JPM",
                          BID = qdata$Bid.Price,
                          BIDSIZ = qdata$Bid.Size,
                          OFR = qdata$Ask.Price,
                          OFRSIZ = qdata$Ask.Size)

head(qdata.small)

# remove the "T" from the Date.Time
longdate <- c('2021-01-13T00:00:00.015487583Z')

shortdate <- gsub("T", " ", longdate, perl=TRUE)

class(tdata.small) # is data.frame. 
# must put it in xts format, to act on it with aggregateTrades

tdata.xts <- xts(tdata.small[,-1], 
                order.by=as.POSIXct(tdata.small[,1], format = "%Y-%m-%d %H:%M:%OS"))

qdata.xts <- xts(qdata.small[,-1], 
                order.by=as.POSIXct(qdata.small[,1], format = "%Y-%m-%d %H:%M:%OS"))


class(tdata.xts) # ok this is in xts format
head(tdata.xts) # usual TAQ format
head(qdata.xts)

#tqdata2.xts <- qdata.xts[tdata.xts, roll = TRUE, on = c("SYMBOL", "DT")]

# when using v.0.8.0.1 highfrequency, 
# combine trades with same timestamp (T3 rule)
# before matching trades and quotes

#mergeTradesSameTimestamp(tdata.xts)

length(tdata.xts$PRICE)


#tradesCleanupUsingQuotes(tdata.xts)

#length(tdata.xts$PRICE)

# merge trade and quote data
#default: 2 sec quotes delay. can't change default
# matchTradesQuotes(tdata, qdata, adjustment = 2)
tqdata = matchTradesQuotes(tdata.xts, qdata.xts) 


head(tqdata,10) # this is the TAQ file for JPM
# if there are some rows with NA, delete them with the next line
# do not delete before adding back in EX data
#tqdata <- na.omit(tqdata) # do not delete NA's to add EX information
#Sys.setenv(TZ = "GMT")  


head(tqdata)
tail(tqdata)

length(tqdata$PRICE) # contains 129,390 rows
length(tdata.xts$PRICE)
#lost the exchange information, add it back in
#matchTradesQuotes dropped the EX column. can be added by modifying this line
# trace(matchTradesQuotes, edit=TRUE)
# tdata <- tdata[, c("DT", "SYMBOL", "PRICE", "SIZE", "EX")]

names(tqdata)

class(tqdataEX)
names(tqdataEX)

head(tqdata$TIME)


rm(tqdataEX)
tqdataEX <- data.frame(TIME = tdata.small$TIME,
                      SYMBOL = tqdata$SYMBOL,
                      BID = tqdata$BID,
                      BIDSIZ = tqdata$BIDSIZ,
                      OFR = tqdata$OFR,
                      OFRSIZ = tqdata$OFRSIZ,
                      PRICE = tqdata$PRICE,
                      SIZE = tqdata$SIZE,
                      EX = tdata$Ex.Cntrb.ID)

head(tqdataEX)
# before conversion to xts remove NA
tqdataEX <- na.omit(tqdataEX)

length(tqdataEX$TIME)

tqdataEX.xts <- xts(tqdataEX[,-1], 
                   order.by=as.POSIXct(tqdataEX[,1], format = "%Y-%m-%d %H:%M:%OS"))

class(tqdataEX.xts)

head(tqdataEX.xts)

Sys.setenv(TZ = "EST")  
head(tqdataEX.xts)
tail(tqdataEX.xts)

############################################
tradhrsEST <- '2021-01-13 09:30:00::2021-01-13 16:00:00'
length(tqdataEX.xts[tradhrsEST]$PRICE) # 127,720 trades

tqdataMktHrs <- tqdataEX.xts[tradhrsEST]
######## Save only MktHrs on EST ###################
head(tqdataMktHrs)
tail(tqdataMktHrs)
# save the TAQ file for later processing
save(tqdataMktHrs, file = "taqdata_JPM_20210113_ESTMktHrsAdj1.RData")

write.csv(tqdataMktHrs, file = "taqdata_JPM_20210113_ESTMktHrs.csv")
##############################################


# how many trades on each exchange
library(plyr)
count(tqdataEX.xts, vars="EX")

tradesEX <- as.data.frame(table(tqdataEX.xts$EX))

tradesEX


# extract only one hr of data
tqdata['2021-01-13 00:07']

# practice with xts objects
indexClass(tqdata) #POSIXct

periodicity(tqdata) #periodicity 0.0003 seconds


# save the TAQ file for later processing
save(tqdataEX.xts, file = "taqdata_JPM_20210113_EXquotesLag0.RData")

write.csv(tqdataEX.xts, file = "taqdata_JPM_20210113_EXquotesLag0.csv")
##############################################


# aggregate data at 1min and 5 min intervals
tdata.1min <- aggregateTrades(tdata.xts, on="minutes", k=1)
tdata.5min <- aggregateTrades(tdata.xts, on="minutes", k=5)

head(tdata.5min,10) # success but duplicated rows remain
head(tdata.5min)
length(tdata.5min$SYMBOL) #423 some duplication

# Remove rows with a duplicated timestamp, but keep the latest one
tdata.XBTUSD.5min0 <- tdata.XBTUSD.5min[ ! duplicated( index(tdata.XBTUSD.5min), fromLast = TRUE ),  ]

tail(tdata.XBTUSD.5min0,10)
length(tdata.XBTUSD.5min0$SYMBOL) # 289
# 24 hrs = 24 * 12 = 288 periods of 5 mins

# Load the data and draw some plots
load("taqdata_JPM_20210113.RData")

head(tqdata)
tail(tqdata)
minprice <- min(tqdata$PRICE) #113.45
maxprice <- max(tqdata$PRICE) #140.79

# draw plots of prices and trade volumes

pJPM <- as.numeric(tqdata$PRICE)
sizeJPM <- as.numeric(tqdata$SIZE)
asks <- as.numeric(tqdata$OFR)
bids <- as.numeric(tqdata$BID)
mids <- 0.5*bids + 0.5*asks

# Plot time series of trades


# Plot time series of trade/quotes prices from 100 to 200
plot(100:150, pJPM[100:150], col="black", type="p", 
     main="Trade price JPM", xlab="Trade #", ylab="Price")
lines(100:150, bids[100:150], type ="l", col="blue")
lines(100:150, asks[100:150], type="l", col="red")
lines(100:150, mids[100:150], type="l", col="black")



pJPM <- as.numeric(tqdata$PRICE)
sizeJPM <- as.numeric(tqdata$SIZE)

class(pJPM)

# plot the price and volume
plot(pJPM[1:200], type="l", col="blue", 
     main="trade price", ylim=c(139,141))

plot(sizeJPM[1:10000], type="l", col="red", 
     main="trade price")



3169/78

