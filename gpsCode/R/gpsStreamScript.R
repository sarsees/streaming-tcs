#' A function to fill the GPS Data into even intervals
#' @author Sarah M. Reehl
#' @date 02/02/2016

ForwardFill <- function(csvPath = "/Volumes/AIM_Bramer/VASTChal2014MC2-20140430/gps.csv"){
  library(dplyr)
  library(lubridate)
  ### Functions
  SmoothTime <- function(byID){
    # create a lag column to subtract from the current time column to measure differnces
    gpsIDandTime <- byID %>% mutate(TimestampLag = lag(Timestamp))
    gpsIDandTime$Diffs <- (gpsIDandTime$Timestamp - gpsIDandTime$TimestampLag) /dseconds(1)
    
    # mark these as original data
    gpsIDandTime$Origins <- 1
    
    # Make a data frame of continuous one second intervals for every ID
    firstSeen <- min(gpsIDandTime$Timestamp)  # what's the first observation
    lastSeen <- max(gpsIDandTime$Timestamp) # what's the last observation
    filledTimes <- seq(firstSeen, lastSeen, by = "sec") # create a spanning sequence by second
    
    # now create a data frame for the IDs with these timestamps
    filledTimeGPS <- data.frame()
    for (i in 1:length(unique(gpsIDandTime$id))) {
      id <- (unique(gpsIDandTime$id))[i]
      id2D2 <- rep(id, length(filledTimes))
      temp <- data.frame(id = id2D2, Timestamp = filledTimes)
      filledTimeGPS  <- rbind(filledTimeGPS, temp)
    }
    return(filledTimeGPS)
  }
  MergeAndClean <- function(gpsIDandTime, filledTimeGPS) {
    # merge the two data frames together
    gpsStream <- merge(gpsIDandTime, filledTimeGPS, all = TRUE)
    #save(gpsStream, file = "~/Documents/Projects/TeMpSA/TeMpSA/gpsStream.RData")
    gpsStream[is.na(gpsStream$Origins), "Origins"] <- 0 # mark NA origins as 0
    gpsStream <- gpsStream[order(gpsStream$id, gpsStream$Timestamp), ]
    gpsByID <- gpsStream %>% group_by(id)
    
    # function to forward fill
    repeat.before = function(x) {   # repeats the last non NA value. Keeps leading NA
      ind = which(!is.na(x))      # get positions of nonmissing values
      if (is.na(x[1]))             # if it begins with a missing, add the 
        ind = c(1, ind)        # first position to the indices
      rep(x[ind], times = diff(   # repeat the values at these indices
        c(ind, length(x) + 1) )) # diffing the indices + length yields how often they need to be repeated
    }
    
    gpsByIDFilled <- gpsByID %>% mutate(lat = repeat.before(lat), long = repeat.before(long))
    # cleanup
    gpsByIDFilled$TimestampLag <- NULL
    gpsByIDFilled$Diffs <- NULL
    names(gpsByIDFilled) <- c("Timestamp", "ID", "Lat", "Long", "Origins")
    result <- gpsByIDFilled[!(is.na(gpsByIDFilled$Lat)), ]
    return(result)
  }
  
  ### Execute
  gpsData <- read.csv(csvPath, header = TRUE, stringsAsFactors = FALSE)
  
  # lubridate time
  gpsData$Timestamp <- parse_date_time(gpsData$Timestamp, "%m%d%Y %H%M%S")
  
  # order by Time and ID
  gpsData <- gpsData[order(gpsData$id, gpsData$Timestamp), ]
  byID <- gpsData %>% group_by(id)
  
  # create intervals in seconds
  filledTimeGPS <- SmoothTime(byID)
  
  # merge and forward fill missing observations
  gpsStream <- MergeAndClean(byID, filledTimeGPS)
  # trim the excess timestamps to the last seen observation
  result = list()
  for (i in 1:length(unique(gpsStream$ID))) {
    id <- (unique(gpsStream$ID))[i]
    temp <- subset(gpsStream, ID == id)
    original <- which(temp$Origins == 1)
    lastOriginal <- original[length(original)]
    result[[i]] <- temp[c(1:lastOriginal), ]
  }
  gpsData <- do.call("rbind", result)
  save(gpsData, file = "/Users/reeh135/Documents/Projects/TeMpSA/TeMpSA/gpsStream.RData")
}

ForwardFill()



