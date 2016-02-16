#' Script to parse and clean SHIPS Developmental data located at 
#' http://rammb.cira.colostate.edu/research/tropical_cyclones/ships/developmental_data.asp
#' 
#' @author Sarah M. Reehl
#' @date Jan 25 2016
#'   
#' @param datpath The path to the original RAMMBS file
#'   lsdiaga_1982_2014_rean_sat_nbc_ts.dat
#' @param datDir a path created in ParseToFiles that holds parsed storm records as csvs (currently ~/Desktop/TC_Data/New/)
#' @param dat.pattern the intermediate storm file extensions (currently .csv)
library(stringr)
library(lubridate)
library(dplyr)
# Function
ParseToFiles <- function(datPath){
#` Parses fortran99 tables to data frames
  parsedData <- list()
  temp <- readLines(con = datPath) # initial read
  IDold <- "None"
  # parse in chunks of 76 rows at a time
  for (i in seq(1,(length(temp) - 75), by = 76)) { 
    startInd = i
    stopInd = i + 75
    parsedTemp <- temp[startInd:stopInd]
    featNames <- matrix(0, nrow = length(parsedTemp), ncol = 1)
    for (j in 1:length(parsedTemp)) {
      featNames[j] <- str_extract(string = parsedTemp[j], pattern = "[A-Z]...") # get attribute names
    }
    featNames <- gsub("[.]", "", featNames)
    featNames <- gsub(" ", "", featNames)
    test <- gsub("^[\" .]", "    ", parsedTemp) # even spaces between fields
    write.table(test, file = "~/Desktop/test", row.names = FALSE) # write a temporary table
    testTable <- read.fwf("~/Desktop/test", widths = rep(5, 24),skip = 2, stringsAsFactors = FALSE) # read as a fixed width table
    unlink("~/Desktop/test") # delete intermediate
    intermediateResult <- t(testTable[, -1]) # transpose
    colnames(intermediateResult) <- featNames[-1] # remove HEAD
    ID <- grep(pattern = "AL[0-9]{6}", strsplit(temp[i], split = " ")[[1]], value = TRUE) 
    DATE <- grep(pattern = "^[0-9]{6}", strsplit(temp[i], split = " ")[[1]], value = TRUE)
    # Get timestamp from HEAD
    p = gsub("\\s+", " ", str_trim(temp[i]))
    time = strsplit(p, split = " ")[[1]][2:3]
    #TIMESTAMP <- parse_date_time(paste(time[1], paste(time[2:4], sep = "", collapse = ":"), sep = " "), "%y%m%d %H%M%S")
    TIMESTAMP <- parse_date_time(paste(time, collapse = " "), "ymdh")
    result <- data.frame(ID, DATE, TIMESTAMP, intermediateResult, stringsAsFactors = FALSE) 
    # Attach a record number to the storm filename
    if (!(IDold == ID)) {
      count = 0
      filename <- paste(paste("~/Desktop/TC_Data/New/", ID, sep = ""), count, "csv", sep = ".")
    }
    if (IDold == ID) {
      count = count + 1
      filename <- paste(paste("~/Desktop/TC_Data/New/", ID, sep = ""), count, "csv", sep = ".")
    }
    write.csv(result, file = filename, row.names = FALSE) 
    IDold <- ID
  } 
  return(parsedData)
  }

MatchTimePoints <- function(datDir = "~/Desktop/TC_Data/New/", datPattern = ".csv"){ 
  #' Data check to find inconsistencies between storm files
  csvPaths <- data.frame(paths = as.character(list.files(path = datDir, pattern = datPattern)), stringsAsFactors = FALSE)
  csvPaths$ID <- unlist(lapply(csvPaths$paths, function(x) strsplit(x, split = "[.]")[[1]][1]))
  csvPaths$recordNumber <- as.numeric(unlist(lapply(csvPaths$paths, function(x) strsplit(x, split = "[.]")[[1]][2])))
  groupedPaths <- csvPaths %>% group_by(ID) %>% arrange(recordNumber)
  firstRecord <- read.csv(paste(datDir, groupedPaths$paths[1], sep = ""), stringsAsFactors = FALSE, na.strings = c("NA","9999")) 
  tcStorms = data.frame(firstRecord[which(firstRecord$TIME == 0), ]) # create a data frame for time 0 observations, fill with record 1 time 0
  tcStorms[1, "TIME"] <- csvPaths$recordNumber[1]
  # create a list of variables that should be consistent across matching time observations
  matchableVars <- c("D200", "DTL", "E000", "ENEG", "ENSS", "EPOS",
                     "EPSS", "INCV", "LAT", "LON", "MSLP", "PEFC",
                     "PENV", "R000", "RHHI", "RHLO", "RHMD",
                     "SHRD", "SHRG", "SHRS", "SHTD", "SHTS", "T000",
                     "T150", "T200", "T250", "U200", "VMAX",
                     "Z000", "Z850", "TYPE", "REFC") # removing TYPE and REFC removes 28 of 38 inconsistencies
  tcStorms$Match <- NA # create a column to track timepoint discrepancies
  # Get the HIST Variables For Current Time
  tcStorms$HIST20 <- firstRecord[which(firstRecord$TIME == 0), "HIST"]
  tcStorms$HIST25 <- firstRecord[which(firstRecord$TIME == 6), "HIST"]
  tcStorms$HIST30 <- firstRecord[which(firstRecord$TIME == 12), "HIST"]
  tcStorms$HIST35 <- firstRecord[which(firstRecord$TIME == 18), "HIST"]
  tcStorms$HIST40 <- firstRecord[which(firstRecord$TIME == 24), "HIST"]
  tcStorms$HIST45 <- firstRecord[which(firstRecord$TIME == 30), "HIST"]
  tcStorms$HIST50 <- firstRecord[which(firstRecord$TIME == 36), "HIST"]
  tcStorms$HIST55 <- firstRecord[which(firstRecord$TIME == 42), "HIST"]
  tcStorms$HIST60 <- firstRecord[which(firstRecord$TIME == 48), "HIST"]
  tcStorms$HIST65 <- firstRecord[which(firstRecord$TIME == 54), "HIST"]
  tcStorms$HIST70 <- firstRecord[which(firstRecord$TIME == 60), "HIST"]
  tcStorms$HIST75 <- firstRecord[which(firstRecord$TIME == 66), "HIST"]
  tcStorms$HIST80 <- firstRecord[which(firstRecord$TIME == 72), "HIST"]
  tcStorms$HIST85 <- firstRecord[which(firstRecord$TIME == 78), "HIST"]
  tcStorms$HIST90 <- firstRecord[which(firstRecord$TIME == 84), "HIST"]
  tcStorms$HIST95 <- firstRecord[which(firstRecord$TIME == 90), "HIST"]
  tcStorms$HIST100 <- firstRecord[which(firstRecord$TIME == 96), "HIST"]
  tcStorms$HIST105 <- firstRecord[which(firstRecord$TIME == 102), "HIST"]
  tcStorms$HIST110 <- firstRecord[which(firstRecord$TIME == 108), "HIST"]
  tcStorms$HIST115 <- firstRecord[which(firstRecord$TIME == 114), "HIST"]
  tcStorms$HIST120 <- firstRecord[which(firstRecord$TIME == 120), "HIST"]
  # Get the GOES Variables For Current Time
  tcStorms$IR00_TIME <- firstRecord[which(firstRecord$TIME == 0), "IR00"]
  tcStorms$IR00_AVG_200BT <- firstRecord[which(firstRecord$TIME == 6), "IR00"]
  tcStorms$IR00_STD_200BT <- firstRecord[which(firstRecord$TIME == 12), "IR00"]
  tcStorms$IR00_AVG_300BT <- firstRecord[which(firstRecord$TIME == 18), "IR00"]
  tcStorms$IR00_STD_300BT <- firstRecord[which(firstRecord$TIME == 24), "IR00"]
  tcStorms$IR00_PCT_AREA_10BT <- firstRecord[which(firstRecord$TIME == 30), "IR00"]
  tcStorms$IR00_PCT_AREA_20BT <- firstRecord[which(firstRecord$TIME == 36), "IR00"]
  tcStorms$IR00_PCT_AREA_30BT <- firstRecord[which(firstRecord$TIME == 42), "IR00"]
  tcStorms$IR00_PCT_AREA_40BT <- firstRecord[which(firstRecord$TIME == 48), "IR00"]
  tcStorms$IR00_PCT_AREA_50BT <- firstRecord[which(firstRecord$TIME == 54), "IR00"]
  tcStorms$IR00_PCT_AREA_60BT <- firstRecord[which(firstRecord$TIME == 60), "IR00"]
  tcStorms$IR00_MAX_BT <- firstRecord[which(firstRecord$TIME == 66), "IR00"]
  tcStorms$IR00_AVG_30BT <- firstRecord[which(firstRecord$TIME == 72), "IR00"]
  tcStorms$IR00_RADIUS_MAXBT <- firstRecord[which(firstRecord$TIME == 78), "IR00"]
  tcStorms$IR00_MIN_20BT <- firstRecord[which(firstRecord$TIME == 84), "IR00"]
  tcStorms$IR00_AVG_20BT <- firstRecord[which(firstRecord$TIME == 90), "IR00"]
  tcStorms$IR00_RADIUS_MINBT <- firstRecord[which(firstRecord$TIME == 96), "IR00"]
  # Get the GOES Variables For 3 Hrs  Time
  tcStorms$IRM3_TIME <- firstRecord[which(firstRecord$TIME == 0), "IRM3"]
  tcStorms$IRM3_AVG_200BT <- firstRecord[which(firstRecord$TIME == 6), "IRM3"]
  tcStorms$IRM3_STD_200BT <- firstRecord[which(firstRecord$TIME == 12), "IRM3"]
  tcStorms$IRM3_AVG_300BT <- firstRecord[which(firstRecord$TIME == 18), "IRM3"]
  tcStorms$IRM3_STD_300BT <- firstRecord[which(firstRecord$TIME == 24), "IRM3"]
  tcStorms$IRM3_PCT_AREA_10BT <- firstRecord[which(firstRecord$TIME == 30), "IRM3"]
  tcStorms$IRM3_PCT_AREA_20BT <- firstRecord[which(firstRecord$TIME == 36), "IRM3"]
  tcStorms$IRM3_PCT_AREA_30BT <- firstRecord[which(firstRecord$TIME == 42), "IRM3"]
  tcStorms$IRM3_PCT_AREA_40BT <- firstRecord[which(firstRecord$TIME == 48), "IRM3"]
  tcStorms$IRM3_PCT_AREA_50BT <- firstRecord[which(firstRecord$TIME == 54), "IRM3"]
  tcStorms$IRM3_PCT_AREA_60BT <- firstRecord[which(firstRecord$TIME == 60), "IRM3"]
  tcStorms$IRM3_MAX_BT <- firstRecord[which(firstRecord$TIME == 66), "IRM3"]
  tcStorms$IRM3_AVG_30BT <- firstRecord[which(firstRecord$TIME == 72), "IRM3"]
  tcStorms$IRM3_RADIUS_MAXBT <- firstRecord[which(firstRecord$TIME == 78), "IRM3"]
  tcStorms$IRM3_MIN_20BT <- firstRecord[which(firstRecord$TIME == 84), "IRM3"]
  tcStorms$IRM3_AVG_20BT <- firstRecord[which(firstRecord$TIME == 90), "IRM3"]
  tcStorms$IRM3_RADIUS_MINBT <- firstRecord[which(firstRecord$TIME == 96), "IRM3"]
  for (i in 2:length(groupedPaths$paths)) {
    prevRecord <- read.csv(paste(datDir, groupedPaths$paths[i - 1], sep = ""), stringsAsFactors = FALSE, na.strings = c("NA","9999")) # r0 record
    prevRecord[ prevRecord == "NA"] <- NA
    currentRecord <- read.csv(paste(datDir, groupedPaths$paths[i], sep = ""), stringsAsFactors = FALSE, na.strings = c("NA","9999")) # r1 record
    currentRecord[ currentRecord == "NA"] <- NA
    tcStorms[i, c(1:ncol(currentRecord))] <- currentRecord[which(currentRecord$TIME == 0), ] # place t1 record in the master data frame
    # if storm IDs match crosscheck that r0 time points 6-120 match r1 time points 0 to 114
    if (currentRecord$ID[1] == prevRecord$ID[1]) {
      p <- prevRecord[which(prevRecord$TIME %in% seq(6,120, by = 6)), matchableVars]
      q <- currentRecord[which(prevRecord$TIME %in% seq(0,114, by = 6)), matchableVars]
      disjoint <- anti_join(p,q, by = matchableVars)
      if ( nrow(disjoint) == 0) {
        tcStorms$Match[i] = TRUE # if they match set TRUE
      }
      if ( nrow(disjoint) > 0) {
        tcStorms$Match[i] = FALSE # if the don't match set FALSE
      }
    }
    # if storm IDs don't match set Match to NA
    if (currentRecord$ID[1] != prevRecord$ID[1]) {
      tcStorms$Match[i] = NA 
    }
    tcStorms[i, "RecordNum"] <- groupedPaths$recordNumber[i]
    # Get the HIST Variables for Current Time
    
    tcStorms$HIST20[i] <- currentRecord[which(currentRecord$TIME == 0), "HIST"]
    tcStorms$HIST25[i] <- currentRecord[which(currentRecord$TIME == 6), "HIST"]
    tcStorms$HIST30[i] <- currentRecord[which(currentRecord$TIME == 12), "HIST"]
    tcStorms$HIST35[i] <- currentRecord[which(currentRecord$TIME == 18), "HIST"]
    tcStorms$HIST40[i] <- currentRecord[which(currentRecord$TIME == 24), "HIST"]
    tcStorms$HIST45[i] <- currentRecord[which(currentRecord$TIME == 30), "HIST"]
    tcStorms$HIST50[i] <- currentRecord[which(currentRecord$TIME == 36), "HIST"]
    tcStorms$HIST55[i] <- currentRecord[which(currentRecord$TIME == 42), "HIST"]
    tcStorms$HIST60[i] <- currentRecord[which(currentRecord$TIME == 48), "HIST"]
    tcStorms$HIST65[i] <- currentRecord[which(currentRecord$TIME == 54), "HIST"]
    tcStorms$HIST70[i] <- currentRecord[which(currentRecord$TIME == 60), "HIST"]
    tcStorms$HIST75[i] <- currentRecord[which(currentRecord$TIME == 66), "HIST"]
    tcStorms$HIST80[i] <- currentRecord[which(currentRecord$TIME == 72), "HIST"]
    tcStorms$HIST85[i] <- currentRecord[which(currentRecord$TIME == 78), "HIST"]
    tcStorms$HIST90[i] <- currentRecord[which(currentRecord$TIME == 84), "HIST"]
    tcStorms$HIST95[i] <- currentRecord[which(currentRecord$TIME == 90), "HIST"]
    tcStorms$HIST100[i] <- currentRecord[which(currentRecord$TIME == 96), "HIST"]
    tcStorms$HIST105[i] <- currentRecord[which(currentRecord$TIME == 102), "HIST"]
    tcStorms$HIST110[i] <- currentRecord[which(currentRecord$TIME == 108), "HIST"]
    tcStorms$HIST115[i] <- currentRecord[which(currentRecord$TIME == 114), "HIST"]
    tcStorms$HIST120[i] <- currentRecord[which(currentRecord$TIME == 120), "HIST"]
    # Get the GOES Variables For Current Time
    tcStorms[i, "IR00_TIME"] <- currentRecord[which(currentRecord$TIME == 0), "IR00"]
    tcStorms[i, "IR00_AVG_200BT"] <- currentRecord[which(currentRecord$TIME == 6), "IR00"]
    tcStorms[i, "IR00_STD_200BT"] <- currentRecord[which(currentRecord$TIME == 12), "IR00"]
    tcStorms[i, "IR00_AVG_300BT"] <- currentRecord[which(currentRecord$TIME == 18), "IR00"]
    tcStorms[i, "IR00_STD_300BT"] <- currentRecord[which(currentRecord$TIME == 24), "IR00"]
    tcStorms[i, "IR00_PCT_AREA_10BT"] <- currentRecord[which(currentRecord$TIME == 30), "IR00"]
    tcStorms[i, "IR00_PCT_AREA_20BT"] <- currentRecord[which(currentRecord$TIME == 36), "IR00"]
    tcStorms[i, "IR00_PCT_AREA_30BT"] <- currentRecord[which(currentRecord$TIME == 42), "IR00"]
    tcStorms[i, "IR00_PCT_AREA_40BT"] <- currentRecord[which(currentRecord$TIME == 48), "IR00"]
    tcStorms[i, "IR00_PCT_AREA_50BT"] <- currentRecord[which(currentRecord$TIME == 54), "IR00"]
    tcStorms[i, "IR00_PCT_AREA_60BT"] <- currentRecord[which(currentRecord$TIME == 60), "IR00"]
    tcStorms[i, "IR00_MAX_BT"] <- currentRecord[which(currentRecord$TIME == 66), "IR00"]
    tcStorms[i, "IR00_AVG_30BT"] <- currentRecord[which(currentRecord$TIME == 72), "IR00"]
    tcStorms[i, "IR00_RADIUS_MAXBT"] <- currentRecord[which(currentRecord$TIME == 78), "IR00"]
    tcStorms[i, "IR00_MIN_20BT"] <- currentRecord[which(currentRecord$TIME == 84), "IR00"]
    tcStorms[i, "IR00_AVG_20BT"] <- currentRecord[which(currentRecord$TIME == 90), "IR00"]
    tcStorms[i, "IR00_RADIUS_MINBT"] <- currentRecord[which(currentRecord$TIME == 96), "IR00"]
    # Get the GOES Variables For 3 Hrs  Time
    tcStorms[i, "IRM3_TIME"] <- currentRecord[which(currentRecord$TIME == 0), "IRM3"]
    tcStorms[i, "IRM3_AVG_200BT"] <- currentRecord[which(currentRecord$TIME == 6), "IRM3"]
    tcStorms[i, "IRM3_STD_200BT"] <- currentRecord[which(currentRecord$TIME == 12), "IRM3"]
    tcStorms[i, "IRM3_AVG_300BT"] <- currentRecord[which(currentRecord$TIME == 18), "IRM3"]
    tcStorms[i, "IRM3_STD_300BT"] <- currentRecord[which(currentRecord$TIME == 24), "IRM3"]
    tcStorms[i, "IRM3_PCT_AREA_10BT"] <- currentRecord[which(currentRecord$TIME == 30), "IRM3"]
    tcStorms[i, "IRM3_PCT_AREA_20BT"] <- currentRecord[which(currentRecord$TIME == 36), "IRM3"]
    tcStorms[i, "IRM3_PCT_AREA_30BT"] <- currentRecord[which(currentRecord$TIME == 42), "IRM3"]
    tcStorms[i, "IRM3_PCT_AREA_40BT"] <- currentRecord[which(currentRecord$TIME == 48), "IRM3"]
    tcStorms[i, "IRM3_PCT_AREA_50BT"] <- currentRecord[which(currentRecord$TIME == 54), "IRM3"]
    tcStorms[i, "IRM3_PCT_AREA_60BT"] <- currentRecord[which(currentRecord$TIME == 60), "IRM3"]
    tcStorms[i, "IRM3_MAX_BT"] <- currentRecord[which(currentRecord$TIME == 66), "IRM3"]
    tcStorms[i, "IRM3_AVG_30BT"] <- currentRecord[which(currentRecord$TIME == 72), "IRM3"]
    tcStorms[i, "IRM3_RADIUS_MAXBT"] <- currentRecord[which(currentRecord$TIME == 78), "IRM3"]
    tcStorms[i, "IRM3_MIN_20BT"] <- currentRecord[which(currentRecord$TIME == 84), "IRM3"]
    tcStorms[i, "IRM3_AVG_20BT"] <- currentRecord[which(currentRecord$TIME == 90), "IRM3"]
    tcStorms[i, "IRM3_RADIUS_MINBT"] <- currentRecord[which(currentRecord$TIME == 96), "IRM3"]
  }
  tcStorms$TYPE <- factor(tcStorms$TYPE)
  paddedDate <- sprintf("%06d", tcStorms$DATE)
  tcStorms$DATE <- parse_date_time(paddedDate, "%y!*%m%d")
  #tcStorms$DATE <- parse_date_time(tcStorms$DATE, "%Y%m%d")
  return(tcStorms)
}

#Execute
TC_Data <- ParseToFiles(datPath = "~/Desktop/lsdiaga_1982_2014_rean_sat_nbc_ts.dat")
tcData <- MatchTimePoints()
save(tcData, file = "~/Documents/Projects/TeMpSA/TeMpSA/SHIPS_Developmental/tcIntermediate.RData")
