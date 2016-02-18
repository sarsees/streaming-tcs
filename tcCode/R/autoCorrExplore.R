#' Script to explore the autocorrelation among features of the SHIPS
#' developmental dataset using Trelliscope
#' 
#' @author Sarah M. Reehl
#' @date Feb 16, 2016
#'   
library(dplyr)
library(datadr)
library(trelliscope)
library(reshape2)
set.seed(42)
# import the data
load("~/Documents/Projects/TeMpSA/TeMpSA/SHIPS_Developmental/tcData.RData")

# find all of the dissipating storms
timeTest <- byStorm %>% mutate(TimeLag = lag(TIMESTAMP))
timeTest$Diff <- (timeTest$TIMESTAMP - timeTest$TimeLag)/ dhours(6)
dissStormIDs <- timeTest$ID[which(timeTest$Diff != 1)]

# split into two sets (preSatellite and postSatellite)
pre95Feats <- byStorm[,!(grepl( "IR." , names(byStorm)))] # remove IR data
preSatellite <- pre95Feats %>% 
  filter(!(ID %in% dissStormIDs)) %>% 
  select(-c(RI,VMAX_Diff, RecordNum, RD20, RD26, RHCN, Match, TYPE)) %>% #Finish removing satellite vars and nonessentials
  mutate(YEAR = year(DATE)) %>% 
  group_by(YEAR) %>% 
  filter(YEAR < 1995)
preSatelliteSamples <- subset(preSatellite, ID %in% sample(unique(preSatellite$ID), size = 10))

postSatellite <-  byStorm %>% 
  filter(!(ID %in% dissStormIDs)) %>% 
  select(-c(RI,VMAX_Diff, RecordNum, Match, RD20, RD26, TYPE)) %>% 
  mutate(YEAR = year(DATE)) %>% 
  group_by(YEAR) %>% 
  filter(YEAR >= 1995) 
postSatelliteSamples <- subset(postSatellite, ID %in% sample(unique(postSatellite$ID), size = 10)) 
  

# melt features
meltedPreSatellite <- melt(preSatellite, id.vars = c("ID", "DATE", "YEAR", "TIMESTAMP")) 
meltedPostSatellite <- melt(postSatellite, id.vars = c("ID", "DATE", "YEAR", "TIMESTAMP")) 
allStorms <- rbind(meltedPreSatellite, meltedPostSatellite)

#################
#  Trelliscope  #
#################
vdbPath <- "/Users/reeh135/Documents/Projects/TeMpSA/TeMpSA/"
conn <- vdbConn(file.path(vdbPath, "stormVDB"), autoYes = TRUE)

# divide by storm and variable
byStormAndVar <- divide(allStorms, by = c("ID", "variable"), output = localDiskConn(file.path(vdbPath, "byStormByVarAutoCorr")), overwrite = TRUE)

# Panel Functions
autoCorrPanel <- function(division){
  # make a time series from the data
  stormTimeSeries <- ts(as.numeric(division$value))
  acf(stormTimeSeries, lag.max = 10, na.action = na.pass)
  return(NULL)
}

makeDisplay(byStormAndVar,
            name = "by_Storm_and_Var_auto_correlation",
            desc = "the autocorrelation as estimated by acf for storm variables",
            panelFn = autoCorrPanel
)
view()







