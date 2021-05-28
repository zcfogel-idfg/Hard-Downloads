#################################################################################
### PROCESS XLSX COLLARS (IMPORT, STANDARDIZE, AND COMPILE INTO SINGLE TABLE) ###
#################################################################################

####-------------------------------------------------------------------------
#### Set working directory 

setwd('K:/Wildlife/Fogel/Collar Data Processing')

####-------------------------------------------------------------------------
#### Read in and clean files 

xlsxnames <- list.files('Collar xlsxs')
#xlsxnames <- xlsxnames[2:10] # for some reason there's a weird filename in the beginning of the list
xlsxnames <- paste0('Collar xlsxs/', xlsxnames)

xlsdat <- xlsxnames %>% 
  set_names() %>%
  # read in each xlsx, ignoring column headers, with every column read in as character data type
  # read_excel is in readxl package
  map_df(read_excel, skip = 1, col_names = F, 
         .id = 'fileName') %>%
  # column names are assigned as '...#'
  rename(Year = ...1, 
         Day = ...2, 
         Hour = ...3,
         Minutes = ...4, 
         Status = ...5, 
         Activity = ...6,
         Temperature_C = ...7,
         Blocknumber = ...8,
         LAT = ...9,
         LONG = ...10,
         HDOP = ...11,
         Satsused = ...12,
         Fix_Time = ...13,
         FixStatus = ...14
  )

# remove .xlsx from fileName
xlsdat$fileName <- tools::file_path_sans_ext(basename(xlsdat$fileName))

# split fileName into collar serial and animal ID
xlsdat$SerialNo <- str_split_fixed(xlsdat$fileName, '_', n=2)[,1] # extract serialNo from filename
xlsdat$ET <- str_split_fixed(xlsdat$fileName, '_', n=2)[,2] # extract ET# from filename
xlsdat$Animal_ID <- gsub('ET', '', xlsdat$ET) # remove 'ET' from ET#

# set origin as January 1 for converting from julian date
xlsdat$Origin <- 'Error'
xlsdat[xlsdat$Year == 19,]$Origin <- '2019-01-01'
xlsdat[xlsdat$Year == 20,]$Origin <- '2020-01-01'
xlsdat[xlsdat$Year == 21,]$Origin <- '2021-01-01'

# for some reason need to subtract 1 from Julian date because the function shifts it by 1
xlsdat$Date <- as.character(as.Date((xlsdat$Day - 1), origin = xlsdat$Origin))
xlsdat$Time <- paste(xlsdat$Hour, xlsdat$Minutes, sep = ':')
xlsdat$DateTime_LMT <- ymd_hm(paste(xlsdat$Date, xlsdat$Time))

## search in ATS collar instructions for these serial numbers (this time these were all ATS collars)
#source('K:/Wildlife/Fogel/Collar Data Processing/ATS Time Zones/ATS_Time_Zones.R')

# initialize time zone as error
xlsdat$tz <- 'Error'

xlsdat[xlsdat$SerialNo %in% MST,]$tz <- 'MST'
xlsdat[xlsdat$SerialNo %in% PST,]$tz <- 'PST'
# all PST

# add PST 
xlsdat$DateTime_LMT <- force_tz(xlsdat$DateTime_LMT, tz = 'America/Los_Angeles')
# convert to GMT
xlsdat$DateTime_GMT <- as.character(with_tz(xlsdat$DateTime_LMT, tz = 'GMT'))

# change LMT to characters
xlsdat$DateTime_LMT <- as.character(xlsdat$DateTime_LMT)

# extract time and date from datetime
xlsdat$GMT_Date <- str_split_fixed(xlsdat$DateTime_GMT, ' ', 2)[,1]
xlsdat$GMT_Time <- str_split_fixed(xlsdat$DateTime_GMT, ' ', 2)[,2]
xlsdat$LMT_Time <- str_split_fixed(xlsdat$DateTime_LMT, ' ', 2)[,2]
# LMT date already exists

# rename columns and remove unneeded columns
xlsdat <- xlsdat %>% 
  rename(
    DOP = HDOP,
    Duration_sec = Fix_Time,
    Collar_Serial_No = SerialNo,
    LMT_Date = Date,
    LMT = DateTime_LMT,
    GMT = DateTime_GMT
  ) %>%
  select(-c('Year', 'Day', 'Hour', 'Minutes', 'Blocknumber', 'Time', 'tz', 'Status'))

xlsdat$CollarMake <- 'ATS'

# check to make sure all the proper headers are included/removed
names(existing[, !(names(existing) %in% names(xlsdat))])
names(xlsdat[, !(names(xlsdat) %in% names(existing))])


































