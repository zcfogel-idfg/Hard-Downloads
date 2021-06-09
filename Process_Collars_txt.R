################################################################################
### PROCESS TXT COLLARS (IMPORT, STANDARDIZE, AND COMPILE INTO SINGLE TABLE) ###
################################################################################

####-------------------------------------------------------------------
#### Get list of column names from server database
# existing <- dbGetQuery(con, 'SELECT *
#                       FROM Collars_Hard_Downloads2')

####-------------------------------------------------------------------
#### first run Sort_Collars.R

####---------------------------------------------------------------------
#### Read in txts table from end of last script

#txts <- read.csv('K:/Wildlife/Fogel/Collar Data Processing/txt_files_to_process.csv', header = T)
#txts <- readRDS('K:/Wildlife/Fogel/Collar Data Processing/RData/txts.RDS')

# change directory to access actual collar files
setwd('K:/Wildlife/Collars/Downloaded Collar Files')



#### For this next section, I needed to go through the header types manually 
####    and determine the necessary column widths for reading the files in.

####------------------------------------------------------------------------
#### Put morts and VITs into separate folder
unused <- subset(txts, type == 1|type == 3)

####-------------------------------------------------------------
#### Type 1 is morts

####-----------------------------------------------------------------------
#### Type 2 (Vectronic)

# vector of txt files, retain file paths
filenames2 <- txts[txts$type == 2, 3]

# import collar data for that type 
newdat2 <- 
  do.call(rbind, lapply(filenames2, function(x) {
    # read_fwf is read fixed width file
    dat2 <- read_fwf(x, skip = 3, col_types = cols(.default = 'c'), fwf_widths(
      # vector of column widths for delineating files
      c(8, 9, 11, 13, 13, 11, 13, 10, 10, 10, 13, 13, 9, 5, 14, 9, 6, 4, 4, 4, 4,
        4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 29, 8, 6,
        7, 6, 21, 21, 9, 9 )
    ))
    # variable of fileName
    dat2$fileName <- tools::file_path_sans_ext(basename(x))
    dat2
  }))

# check to make sure it read in properly
#head(newdat2)

# split fileName into serialno and ET#
newdat2$SerialNo <- str_split_fixed(newdat2$fileName, '_', n=2)[,1] # extract SerialNo from fileName
newdat2$ET <- str_split_fixed(newdat2$fileName, '_', n=2)[,2] # extract ET# from fileName
newdat2$Animal_ID <- gsub('ET', '', newdat2$ET) # remove 'ET' from ET#

# set column headers based on existing 
names(newdat2) <- c('No', 'CollarID', 'LMT Date', 'LMT Time', 'Origin', 
                    'SCTS Date', 'SCTS Time', 'ECEF X', 'ECEF Y', 'ECEF Z', 
                    'Latitude', 'Longitude', 'Height','DOP','FixType', 
                    '3D Error',  'Sats', 'Sat 1',  'C/N 1', 'Sat 2', 'C/N 2', 
                    'Sat 3', 'C/N 3', 'Sat 4', 'C/N 4', 'Sat 5', 'C/N 5', 
                    'Sat 6', 'C/N 6', 'Sat 7', 'C/N 7', 'Sat 8', 'C/N 8', 'Sat 9', 
                    'C/N 9', 'Sat 10', 'C/N 10', 'Sat 11', 'C/N 11', 'Sat 12', 
                    'C/N 12', 'Mort. Status', 'Activity', 'Main', 'Beacon', 'Temp', 
                    'Easting', 'Northing', 'AnimalID', 'GroupID', 'fileName',
                    'SerialNo', 'ET', 'Animal_ID')

# turn date and time columns into actual date and time
newdat2$DateTime_LMT <- mdy_hms(paste(newdat2$`LMT Date`, newdat2$`LMT Time`, sep = ' '))
# force_tz() assigns a given time zone and doesn't change the numbers (with_tz and tz = as an argument assumes the given time zone is GMT and converts it)
# I found the time zone by looking up the collar serial numbers on the Vectronic website
newdat2$DateTime_LMT <- as.character(force_tz(newdat2$DateTime_LMT, tz = 'MST'))

# create a GMT datetime column
newdat2$GMT <- as.character(with_tz(newdat2$DateTime_LMT, tz = 'GMT'))
# split GMT into GMT_Date and GMT_Time
newdat2$GMT_Date <- str_split_fixed(newdat2$GMT, ' ', 2)[,1]
newdat2$GMT_Time <- str_split_fixed(newdat2$GMT, ' ', 2)[,2]

# rename columns according to format in Hard Downloads Table in SAD
#names(existing)
## some of the column names are from the linked capture and necropsy tables so this won't include all the header column names
newdat2 <- newdat2 %>% 
  dplyr::rename(
    LMT_Date = `LMT Date`,
    LMT_Time = `LMT Time`,
    ECEF_X = `ECEF X`,
    ECEF_Y = `ECEF Y`,
    ECEF_Z = `ECEF Z`,
    LAT = `Latitude`,
    LONG = `Longitude`,
    ALT = Height,
    FixStatus = FixType,
    Satsused = Sats,
    SatNo1 = `Sat 1`,
    CNdb1 = `C/N 1`,
    SatNo2 = `Sat 2`,
    CNdb2 = `C/N 2`,
    SatNo3 = `Sat 3`,
    CNdb3 = `C/N 3`,
    SatNo4 = `Sat 4`,
    CNdb4 = `C/N 4`,
    SatNo5 = `Sat 5`,
    CNdb5 = `C/N 5`,
    SatNo6 = `Sat 6`,
    CNdb6 = `C/N 6`,
    SatNo7 = `Sat 7`,
    CNdb7 = `C/N 7`,
    SatNo8 = `Sat 8`,
    CNdb8 = `C/N 8`,
    SatNo9 = `Sat 9`,
    CNdb9 = `C/N 9`,
    SatNo10 = `Sat 10`,
    CNdb10 = `C/N 10`,
    SatNo11 = `Sat 11`,
    CNdb11 = `C/N 11`,
    SatNo12 = `Sat 12`,
    CNdb12 = `C/N 12`,
    MortStatus = `Mort. Status`,
    Main_V = Main,
    Beacon_v = Beacon,
    Temperature_C = `Temp`,
    SCTS_Date = `SCTS Date`,
    SCTS_Time = `SCTS Time`, 
    LMT = DateTime_LMT,
    Collar_Serial_No = SerialNo
  ) %>% 
  select(-c('AnimalID', 'GroupID', 'No', '3D Error', 'CollarID'))

####-------------------------------------------------------------
#### Type 3 are all VITs and fawn collars

####----------------------------------------------------------------
#### Type 4 (Vectronic)

filenames4 <- txts[txts$type == 4, 3]

newdat4 <- 
  do.call(rbind, lapply(filenames4, function(x) {
    dat4 <- read_fwf(x, skip = 3, col_types = cols(.default = 'c'), fwf_widths(
      # column widths
      c(8, 9, 11, 13, 11, 13, 13, 11, 13, 10, 10, 10, 13, 13, 9, 5, 14, 9, 6, 4, 4, 
        4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 28, 10, 6, 
        7, 6, 21, 21)
    ))
    dat4$fileName <- tools::file_path_sans_ext(basename(x))
    dat4
  }))


newdat4$SerialNo <- str_split_fixed(newdat4$fileName, '_', n=2)[,1] #extract serial no from filename
newdat4$ET <- str_split_fixed(newdat4$fileName, '_', n=2)[,2] # extract ET# from filename
newdat4$Animal_ID <- gsub('ET', '', newdat4$ET) # remove 'ET' from ET#

# set column headers
names(newdat4) <- c( 'No', 'CollarID', 'UTC Date', 'UTC Time', 'LMT Date', 
                     'LMT Time', 'Origin', 'SCTS Date', 'SCTS Time', 'ECEF X', 
                     'ECEF Y', 'ECEF Z', 'Latitude', 'Longitude', 'Height', 
                     'DOP', 'FixType', '3D Error', 'Sats', 
                     'Sat 1',  'C/N 1', 'Sat 2', 'C/N 2', 
                     'Sat 3', 'C/N 3', 'Sat 4', 'C/N 4', 'Sat 5', 'C/N 5', 
                     'Sat 6', 'C/N 6', 'Sat 7', 'C/N 7', 'Sat 8', 'C/N 8', 'Sat 9', 
                     'C/N 9', 'Sat 10', 'C/N 10', 'Sat 11', 'C/N 11', 'Sat 12', 
                     'C/N 12',
                     'Mort. Status', 'Activity', 'Main', 'Beacon', 'Temp', 
                     'Easting', 'Northing', 'fileName', 'SerialNo', 'ET', 
                     'Animal_ID')

newdat4$DateTime_UTC <- as.character(mdy_hms(paste(newdat4$`UTC Date`, newdat4$`UTC Time`,
                                                   sep = ' '), tz = 'GMT'))

# make LMT column but don't set tz yet
newdat4$DateTime_LMT <- as.character(mdy_hms(paste(newdat4$`LMT Date`, newdat4$`LMT Time`,
                                                   sep = " ")))

newdat4 <- newdat4 %>%
  dplyr::rename(
    GMT_Date = `UTC Date`,
    GMT_Time = `UTC Time`,
    LMT_Date = `LMT Date`,
    LMT_Time = `LMT Time`,
    ECEF_X = `ECEF X`,
    ECEF_Y = `ECEF Y`,
    ECEF_Z = `ECEF Z`,
    LAT = Latitude,
    LONG = Longitude,
    ALT = Height,
    FixStatus = FixType,
    Satsused = `Sats`,
    SatNo1 = `Sat 1`,
    CNdb1 = `C/N 1`,
    SatNo2 = `Sat 2`,
    CNdb2 = `C/N 2`,
    SatNo3 = `Sat 3`,
    CNdb3 = `C/N 3`,
    SatNo4 = `Sat 4`,
    CNdb4 = `C/N 4`,
    SatNo5 = `Sat 5`,
    CNdb5 = `C/N 5`,
    SatNo6 = `Sat 6`,
    CNdb6 = `C/N 6`,
    SatNo7 = `Sat 7`,
    CNdb7 = `C/N 7`,
    SatNo8 = `Sat 8`,
    CNdb8 = `C/N 8`,
    SatNo9 = `Sat 9`,
    CNdb9 = `C/N 9`,
    SatNo10 = `Sat 10`,
    CNdb10 = `C/N 10`,
    SatNo11 = `Sat 11`,
    CNdb11 = `C/N 11`,
    SatNo12 = `Sat 12`,
    CNdb12 = `C/N 12`,
    MortStatus = `Mort. Status`,
    Main_V = Main,
    Beacon_v = Beacon,
    Temperature_C = Temp,
    SCTS_Date = `SCTS Date`,
    SCTS_Time = `SCTS Time`,
    LMT = DateTime_LMT,
    GMT = DateTime_UTC,
    Collar_Serial_No = SerialNo
  ) %>% 
  # remove variables that aren't in existing
  select(-c('3D Error', 'No', 'CollarID'))


####-------------------------------------------------
#### Type 5 (Vectronic)

filenames5 <- txts[txts$type == 5, 3]

newdat5 <- 
  do.call(rbind, lapply(filenames5, function(x) {
    dat5 <- read_fwf(x, skip = 3, col_types = cols(.default = 'c'), fwf_widths(
      # column widths
      c(8,9, 11, 13, 11, 13, 13, 11, 13, 10, 10, 10, 13, 13, 9, 5, 14, 9, 6, 4, 4, 
        4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 29, 9, 6, 
        7, 5, 21, 21, 11, 7)
    ))
    dat5$fileName <- tools::file_path_sans_ext(basename(x))
    dat5
  }))

newdat5$SerialNo <- str_split_fixed(newdat5$fileName, '_', n=2)[,1] #extract serial no from filename
newdat5$ET <- str_split_fixed(newdat5$fileName, '_', n=2)[,2] # extract ET# from filename
newdat5$Animal_ID <- gsub('ET', '', newdat5$ET) # remove 'ET' from ET#

# set column headers
names(newdat5) <- c( 'No', 'CollarID', 'UTC Date', 'UTC Time', 'LMT Date', 
                     'LMT Time', 'Origin', 'SCTS Date', 'SCTS Time', 'ECEF X', 
                     'ECEF Y', 'ECEF Z', 'Latitude', 'Longitude', 'Height', 
                     'DOP', 'FixType', '3D Error', 'Sats', 
                     'Sat 1',  'C/N 1', 'Sat 2', 'C/N 2', 
                     'Sat 3', 'C/N 3', 'Sat 4', 'C/N 4', 'Sat 5', 'C/N 5', 
                     'Sat 6', 'C/N 6', 'Sat 7', 'C/N 7', 'Sat 8', 'C/N 8', 'Sat 9', 
                     'C/N 9', 'Sat 10', 'C/N 10', 'Sat 11', 'C/N 11', 'Sat 12', 'C/N 12',   
                     'Mort. Status', 'Activity', 'Main', 'Beacon', 'Temp', 
                     'Easting', 'Northing', 'AnimalID', 'GroupID', 'fileName', 
                     'SerialNo', 'ET', 'Animal_ID')

# convert these to characters
newdat5$DateTime_GMT <- as.character(mdy_hms(paste(newdat5$`UTC Date`, newdat5$`UTC Time`,
                                                   sep = ' '), tz = 'GMT'))
newdat5$DateTime_LMT <- as.character(mdy_hms(paste(newdat5$`LMT Date`, newdat5$`LMT Time`,
                                                   sep = ' ')))


newdat5 <- newdat5 %>%
  dplyr::rename(
    GMT_Date = `UTC Date`,
    GMT_Time = `UTC Time`,
    LMT_Date = `LMT Date`,
    LMT_Time = `LMT Time`,
    ECEF_X = `ECEF X`,
    ECEF_Y = `ECEF Y`,
    ECEF_Z = `ECEF Z`,
    LAT = `Latitude`,
    LONG = `Longitude`,
    ALT = Height,
    FixStatus = FixType,
    Satsused = `Sats`,
    SatNo1 = `Sat 1`,
    CNdb1 = `C/N 1`,
    SatNo2 = `Sat 2`,
    CNdb2 = `C/N 2`,
    SatNo3 = `Sat 3`,
    CNdb3 = `C/N 3`,
    SatNo4 = `Sat 4`,
    CNdb4 = `C/N 4`,
    SatNo5 = `Sat 5`,
    CNdb5 = `C/N 5`,
    SatNo6 = `Sat 6`,
    CNdb6 = `C/N 6`,
    SatNo7 = `Sat 7`,
    CNdb7 = `C/N 7`,
    SatNo8 = `Sat 8`,
    CNdb8 = `C/N 8`,
    SatNo9 = `Sat 9`,
    CNdb9 = `C/N 9`,
    SatNo10 = `Sat 10`,
    CNdb10 = `C/N 10`,
    SatNo11 = `Sat 11`,
    CNdb11 = `C/N 11`,
    SatNo12 = `Sat 12`,
    CNdb12 = `C/N 12`,
    MortStatus = `Mort. Status`,
    Main_V = Main,
    Beacon_v = Beacon,
    Temperature_C = `Temp`,
    SCTS_Date = `SCTS Date`,
    SCTS_Time = `SCTS Time`,
    GMT = DateTime_GMT,
    LMT = DateTime_LMT,
    Collar_Serial_No = SerialNo) %>%
  select(-c('3D Error', 'AnimalID', 'GroupID', 'No', '3D Error', 'CollarID'))


####---------------------------------------------------------------------
#### Type 6 (Lotek) 
####   -> these header rows are slightly different format but the actual data are in the same format

filenames6 <- txts[txts$type == 6, 3]

newdat6 <- 
  do.call(rbind, lapply(filenames6, function(x) {
    dat6 <- read_fwf(x, skip = 3, col_types = cols(.default = 'c'), fwf_widths(
      # column widths
      c(
        6, 11, 9, 11, 9, 10, 10, 10, 13, 13, 9, 5, 6, 10, 5, 4, 4, 4, 4, 4, 4,
        4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 6, 6, 4))
    )
    dat6$fileName <- tools::file_path_sans_ext(basename(x))
    dat6
  }))

newdat6$SerialNo <- str_split_fixed(newdat6$fileName, '_', n=2)[,1]
newdat6$ET <- str_split_fixed(newdat6$fileName, '_', n=2)[,2]
newdat6$Animal_ID <- gsub('ET', '', newdat6$ET)

# set column names
names(newdat6) <- c("No","GMT Date","GMT Time","LMT Date","LMT Time","ECEF X","ECEF Y",
                    "ECEF Z","Latitude(deg)","Longitude(deg)","Height", "DOP", "Nav", 
                    "Validated", "Sats used", "Sat No1", "C/N db1", "Sat No2", "C/N db2", 
                    "Sat No3", "C/N db3", "Sat No4", "C/N db4", "Sat No5", "C/N db5", "Sat No6", 
                    "C/N db6", "Sat No7", "C/N db7", "Sat No8", "C/N db8", "Sat No9", "C/N db9", 
                    "Sat No10", "C/N db10", "Sat No11", "C/N db11", "Sat No12", "C/N db12", "Main", 
                    "Bkup", "Temp(C)", "fileName", "SerialNo", "ET", "Animal_ID")

newdat6$DateTime_GMT <- as.character(dmy_hms(paste(newdat6$`GMT Date`, newdat6$`GMT Time`,
                                                   sep = ' '), tz = 'GMT'))

# make LMT but don't set tz yet
newdat6$DateTime_LMT <- as.character(dmy_hms(paste(newdat6$`LMT Date`, newdat6$`LMT Time`,
                                                   sep = ' ')))

newdat6 <- newdat6 %>%
  dplyr::rename(
    GMT_Date = `GMT Date`,
    GMT_Time = `GMT Time`,
    LMT_Date = `LMT Date`,
    LMT_Time = `LMT Time`,
    ECEF_X = `ECEF X`,
    ECEF_Y = `ECEF Y`,
    ECEF_Z = `ECEF Z`,
    LAT = `Latitude(deg)`,
    LONG = `Longitude(deg)`,
    ALT = Height,
    DOP = DOP,
    FixStatus = Nav,
    FixValidated = Validated,
    Satsused = `Sats used`,
    #Satellite numbers 
    SatNo1 = `Sat No1`,
    CNdb1 = `C/N db1`,
    SatNo2 = `Sat No2`,
    CNdb2 = `C/N db2`,
    SatNo3 = `Sat No3`,
    CNdb3 = `C/N db3`,
    SatNo4 = `Sat No4`,
    CNdb4 = `C/N db4`,
    SatNo5 = `Sat No5`,
    CNdb5 = `C/N db5`,
    SatNo6 = `Sat No6`,
    CNdb6 = `C/N db6`,
    SatNo7 = `Sat No7`,
    CNdb7 = `C/N db7`,
    SatNo8 = `Sat No8`,
    CNdb8 = `C/N db8`,
    SatNo9 = `Sat No9`,
    CNdb9 = `C/N db9`,
    SatNo10 = `Sat No10`,
    CNdb10 = `C/N db10`,
    SatNo11 = `Sat No11`,
    CNdb11 = `C/N db11`,
    SatNo12 = `Sat No12`,
    CNdb12 = `C/N db12`,
    Main_V = Main,
    Backup_v = Bkup,
    Temperature_C = `Temp(C)`,
    GMT = DateTime_GMT,
    LMT = DateTime_LMT,
    Collar_Serial_No = SerialNo
  ) %>%
  select(-c('No'))

####-------------------------------------------------------------------
#### Type 7 (ATS)

filenames7 <- txts[txts$type == 7, 3]

newdat7 <- 
  do.call(rbind, lapply(filenames7, function(x) {
    dat7 <- read.table(x, sep = ',', skip = 1, fill = T)
    dat7$fileName <- tools::file_path_sans_ext(basename(x))
    dat7
  }))


newdat7$SerialNo <- str_split_fixed(newdat7$fileName, '_', n=2)[,1]
newdat7$ET <- str_split_fixed(newdat7$fileName,'_', n=2)[,2]
newdat7$Animal_ID <- gsub('ET', '', newdat7$ET)

names(newdat7) <- c('Year', 'JulianDate', 'Hour', 'Minute', 'Status', 'Activity',
                     'Temperature_C', 'BlockNumber', 'Latitude', 'Longitude', 'DOP',
                     'Satsused', 'Duration_sec', 'FixStatus', 'fileName', 'Collar_Serial_No', 'ET',
                     'Animal_ID')

# convert julian date to mdy
# need to set origin as january 1 of that year (all these data were from 2019, 2020 and 2021)
newdat7$Origin <- NA
newdat7[newdat7$Year == 19,]$Origin <- '2019-01-01'
newdat7[newdat7$Year == 20,]$Origin <- '2020-01-01'
newdat7[newdat7$Year == 21,]$Origin <- '2021-01-01'

# for some reason this function shifts dates by 1 (ie JulDate 20 becomes January 21)
#   so need to subtract 1 from Julian date to convert
newdat7$LMT_Date <- as.Date((newdat7$JulianDate - 1), origin = newdat7$Origin)

# remove errors 
newdat7 <- newdat7[!is.na(newdat7$LMT_Date),]

## determine time zone
#   this runs a code to look at all of the ATS collars (only from 2020 and 2021) and determines the collar's
source('K:/Wildlife/Fogel/Collar Data Processing/ATS Time Zones/ATS_Time_Zones.R')
# MST contains collars in MST
# PST contains collars in PST

# initialize time zone as Error
newdat7$tz <- 'Error'

# assign time zone by looking in the MST20 etc vectors for each serial number
newdat7[newdat7$Collar_Serial_No %in% MST,]$tz <- 'MST'
newdat7[newdat7$Collar_Serial_No %in% PST,]$tz <- 'PST'
#newdat7[newdat7$Collar_Serial_No %in% CST,]$tz <- 'CST' # none are in CST

## put all ones with missing tz into one dataframe
newdat7MISSING.TZ <- newdat7[newdat7$tz == 'Error',]
# split remaining data into different dfs, one for PST one for MST
newdat7MST <- newdat7[newdat7$tz == 'MST',]
newdat7PST <- newdat7[newdat7$tz == 'PST',]

# combine Date, hour, and minute into DateTime object
newdat7MST$LMT <- ymd_hm(paste(newdat7MST$LMT_Date, newdat7MST$Hour, newdat7MST$Minute))
newdat7PST$LMT <- ymd_hm(paste(newdat7PST$LMT_Date, newdat7PST$Hour, newdat7PST$Minute))

# MST
newdat7MST$LMT <- force_tz(newdat7MST$LMT, tz = 'MST')
# PST -- it wouldn't recognize PST for me for some reason
newdat7PST$LMT <- force_tz(newdat7PST$LMT, tz = 'America/Los_Angeles')

## convert LMT to GMT 
newdat7MST$GMT <- as.character(with_tz(newdat7MST$LMT, tz = 'GMT'))
newdat7PST$GMT <- as.character(with_tz(newdat7PST$LMT, tz = 'GMT'))

# combine back into one dataframe
newdat7 <- rbind(newdat7MST, newdat7PST)

# convert LMT to character
newdat7$LMT <- as.character(newdat7$LMT)

# extract date and time from datetime (LMT_Date already exists)
newdat7$LMT_Time <- str_split_fixed(newdat7$LMT, ' ', 2)[,2]
newdat7$GMT_Date <- str_split_fixed(newdat7$GMT, ' ', 2)[,1]
newdat7$GMT_Time <- str_split_fixed(newdat7$GMT, ' ', 2)[,2]

# rename and clean columns 
newdat7 <- newdat7 %>%
  dplyr::rename(LAT = `Latitude`,
                LONG = `Longitude`) %>%
  select(-c('Year', 'JulianDate', 'Hour', 'Minute', 'Status', 'BlockNumber', 'tz', 'Origin'))


####-------------------------------------------------
#### Type 8 (Vectronic, tab separated)

# vector of txt files, retain file paths
filenames8 <- txts[txts$type == 8, 3]

# import collar data for that type 
newdat8 <- 
  do.call(rbind, lapply(filenames8, function(x) {
    # read_fwf is read fixed width file
    dat8 <- read.table(x, sep = '\t', skip = 1)
    # variable of fileName
    dat8$fileName <- tools::file_path_sans_ext(basename(x))
    dat8
  }))

# check to make sure it read in properly
#head(newdat8)

# split fileName into serialno and ET#
newdat8$SerialNo <- str_split_fixed(newdat8$fileName, '_', n=2)[,1] # extract SerialNo from fileName
newdat8$ET <- str_split_fixed(newdat8$fileName, '_', n=2)[,2] # extract ET# from fileName
newdat8$Animal_ID <- gsub('ET', '', newdat8$ET) # remove 'ET' from ET#

# set column headers based on existing 
names(newdat8) <- c('No', 'CollarID', 'LMT Date', 'LMT Time', 'Origin', 
                    'SCTS Date', 'SCTS Time', 'ECEF X', 'ECEF Y', 'ECEF Z', 
                    'Latitude', 'Longitude', 'Height','DOP','FixType', 
                    '3D Error',  'Sats', 'Sat 1',  'C/N 1', 'Sat 2', 'C/N 2', 
                    'Sat 3', 'C/N 3', 'Sat 4', 'C/N 4', 'Sat 5', 'C/N 5', 
                    'Sat 6', 'C/N 6', 'Sat 7', 'C/N 7', 'Sat 8', 'C/N 8', 'Sat 9', 
                    'C/N 9', 'Sat 10', 'C/N 10', 'Sat 11', 'C/N 11', 'Sat 12', 
                    'C/N 12', 'Mort. Status', 'Activity', 'Main', 'Beacon', 'Temp', 
                    'Easting', 'Northing', 'AnimalID', 'GroupID', 'fileName',
                    'SerialNo', 'ET', 'Animal_ID')

# turn date and time columns into actual date and time
newdat8$DateTime_LMT <- mdy_hms(paste(newdat8$`LMT Date`, newdat8$`LMT Time`, sep = ' '))
# force_tz() assigns a given time zone and doesn't change the numbers (with_tz and tz = as an argument assumes the given time zone is GMT and converts it)
# I found the time zone by looking up the collar serial numbers on the Vectronic website
newdat8$DateTime_LMT <- as.character(force_tz(newdat8$DateTime_LMT, tz = 'MST'))

# create a GMT datetime column
newdat8$GMT <- as.character(with_tz(newdat8$DateTime_LMT, tz = 'GMT'))
# split GMT into GMT_Date and GMT_Time
newdat8$GMT_Date <- str_split_fixed(newdat8$GMT, ' ', 2)[,1]
newdat8$GMT_Time <- str_split_fixed(newdat8$GMT, ' ', 2)[,2]

# rename columns according to format in Hard Downloads Table in SAD
#names(existing)
## some of the column names are from the linked capture and necropsy tables so this won't include all the header column names
newdat8 <- newdat8 %>% 
  dplyr::rename(
    LMT_Date = `LMT Date`,
    LMT_Time = `LMT Time`,
    ECEF_X = `ECEF X`,
    ECEF_Y = `ECEF Y`,
    ECEF_Z = `ECEF Z`,
    LAT = `Latitude`,
    LONG = `Longitude`,
    ALT = Height,
    FixStatus = FixType,
    Satsused = Sats,
    SatNo1 = `Sat 1`,
    CNdb1 = `C/N 1`,
    SatNo2 = `Sat 2`,
    CNdb2 = `C/N 2`,
    SatNo3 = `Sat 3`,
    CNdb3 = `C/N 3`,
    SatNo4 = `Sat 4`,
    CNdb4 = `C/N 4`,
    SatNo5 = `Sat 5`,
    CNdb5 = `C/N 5`,
    SatNo6 = `Sat 6`,
    CNdb6 = `C/N 6`,
    SatNo7 = `Sat 7`,
    CNdb7 = `C/N 7`,
    SatNo8 = `Sat 8`,
    CNdb8 = `C/N 8`,
    SatNo9 = `Sat 9`,
    CNdb9 = `C/N 9`,
    SatNo10 = `Sat 10`,
    CNdb10 = `C/N 10`,
    SatNo11 = `Sat 11`,
    CNdb11 = `C/N 11`,
    SatNo12 = `Sat 12`,
    CNdb12 = `C/N 12`,
    MortStatus = `Mort. Status`,
    Main_V = Main,
    Beacon_v = Beacon,
    Temperature_C = `Temp`,
    SCTS_Date = `SCTS Date`,
    SCTS_Time = `SCTS Time`, 
    LMT = DateTime_LMT,
    Collar_Serial_No = SerialNo
  ) %>% 
  select(-c('AnimalID', 'GroupID', 'No', '3D Error', 'CollarID'))


####----------------------------------------------------------------
#### Type 9 (Vectronic, tab separated)

filenames9 <- txts[txts$type == 9, 3]

newdat9 <- 
  do.call(rbind, lapply(filenames9, function(x) {
    dat9 <- read.table(x, sep = '\t', skip = 1)
    dat9$fileName <- tools::file_path_sans_ext(basename(x))
    dat9
  }))


newdat9$SerialNo <- str_split_fixed(newdat9$fileName, '_', n=2)[,1] #extract serial no from filename
newdat9$ET <- str_split_fixed(newdat9$fileName, '_', n=2)[,2] # extract ET# from filename
newdat9$Animal_ID <- gsub('ET', '', newdat9$ET) # remove 'ET' from ET#

# set column headers
names(newdat9) <- c( 'XNo', 'CollarID', 'UTC Date', 'UTC Time', 'LMT Date', 
                     'LMT Time', 'Origin', 'SCTS Date', 'SCTS Time', 'ECEF X', 
                     'ECEF Y', 'ECEF Z', 'Latitude', 'Longitude', 'Height', 
                     'DOP', 'FixType1', 
                     'FixType2', # data has a tab in the middle of this column so it gets split into two
                     '3D Error', 'Sats', 
                     'Sat 1',  'C/N 1', 'Sat 2', 'C/N 2', 
                     'Sat 3', 'C/N 3', 'Sat 4', 'C/N 4', 'Sat 5', 'C/N 5', 
                     'Sat 6', 'C/N 6', 'Sat 7', 'C/N 7', 'Sat 8', 'C/N 8', 'Sat 9', 
                     'C/N 9', 'Sat 10', 'C/N 10', 'Sat 11', 'C/N 11', 'Sat 12', 
                     'C/N 12',
                     'Mort. Status', 'Activity', 'Main', 'Beacon', 'Temp', 
                     'Easting', 'Northing', 'fileName', 'SerialNo', 'ET', 
                     'Animal_ID')

newdat9 <- newdat9 %>%
  unite('FixType', FixType1:FixType2, remove = T, sep = ' ')

newdat9$DateTime_UTC <- as.character(mdy_hms(paste(newdat9$`UTC Date`, newdat9$`UTC Time`,
                                                   sep = ' '), tz = 'GMT'))

# make LMT column but don't set tz yet
newdat9$DateTime_LMT <- as.character(mdy_hms(paste(newdat9$`LMT Date`, newdat9$`LMT Time`,
                                                   sep = " ")))

newdat9 <- newdat9 %>%
  dplyr::rename(
    GMT_Date = `UTC Date`,
    GMT_Time = `UTC Time`,
    LMT_Date = `LMT Date`,
    LMT_Time = `LMT Time`,
    ECEF_X = `ECEF X`,
    ECEF_Y = `ECEF Y`,
    ECEF_Z = `ECEF Z`,
    LAT = Latitude,
    LONG = Longitude,
    ALT = Height,
    FixStatus = FixType,
    Satsused = `Sats`,
    SatNo1 = `Sat 1`,
    CNdb1 = `C/N 1`,
    SatNo2 = `Sat 2`,
    CNdb2 = `C/N 2`,
    SatNo3 = `Sat 3`,
    CNdb3 = `C/N 3`,
    SatNo4 = `Sat 4`,
    CNdb4 = `C/N 4`,
    SatNo5 = `Sat 5`,
    CNdb5 = `C/N 5`,
    SatNo6 = `Sat 6`,
    CNdb6 = `C/N 6`,
    SatNo7 = `Sat 7`,
    CNdb7 = `C/N 7`,
    SatNo8 = `Sat 8`,
    CNdb8 = `C/N 8`,
    SatNo9 = `Sat 9`,
    CNdb9 = `C/N 9`,
    SatNo10 = `Sat 10`,
    CNdb10 = `C/N 10`,
    SatNo11 = `Sat 11`,
    CNdb11 = `C/N 11`,
    SatNo12 = `Sat 12`,
    CNdb12 = `C/N 12`,
    MortStatus = `Mort. Status`,
    Main_V = Main,
    Beacon_v = Beacon,
    Temperature_C = Temp,
    SCTS_Date = `SCTS Date`,
    SCTS_Time = `SCTS Time`,
    LMT = DateTime_LMT,
    GMT = DateTime_UTC,
    Collar_Serial_No = SerialNo
  ) %>% 
  # remove variables that aren't in existing
  select(-c('3D Error', 'XNo', 'CollarID'))


####-------------------------------------------------
#### Type 10 (Vectronic, tab separated)

filenames10 <- txts[txts$type == 10, 3]

newdat10 <- 
  do.call(rbind, lapply(filenames10, function(x) {
    dat10 <- read.table(x, sep = '\t', skip = 1)
    dat10$fileName <- tools::file_path_sans_ext(basename(x))
    dat10
  }))

newdat10$SerialNo <- str_split_fixed(newdat10$fileName, '_', n=2)[,1] #extract serial no from filename
newdat10$ET <- str_split_fixed(newdat10$fileName, '_', n=2)[,2] # extract ET# from filename
newdat10$Animal_ID <- gsub('ET', '', newdat10$ET) # remove 'ET' from ET#

# set column headers
names(newdat10) <- c( 'No', 'CollarID', 'UTC Date', 'UTC Time', 'LMT Date', 
                     'LMT Time', 'Origin', 'SCTS Date', 'SCTS Time', 'ECEF X', 
                     'ECEF Y', 'ECEF Z', 'Latitude', 'Longitude', 'Height', 
                     'DOP', 'FixType1', 'FixType2', # need two fixtype columns bc split into two columns when data are read in
                     '3D Error', 'Sats', 
                     'Sat 1',  'C/N 1', 'Sat 2', 'C/N 2', 
                     'Sat 3', 'C/N 3', 'Sat 4', 'C/N 4', 'Sat 5', 'C/N 5', 
                     'Sat 6', 'C/N 6', 'Sat 7', 'C/N 7', 'Sat 8', 'C/N 8', 'Sat 9', 
                     'C/N 9', 'Sat 10', 'C/N 10', 'Sat 11', 'C/N 11', 'Sat 12', 'C/N 12',   
                     'Mort. Status', 'Activity', 'Main', 'Beacon', 'Temp', 
                     'Easting', 'Northing', 'AnimalID', 'GroupID', 'fileName', 
                     'SerialNo', 'ET', 'Animal_ID')

newdat10 <- newdat10 %>% 
  unite('FixType', FixType1:FixType2, remove = T, sep = ' ')

# convert these to characters
newdat10$DateTime_GMT <- as.character(mdy_hms(paste(newdat10$`UTC Date`, newdat10$`UTC Time`,
                                                   sep = ' '), tz = 'GMT'))
newdat10$DateTime_LMT <- as.character(mdy_hms(paste(newdat10$`LMT Date`, newdat10$`LMT Time`,
                                                   sep = ' ')))


newdat10 <- newdat10 %>%
  dplyr::rename(
    GMT_Date = `UTC Date`,
    GMT_Time = `UTC Time`,
    LMT_Date = `LMT Date`,
    LMT_Time = `LMT Time`,
    ECEF_X = `ECEF X`,
    ECEF_Y = `ECEF Y`,
    ECEF_Z = `ECEF Z`,
    LAT = `Latitude`,
    LONG = `Longitude`,
    ALT = Height,
    FixStatus = FixType,
    Satsused = `Sats`,
    SatNo1 = `Sat 1`,
    CNdb1 = `C/N 1`,
    SatNo2 = `Sat 2`,
    CNdb2 = `C/N 2`,
    SatNo3 = `Sat 3`,
    CNdb3 = `C/N 3`,
    SatNo4 = `Sat 4`,
    CNdb4 = `C/N 4`,
    SatNo5 = `Sat 5`,
    CNdb5 = `C/N 5`,
    SatNo6 = `Sat 6`,
    CNdb6 = `C/N 6`,
    SatNo7 = `Sat 7`,
    CNdb7 = `C/N 7`,
    SatNo8 = `Sat 8`,
    CNdb8 = `C/N 8`,
    SatNo9 = `Sat 9`,
    CNdb9 = `C/N 9`,
    SatNo10 = `Sat 10`,
    CNdb10 = `C/N 10`,
    SatNo11 = `Sat 11`,
    CNdb11 = `C/N 11`,
    SatNo12 = `Sat 12`,
    CNdb12 = `C/N 12`,
    MortStatus = `Mort. Status`,
    Main_V = Main,
    Beacon_v = Beacon,
    Temperature_C = `Temp`,
    SCTS_Date = `SCTS Date`,
    SCTS_Time = `SCTS Time`,
    GMT = DateTime_GMT,
    LMT = DateTime_LMT,
    Collar_Serial_No = SerialNo) %>%
  select(-c('3D Error', 'AnimalID', 'GroupID', 'No', '3D Error', 'CollarID'))


####-------------------------------------------------
#### Type 10 (Vectronic, tab separated)

filenames11 <- txts[txts$type == 11, 3]

newdat11 <- 
  do.call(rbind, lapply(filenames11, function(x) {
    dat11 <- read.table(x, sep = ',', skip = 1, fill = T)
    dat11$fileName <- tools::file_path_sans_ext(basename(x))
    dat11
  }))


newdat11$SerialNo <- str_split_fixed(newdat11$fileName, '_', n=2)[,1]
newdat11$ET <- str_split_fixed(newdat11$fileName,'_', n=2)[,2]
newdat11$Animal_ID <- gsub('ET', '', newdat11$ET)

names(newdat11) <- c('CollarSerialNumber', 'Year', 'JulianDate', 'Hour', 'Minute', 'Activity',
                     'Temperature_C', 'Latitude', 'Longitude', 'DOP',
                     'Satsused', 'Duration_sec', '2D3D', 'LMT_Date', 'fileName', 'Collar_Serial_No', 'ET',
                     'Animal_ID')

# # convert julian date to mdy
# # need to set origin as january 1 of that year (all these data were from 2019, 2020 and 2021)
# newdat11$Origin <- NA
# if (sum(newdat11$Year == 19) > 0) newdat11[newdat11$Year == 19,]$Origin <- '2019-01-01'
# if (sum(newdat11$Year == 20) > 0) newdat11[newdat11$Year == 20,]$Origin <- '2020-01-01'
# if (sum(newdat11$Year == 21) > 0) newdat11[newdat11$Year == 21,]$Origin <- '2021-01-01'

# for some reason this function shifts dates by 1 (ie JulDate 20 becomes January 21)
#   so need to subtract 1 from Julian date to convert
newdat11$LMT_Date <- ymd(newdat11$LMT_Date)

# remove errors 
newdat11 <- newdat11[!is.na(newdat11$LMT_Date),]

## determine time zone
#   this runs a code to look at all of the ATS collars (only from 2020 and 2021) and determines the collar's
source('K:/Wildlife/Fogel/Collar Data Processing/ATS Time Zones/ATS_Time_Zones.R')
# MST contains collars in MST
# PST contains collars in PST

# initialize time zone as Error
newdat11$tz <- 'Error'

# assign time zone by looking in the MST20 etc vectors for each serial number
newdat11[newdat11$Collar_Serial_No %in% MST,]$tz <- 'MST'
newdat11[newdat11$Collar_Serial_No %in% PST,]$tz <- 'PST'
#newdat11[newdat11$Collar_Serial_No %in% CST,]$tz <- 'CST' # none are in CST

## put all ones with missing tz into one dataframe
newdat11MISSING.TZ <- newdat11[newdat11$tz == 'Error',]
# split remaining data into different dfs, one for PST one for MST
newdat11MST <- newdat11[newdat11$tz == 'MST',]
newdat11PST <- newdat11[newdat11$tz == 'PST',]

# combine Date, hour, and minute into DateTime object
newdat11MST$LMT <- ymd_hm(paste(newdat11MST$LMT_Date, newdat11MST$Hour, newdat11MST$Minute))
newdat11PST$LMT <- ymd_hm(paste(newdat11PST$LMT_Date, newdat11PST$Hour, newdat11PST$Minute))

# MST
newdat11MST$LMT <- force_tz(newdat11MST$LMT, tz = 'MST')
# PST -- it wouldn't recognize PST for me for some reason
newdat11PST$LMT <- force_tz(newdat11PST$LMT, tz = 'America/Los_Angeles')

## convert LMT to GMT 
newdat11MST$GMT <- as.character(with_tz(newdat11MST$LMT, tz = 'GMT'))
newdat11PST$GMT <- as.character(with_tz(newdat11PST$LMT, tz = 'GMT'))

# combine back into one dataframe
newdat11 <- rbind(newdat11MST, newdat11PST)

# convert LMT to character
newdat11$LMT <- as.character(newdat11$LMT)

# extract date and time from datetime (LMT_Date already exists)
newdat11$LMT_Time <- str_split_fixed(newdat11$LMT, ' ', 2)[,2]
newdat11$GMT_Date <- str_split_fixed(newdat11$GMT, ' ', 2)[,1]
newdat11$GMT_Time <- str_split_fixed(newdat11$GMT, ' ', 2)[,2]

# rename and clean columns 
newdat11 <- newdat11 %>%
  dplyr::rename(LAT = `Latitude`,
                LONG = `Longitude`) %>%
  select(-c('CollarSerialNumber', 'Year', 'JulianDate', 'Hour', 'Minute', '2D3D', 'tz'))




####-------------------------------------------------------------
#### Combine all txt files into single data frame

## rbind 2, 6 and 7 into vec_txt 
vec_txt <- newdat4 # rbind.fill fills missing columns with NAs
lotek_txt <- newdat6
ats_txt <- newdat7

# add collar makes 
vec_txt$CollarMake <- 'Vectronic'
lotek_txt$CollarMake <- 'Lotek'
ats_txt$CollarMake <- 'ATS'

## rbind all txts together
all_txt <- plyr::rbind.fill(lotek_txt, ats_txt, vec_txt)

# clean mort status (change any text to 1 or 0)
all_txt[!is.na(all_txt$MortStatus) & all_txt$MortStatus == 'normal',]$MortStatus <- 0
all_txt[!is.na(all_txt$MortStatus) & all_txt$MortStatus == 'Mortality no radius',]$MortStatus <- 1
all_txt[!is.na(all_txt$MortStatus) & all_txt$MortStatus == 'N/A',]$MortStatus <- NA

## check for column names that are in downloaded data and aren't in all_txt
##    (make sure none of them should have been imported)
# names(existing[, !(names(existing) %in% names(all_txt))])
# names(all_txt[, !(names(all_txt) %in% names(existing))])

## check for ones where GMT and LMT are exact same (and need to have time zones altered)
all_txt$TimeZoneFlag <- ifelse(all_txt$GMT == all_txt$LMT, 1, 0)
all_txt$TimeZoneFlag_Reason <- ifelse(all_txt$GMT == all_txt$LMT, 'GMT and LMT were identical', NA)


# remove individual tables and collar make ones to free up memory space
#rm(newdat2, newdat4, newdat5, newdat6, newdat7, newdat8, newdat9, newdat10,
#   vec_txt, lotek_txt, ats_txt)

# check how many txts are VITs/morts/etc
# sum(txts$type == 1 | txts$type == 3 |txts$type == 4 | txts$type == 5 | txts$type == 9 | txts$type == 10 |txts$type == 11 | txts$type == 13 | txts$type == 14)
# 123












































