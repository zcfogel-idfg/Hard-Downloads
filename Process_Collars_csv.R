################################################################################
### PROCESS CSV COLLARS (IMPORT, STANDARDIZE, AND COMPILE INTO SINGLE TABLE) ###
################################################################################

####-----------------------------------------------------------------------
#### Set Working Directory
setwd('K:/Wildlife/Fogel/Collar Data Processing/Collar csvs new')

# I couldn't figure out how to sort them programmatically so I had to sort them by hand 

####------------------------------------------------------------------------
#### Type 1 (Vectronic)

csvnames1 <- list.files('csv1')
csvnames1 <- paste0('csv1/', csvnames1) # add folder name so the next function can read them in

csvnames1b <- paste0('csv1b/', list.files('csv1b'))

csvnames1 <- c(csvnames1, csvnames1b)

csvdat1 <- csvnames1 %>%
  set_names() %>% # set up filename character string for .id argument in map_df() I don't know what this is but it was in Brandon's code
  # read in each csv, ignoring column headers, with every column read in as character data type
  map_df(read_csv, skip = 1, col_names = F, col_types = cols(.default = 'c'), 
         .id = 'fileName') %>%
  dplyr::rename(No = X1,
                CollarID = X2,
                UTC_Date = X3,
                UTC_Time = X4,
                LMT_Date = X5,
                LMT_Time = X6,
                SCTS_Date = X7,
                SCTS_Time = X8,
                `ECEF_X [m]` = X9,
                `ECEF_Y [m]` = X10,
                `ECEF_Z [m]` = X11,
                Latitude = X12,
                Longitude = X13,
                FixType = X14,
                `3D_Error [m]` = X15,
                `Mort. Status` = X16,
                Activity = X17,
                `Main [V]` = X18,
                `Beacon [V]` = X19,
                `Temp [°C]` = X20,
                Easting = X21,
                Northing = X22)

# remove csv1/ and .csv from fileName
csvdat1$fileName <- tools::file_path_sans_ext(basename(csvdat1$fileName))

# split fileName into collar serial and animal ID
csvdat1$SerialNo <- str_split_fixed(csvdat1$fileName, '_', n=2)[,1] # extract SerialNo from fileName
csvdat1$ET <- str_split_fixed(csvdat1$fileName, '_', n=2)[,2] # extract ET# from fileName
csvdat1$Animal_ID <- gsub('ET', '', csvdat1$ET) # remove 'ET' from ET#

csvdat1$DateTime_GMT <- as.character(mdy_hms(paste(csvdat1$UTC_Date, csvdat1$UTC_Time), tz = 'GMT'))
# check tz -- listed as same as GMT?
csvdat1$DateTime_LMT <- as.character(mdy_hms(paste(csvdat1$LMT_Date, csvdat1$LMT_Time), tz = 'GMT'))
# put dates in lubridate format then back to text
csvdat1$UTC_Date <- as.character(mdy(csvdat1$UTC_Date))
csvdat1$LMT_Date <- as.character(mdy(csvdat1$LMT_Date))

## rename columns for binding with existing database
names(csvdat1) <- c('fileName', 'XNo', 'XCollarID', 'GMT_Date', 'GMT_Time', 'LMT_Date',
                    'LMT_Time', 'SCTS_Date', 'SCTS_Time', 'ECEF_X', 'ECEF_Y', 
                    'ECEF_Z', 'LAT', 'LONG', 'FixStatus', 'X3D_Error', 'MortStatus',
                    'Activity',
                    'Main_V', 'Beacon_v', 'Temperature_C', 'Easting', 'Northing',
                    'Collar_Serial_No', 'ET', 'Animal_ID', 'GMT', 'LMT')

# get rid of extra columns
csvdat1 <- csvdat1 %>%
  select(-c('XNo', 'XCollarID', 'X3D_Error'))

csvdat1$CollarMake <- 'Vectronic'

####----------------------------------------------------------------
#### Type 2 (Vectronic)


csvnames2 <- list.files('csv2')
csvnames2 <- paste0('csv2/', csvnames2)

csvnames2b <- paste0('csv2b/', list.files('csv2b'))
csvnames2 <- c(csvnames2, csvnames2b)

csvdat2 <- csvnames2 %>%
  set_names() %>% 
  map_df(read_csv, skip = 1, col_names = F, col_types = cols(.default = 'c'),
         .id = 'fileName') %>%
  dplyr::rename(No = X1, 
                CollarID = X2, 
                UTC_Date = X3, 
                UTC_Time = X4, 
                LMT_Date = X5, 
                LMT_Time = X6, 
                SCTS_Date = X7, 
                SCTS_Time = X8, 
                `ECEF_X [m]`  = X9, 
                `CEF_Y [m]` = X10, 
                `ECEF_Z [m]` = X11, 
                `Latitude [°]` = X12, 
                `Longitude [°]` = X13, 
                `Height [m]` = X14,	
                DOP = X15, 
                FixType = X16, 
                `3D_Error [m]` = X17, 
                Sats = X18,	
                `Sat No1` = X19,	
                `C/N db1` = X20,	
                `Sat No2` = X21,	
                `C/N db2` = X22,	
                `Sat No3` = X23,	
                `C/N db3` = X24,	
                `Sat No4` = X25,	
                `C/N db4` = X26, 
                `Sat No5` = X27, 
                `C/N db5` = X28, 
                `Sat No6` = X29, 
                `C/N db6` = X30, 
                `Sat No7` = X31, 
                `C/N db7` = X32, 
                `Sat No8` = X33, 
                `C/N db8` = X34,	
                `Sat No9` = X35,	
                `C/N db9` = X36,	
                `Sat No10` = X37,	
                `C/N db10` = X38,	
                `Sat No11` = X39,	
                `C/N db11` = X40,	
                `Sat No12` = X41,	
                `C/N db12` = X42,	
                `Mort. Status` = X43, 
                Activity = X44, 
                `Main [V]` = X45, 
                `Beacon [V]` = X46, 
                `Temp [°C]` = X47, 
                Easting = X48, 
                Northing = X49)

csvdat2$fileName <- tools::file_path_sans_ext(basename(csvdat2$fileName))

# split fileName into collar serial and animal ID
csvdat2$SerialNo <- str_split_fixed(csvdat2$fileName, '_', n=2)[,1] # extract SerialNo from fileName
csvdat2$ET <- str_split_fixed(csvdat2$fileName, '_', n=2)[,2] # extract ET# from fileName
csvdat2$Animal_ID <- gsub('ET', '', csvdat2$ET) # remove 'ET' from ET#

csvdat2$DateTime_GMT <- as.character(mdy_hms(paste(csvdat2$UTC_Date, csvdat2$UTC_Time), tz = 'GMT'))
# check tz -- listed as same as GMT?
csvdat2$DateTime_LMT <- as.character(mdy_hms(paste(csvdat2$LMT_Date, csvdat2$LMT_Time), tz = 'GMT'))

# put dates in y-m-d format with lubridate
csvdat2$UTC_Date <- as.character(mdy(csvdat2$UTC_Date))
csvdat2$LMT_Date <- as.character(mdy(csvdat2$LMT_Date))

## rename headers to match existing downloads
names(csvdat2) <- c('fileName', 'XNo', 'XCollarID', 'GMT_Date', 'GMT_Time', 'LMT_Date',
                    'LMT_Time', 'SCTS_Date', 'SCTS_Time', 'ECEF_X', 'ECEF_Y', 
                    'ECEF_Z', 'LAT', 'LONG', 'ALT', 'DOP', 'FixStatus', 'X3D_Error', 
                    'Satsused', 'SatNo1', 'CNdb1', 'SatNo2', 'CNdb2', 'SatNo3', 
                    'CNdb3', 'SatNo4', 'CNdb4', 'SatNo5', 'CNdb5', 'SatNo6', 'CNdb6', 
                    'SatNo7', 'CNdb7', 'SatNo8', 'CNdb8', 'SatNo9', 'CNdb9',              
                    'SatNo10', 'CNdb10', 'SatNo11', 'CNdb11', 'SatNo12', 'CNdb12', 
                    'MortStatus', 'Activity', 'Main_V', 'Beacon_v', 'Temperature_C', 'Easting', 
                    'Northing', 'Collar_Serial_No', 'ET', 'Animal_ID', 'GMT', 'LMT')

# remove unnecessary columns
csvdat2 <- csvdat2 %>% 
  select(-c('XNo', 'XCollarID', 'X3D_Error', ))

csvdat2$CollarMake <- 'Vectronic'


####----------------------------------------------------------------------
#### Type 3 (Vectronic)

csvnames3 <- list.files('csv3')
csvnames3 <- paste0('csv3/', csvnames3) # add folder name so the next function can read them in

csvdat3 <- csvnames3 %>%
  set_names() %>%
  map_df(read_csv, skip = 1, col_names = F, col_types = cols(.default = 'c'),
         .id = 'fileName') %>%
  dplyr::rename(No = X1,
                CollarID = X2,
                LMT_Date = X3,
                LMT_Time = X4,
                Origin = X5,
                SCTS_Date = X6,
                SCTS_Time = X7,
                `ECEF_X [m]`  = X8,
                `ECEF_Y [m]` = X9,
                `ECEF_Z [m]` = X10,
                `Latitude [°]` = X11,
                `Longitude [°]` = X12,
                `Height [m]` = X13,
                DOP = X14,
                FixType = X15,
                `3D_Error [m]` = X16,
                Sats = X17,
                `Sat No1` = X18,
                `C/N db1` = X19,
                `Sat No2` = X20,
                `C/N db2` = X21,
                `Sat No3` = X22,
                `C/N db3` = X23,
                `Sat No4` = X24,
                `C/N db4` = X25,
                `Sat No5` = X26,
                `C/N db5` = X27,
                `Sat No6` = X28,
                `C/N db6` = X29,
                `Sat No7` = X30,
                `C/N db7` = X31,
                `Sat No8` = X32,
                `C/N db8` = X33,
                `Sat No9` = X34,
                `C/N db9` = X35,
                `Sat No10` = X36,
                `C/N db10` = X37,
                `Sat No11` = X38,
                `C/N db11` = X39,
                `Sat No12` = X40,
                `C/N db12` = X41,
                `Mort. Status` = X42,
                Activity = X43,
                `Main [V]` = X44,
                `Beacon [V]` = X45,
                `Temp [°C]` = X46,
                Easting = X47,
                Northing = X48,
                AnimalID = X49,
                GroupID = X50)

csvdat3$fileName <- tools::file_path_sans_ext(basename(csvdat3$fileName))

# split fileName into collar serial and animal ID
csvdat3$SerialNo <- str_split_fixed(csvdat3$fileName, '_', n=2)[,1] # extract SerialNo from fileName
csvdat3$ET <- str_split_fixed(csvdat3$fileName, '_', n=2)[,2] # extract ET# from fileName
csvdat3$Animal_ID <- gsub('ET', '', csvdat3$ET) # remove 'ET' from ET#

# check tz
csvdat3$DateTime_LMT <- as.character(mdy_hms(paste(csvdat3$LMT_Date, csvdat3$LMT_Time), tz = 'GMT'))

# put dates in y-m-d format with lubridate
csvdat3$LMT_Date <- as.character(mdy(csvdat3$LMT_Date))

# rename files to match downloaded files (X indicates that column isn't present in downloaded files)
names(csvdat3) <- c('fileName', 'XNo', 'XCollarID', 'Origin', 'LMT_Date',
                    'LMT_Time', 'SCTS_Date', 'SCTS_Time', 'ECEF_X', 'ECEF_Y',
                    'ECEF_Z', 'LAT', 'LONG', 'ALT', 'DOP', 'FixStatus', 'X3D_Error',
                    'Satsused', 'SatNo1', 'CNdb1', 'SatNo2', 'CNdb2', 'SatNo3',
                    'CNdb3', 'SatNo4', 'CNdb4', 'SatNo5', 'CNdb5', 'SatNo6', 'CNdb6',
                    'SatNo7', 'CNdb7', 'SatNo8', 'CNdb8', 'SatNo9', 'CNdb9',
                    'SatNo10', 'CNdb10', 'SatNo11', 'CNdb11', 'SatNo12', 'CNdb12',
                    'MortStatus', 'Activity', 'Main_V', 'Beacon_v', 'Temperature_C', 'Easting',
                    'Northing', 'XAnimalID', 'XGroupID', 'Collar_Serial_No', 'ET',
                    'Animal_ID','LMT')

# remove unnecessary columns
csvdat3 <- csvdat3 %>%
  select(-c('XNo', 'XCollarID', 'X3D_Error', 'XAnimalID', 'XGroupID'))

csvdat3$CollarMake <- 'Vectronic'


####------------------------------------------------------------------
#### Type 4 (Vectronic)

csvnames4 <- list.files('csv4')
csvnames4 <- paste0('csv4/', csvnames4) # add folder name so the next function can read them in

csvnames4 <- c(csvnames4, paste0('csv4b/', list.files('csv4b')))

csvdat4 <- csvnames4 %>%
  set_names() %>% 
  map_df(read_csv, skip = 1, col_names = F, col_types = cols(.default = 'c'),
         .id = 'fileName') %>%
  dplyr::rename(No = X1, 
                CollarID = X2, 
                UTC_Date = X3,
                UTC_Time = X4,
                LMT_Date = X5, 
                LMT_Time = X6,
                Origin = X7,
                SCTS_Date = X8, 
                SCTS_Time = X9, 
                `ECEF_X [m]`  = X10, 
                `CEF_Y [m]` = X11, 
                `ECEF_Z [m]` = X12, 
                `Latitude [°]` = X13, 
                `Longitude [°]` = X14, 
                `Height [m]` = X15,	
                DOP = X16, 
                FixType = X17, 
                `3D_Error [m]` = X18, 
                Sats = X19,	
                `Sat No1` = X20,	
                `C/N db1` = X21,	
                `Sat No2` = X22,	
                `C/N db2` = X23,	
                `Sat No3` = X24,	
                `C/N db3` = X25,	
                `Sat No4` = X26,	
                `C/N db4` = X27, 
                `Sat No5` = X28, 
                `C/N db5` = X29, 
                `Sat No6` = X30, 
                `C/N db6` = X31, 
                `Sat No7` = X32, 
                `C/N db7` = X33, 
                `Sat No8` = X34, 
                `C/N db8` = X35,	
                `Sat No9` = X36,	
                `C/N db9` = X37,	
                `Sat No10` = X38,	
                `C/N db10` = X39,	
                `Sat No11` = X40,	
                `C/N db11` = X41,	
                `Sat No12` = X42,	
                `C/N db12` = X43,	
                `Mort. Status` = X44, 
                Activity = X45, 
                `Main [V]` = X46, 
                `Beacon [V]` = X47, 
                `Temp [°C]` = X48,
                Easting = X49, 
                Northing = X50,
                AnimalID = X51,
                GroupID = X52)

csvdat4$fileName <- tools::file_path_sans_ext(basename(csvdat4$fileName))

# split fileName into collar serial and animal ID
csvdat4$SerialNo <- str_split_fixed(csvdat4$fileName, '_', n=2)[,1] # extract SerialNo from fileName
csvdat4$ET <- str_split_fixed(csvdat4$fileName, '_', n=2)[,2] # extract ET# from fileName
csvdat4$Animal_ID <- gsub('ET', '', csvdat4$ET) # remove 'ET' from ET#

# merge UTC date and time into single DateTime value
csvdat4$DateTime_GMT <- as.character(mdy_hms(paste(csvdat4$UTC_Date, csvdat4$UTC_Time), tz = 'GMT'))
# check tz -- listed as same as GMT?
csvdat4$DateTime_LMT <- as.character(mdy_hms(paste(csvdat4$LMT_Date, csvdat4$LMT_Time), tz = 'GMT'))

# put dates in y-m-d format with lubridate
csvdat4$UTC_Date <- as.character(mdy(csvdat4$UTC_Date))
csvdat4$LMT_Date <- as.character(mdy(csvdat4$LMT_Date))

# rename files to match downloaded files (X indicates that column isn't present in downloaded files)
names(csvdat4) <- c('fileName', 'XNo', 'XCollarID', 'GMT_Date', 'GMT_Time', 'LMT_Date',
                    'LMT_Time', 'Origin', 'SCTS_Date', 'SCTS_Time', 'ECEF_X', 'ECEF_Y', 
                    'ECEF_Z', 'LAT', 'LONG', 'ALT', 'DOP', 'FixStatus', 'X3D_Error', 
                    'Satsused', 'SatNo1', 'CNdb1', 'SatNo2', 'CNdb2', 'SatNo3', 
                    'CNdb3', 'SatNo4', 'CNdb4', 'SatNo5', 'CNdb5', 'SatNo6', 'CNdb6', 
                    'SatNo7', 'CNdb7', 'SatNo8', 'CNdb8', 'SatNo9', 'CNdb9',              
                    'SatNo10', 'CNdb10', 'SatNo11', 'CNdb11', 'SatNo12', 'CNdb12', 
                    'MortStatus', 'Activity', 'Main_V', 'Beacon_v', 'Temperature_C', 'Easting', 
                    'Northing', 'XAnimalID', 'XGroupID', 'Collar_Serial_No', 'ET', 
                    'Animal_ID', 'GMT', 'LMT')

csvdat4 <- csvdat4 %>% 
  select(-c('XNo', 'XCollarID', 'X3D_Error', 'XAnimalID', 'XGroupID'))

csvdat4$CollarMake <- 'Vectronic'


####-------------------------------------------------------------------------
#### Type 5 (Vectronic)

csvnames5 <- list.files('csv5')
csvnames5 <- paste0('csv5/', csvnames5) # add folder name so the next function can read them in

csvdat5 <- csvnames5 %>%
  set_names() %>% 
  map_df(read_csv, skip = 1, col_names = F, col_types = cols(.default = 'c'),
         .id = 'fileName') %>%
  dplyr::rename(No = X1, 
                CollarID = X2, 
                UTC_Date = X3,
                UTC_Time = X4,
                LMT_Date = X5, 
                LMT_Time = X6,
                Origin = X7,
                SCTS_Date = X8, 
                SCTS_Time = X9, 
                `ECEF_X [m]`  = X10, 
                `CEF_Y [m]` = X11, 
                `ECEF_Z [m]` = X12, 
                `Latitude [°]` = X13, 
                `Longitude [°]` = X14, 
                `Height [m]` = X15,	
                DOP = X16, 
                FixType = X17, 
                `3D_Error [m]` = X18, 
                Sats = X19,	
                `Sat No1` = X20,	
                `C/N db1` = X21,	
                `Sat No2` = X22,	
                `C/N db2` = X23,	
                `Sat No3` = X24,	
                `C/N db3` = X25,	
                `Sat No4` = X26,	
                `C/N db4` = X27, 
                `Sat No5` = X28, 
                `C/N db5` = X29, 
                `Sat No6` = X30, 
                `C/N db6` = X31, 
                `Sat No7` = X32, 
                `C/N db7` = X33, 
                `Sat No8` = X34, 
                `C/N db8` = X35,	
                `Sat No9` = X36,	
                `C/N db9` = X37,	
                `Sat No10` = X38,	
                `C/N db10` = X39,	
                `Sat No11` = X40,	
                `C/N db11` = X41,	
                `Sat No12` = X42,	
                `C/N db12` = X43,	
                `Mort. Status` = X44, 
                Activity = X45, 
                `Main [V]` = X46, 
                `Beacon [V]` = X47, 
                `Temp [°C]` = X48,
                Easting = X49, 
                Northing = X50)

csvdat5$fileName <- tools::file_path_sans_ext(basename(csvdat5$fileName))

# split fileName into collar serial and animal ID
csvdat5$SerialNo <- str_split_fixed(csvdat5$fileName, '_', n=2)[,1] # extract SerialNo from fileName
csvdat5$ET <- str_split_fixed(csvdat5$fileName, '_', n=2)[,2] # extract ET# from fileName
csvdat5$Animal_ID <- gsub('ET', '', csvdat5$ET) # remove 'ET' from ET#

# merge UTC date and time into single DateTime value
csvdat5$DateTime_GMT <- as.character(mdy_hms(paste(csvdat5$UTC_Date, csvdat5$UTC_Time), tz = 'GMT'))
# check tz -- listed as same as GMT?
csvdat5$DateTime_LMT <- as.character(mdy_hms(paste(csvdat5$LMT_Date, csvdat5$LMT_Time), tz = 'GMT'))

# put dates in y-m-d format with lubridate
csvdat5$UTC_Date <- as.character(mdy(csvdat5$UTC_Date))
csvdat5$LMT_Date <- as.character(mdy(csvdat5$LMT_Date))

# rename files to match downloaded files (X indicates that column isn't present in downloaded files)
names(csvdat5) <- c('fileName', 'XNo', 'XCollarID', 'GMT_Date', 'GMT_Time', 'LMT_Date',
                    'LMT_Time', 'Origin', 'SCTS_Date', 'SCTS_Time', 'ECEF_X', 'ECEF_Y', 
                    'ECEF_Z', 'LAT', 'LONG', 'ALT', 'DOP', 'FixStatus', 'X3D_Error', 
                    'Satsused', 'SatNo1', 'CNdb1', 'SatNo2', 'CNdb2', 'SatNo3', 
                    'CNdb3', 'SatNo4', 'CNdb4', 'SatNo5', 'CNdb5', 'SatNo6', 'CNdb6', 
                    'SatNo7', 'CNdb7', 'SatNo8', 'CNdb8', 'SatNo9', 'CNdb9',              
                    'SatNo10', 'CNdb10', 'SatNo11', 'CNdb11', 'SatNo12', 'CNdb12', 
                    'MortStatus', 'Activity', 'Main_V', 'Beacon_v', 'Temperature_C', 'Easting', 
                    'Northing', 'Collar_Serial_No', 'ET', 
                    'Animal_ID', 'GMT', 'LMT')

csvdat5 <- csvdat5 %>%
  select(-c('XNo', 'XCollarID', 'X3D_Error'))

csvdat5$CollarMake <- 'Vectronic'


####--------------------------------------------------------------
#### Type 6 (Lotek)

csvnames6 <- list.files('csv6')
csvnames6 <- paste0('csv6/', csvnames6) # add folder name so the next function can read them in

csvdat6 <- csvnames6 %>%
  set_names() %>% 
  map_df(read_csv, skip = 5, col_names = F, col_types = cols(.default = 'c'), # skip metadata and header row
         .id = 'fileName') %>%
  dplyr::rename(GMT_Time = X1,
                Latitude = X2,
                Longitude = X3, 
                Altitude = X4,
                Duration = X5,
                Temperature = X6,
                DOP = X7,
                Satellites = X8)

csvdat6$fileName <- tools::file_path_sans_ext(basename(csvdat6$fileName))

# split fileName into collar serial and animal ID
csvdat6$SerialNo <- str_split_fixed(csvdat6$fileName, '_', n=2)[,1] # extract SerialNo from fileName
csvdat6$ET <- str_split_fixed(csvdat6$fileName, '_', n=2)[,2] # extract ET# from fileName
csvdat6$Animal_ID <- gsub('ET', '', csvdat6$ET) # remove 'ET' from ET#

# create datetime column in correct format
csvdat6$DateTime_GMT <- as.character(mdy_hms(csvdat6$GMT_Time, tz = 'GMT'))

# split datetime into date and time
csvdat6$GMT_Date <- str_split_fixed(csvdat6$DateTime_GMT, ' ', n=2)[,1]
csvdat6$GMT_Time <- str_split_fixed(csvdat6$DateTime_GMT, ' ', n=2)[,2]

# rename files to match downloaded files (X indicates that column isn't present in downloaded files)
names(csvdat6) <- c('fileName', 'GMT_Time', 'LAT', 'LONG', 'ALT', 'Duration_sec', 
                    'Temperature_C', 'DOP', 'Satsused', 'Collar_Serial_No',
                    'ET', 'Animal_ID', 'GMT', 'GMT_Date')

csvdat6$CollarMake <- 'Lotek'


####--------------------------------------------------------------------
#### Type 7 (Lotek)

csvnames7 <- list.files('csv7')
csvnames7 <- paste0('csv7/', csvnames7) # add folder name so the next function can read them in

csvnames7 <- c(csvnames7, paste0('csv7b/', list.files('csv7b')))

csvdat7 <- csvnames7 %>%
  set_names() %>% 
  map_df(read_csv, skip = 5, col_names = F, col_types = cols(.default = 'c'), # skip metadata and header row
         .id = 'fileName') %>%
  dplyr::rename(GMT_Time = X1,
                Latitude = X2,
                Longitude = X3, 
                Altitude = X4,
                Duration = X5,
                Temperature = X6,
                DOP = X7,
                Satellites = X8,
                Cause_of_Fix = X9)

csvdat7$fileName <- tools::file_path_sans_ext(basename(csvdat7$fileName))

# split fileName into collar serial and animal ID
csvdat7$SerialNo <- str_split_fixed(csvdat7$fileName, '_', n=2)[,1] # extract SerialNo from fileName
csvdat7$ET <- str_split_fixed(csvdat7$fileName, '_', n=2)[,2] # extract ET# from fileName
csvdat7$Animal_ID <- gsub('ET', '', csvdat7$ET) # remove 'ET' from ET#

# rename time column
csvdat7$DateTime_GMT <- as.character(mdy_hms(csvdat7$GMT_Time, tz = 'GMT'))

# split datetime into date and time
csvdat7$GMT_Date <- str_split_fixed(csvdat7$DateTime_GMT, ' ', n=2)[,1]
csvdat7$GMT_Time <- str_split_fixed(csvdat7$DateTime_GMT, ' ', n=2)[,2]

# rename files to match downloaded files (X indicates that column isn't present in downloaded files)
names(csvdat7) <- c('fileName', 'GMT_Time', 'LAT', 'LONG', 'ALT', 'Duration_sec', 
                    'Temperature_C', 'DOP', 'Satsused', 'Cause_of_Fix', 'Collar_Serial_No',
                    'ET', 'Animal_ID', 'GMT', 'GMT_Date')

csvdat7$CollarMake <- 'Lotek'

####--------------------------------------------------------------------
#### Type 8 (Lotek)

csvnames8 <- paste0('csv8/', list.files('csv8'))

csvdat8 <-
  do.call(bind_rows, lapply(csvnames8, function(x) {
    dat8 <- read.csv(x, skip = 4, header = TRUE)
    dat8 <- separate(dat8, GMT.Time.csvLatitude.csvLongitude.csvAltitude.csvDuration.csvTemperature.csvDOP.csvSatellites.csvCause.of.Fix, 
                     c("GMT_Time", "Latitude", "Longitude", "Altitude", "Duration", 
                       "Temperature", "DOP", "Satellites", "Cause"), sep = ".csv")
    dat8$fileName <- tools::file_path_sans_ext(basename(x))
    dat8
  }))

csvdat8$SerialNo <- str_split_fixed(csvdat8$fileName, '_', n=2)[,1] # extract SerialNo from fileName
csvdat8$ET <- str_split_fixed(csvdat8$fileName, '_', n=2)[,2] # extract ET# from fileName
csvdat8$Animal_ID <- gsub('ET', '', csvdat8$ET) # remove 'ET' from ET#

csvdat8$DateTime_GMT <- as.character(mdy_hms(csvdat8$GMT, tz = 'GMT'))

# split dateTime into date and time
csvdat8$GMT_Date <- str_split_fixed(csvdat8$DateTime_GMT, ' ', n=2)[,1]
csvdat8$GMT_Time <- str_split_fixed(csvdat8$DateTime_GMT, ' ', n=2)[,2]

# rename files to match downloaded files (X indicates that column isn't present in downloaded files)
names(csvdat8) <- c('GMT_Time', 'LAT', 'LONG', 'ALT', 'Duration_sec', 
                    'Temperature_C', 'DOP', 'Satsused', 'Cause_of_Fix', 'fileName', 
                    'Collar_Serial_No', 'ET', 'Animal_ID', 'GMT', 'GMT_Date')



csvdat8$CollarMake <- 'Lotek'


####------------------------------------------------------------------------
#### Type 9 (Hold off on these, weird format)


####--------------------------------------------------------------------------
#### Type 10 (Lotek)

csvnames10 <- paste0('csv10/', list.files('csv10'))

csvdat10 <-
  do.call(bind_rows, lapply(csvnames10, function(x) {
    dat10 <- read.csv(x, skip = 4, header = TRUE)
    dat10 <- separate(dat10, GMT.Time.tabLatitude.tabLongitude.tabAltitude.tabDuration.tabTemperature.tabDOP.tabSatellites.tabCause.of.Fix,
                      c("GMT_Time", "Latitude", "Longitude", "Altitude", "Duration_sec",
                        "Temperature", "DOP", "Satellites", "Cause"), sep = ".tab")
    dat10$fileName <- tools::file_path_sans_ext(basename(x))
    dat10
  }))

csvdat10$SerialNo <- str_split_fixed(csvdat10$fileName, '_', n=2)[,1] # extract SerialNo from fileName
csvdat10$ET <- str_split_fixed(csvdat10$fileName, '_', n=2)[,2] # extract ET# from fileName
csvdat10$Animal_ID <- gsub('ET', '', csvdat10$ET) # remove 'ET' from ET#

# change datetime to standard format, then back to character
csvdat10$DateTime_GMT <- as.character(mdy_hms(csvdat10$GMT, tz = 'GMT'))

# split dateTime into date and time
csvdat10$GMT_Date <- str_split_fixed(csvdat10$DateTime_GMT, ' ', n=2)[,1]
csvdat10$GMT_Time <- str_split_fixed(csvdat10$DateTime_GMT, ' ', n=2)[,2]

# rename files to match downloaded files (X indicates that column isn't present in downloaded files)
names(csvdat10) <- c('GMT_Time', 'LAT', 'LONG', 'ALT', 'Duration',
                     'Temperature_C', 'DOP', 'Satsused', 'Cause_of_Fix', 'fileName',
                     'Collar_Serial_No', 'ET', 'Animal_ID', 'GMT', 'GMT_Date')

csvdat10$CollarMake <- 'Lotek'


####------------------------------------------------------------------------
#### Type 11 (Lotek) (Hold off on these, weird format)


####----------------------------------------------------------------------
#### Type 12 (Telonics)

# list of all the file names
csvnames12 <- list.files('csv12')
csvnames12 <- paste0('csv12/', csvnames12) # add folder name so the next function can read them in

csvdat12 <- csvnames12 %>%
  set_names() %>%
  map_df(read_csv, skip = 23, col_names = F, col_types = cols(.default = 'c'), # skip metadata and header row
         .id = 'fileName') %>%
  dplyr::rename(`Acquisition Time` = X1,
                `Acquisition Start Time` = X2,
                `GPS Fix Time` = X3,
                `GPS Fix Attempt` = X4,
                `GPS Latitude` = X5,
                `GPS Longitude` = X6,
                `GPS UTM Zone` = X7,
                `GPS UTM Northing` = X8,
                `GPS UTM Easting` = X9,
                `GPS Altitude` = X10,
                `GPS Speed` = X11,
                `GPS Heading` = X12,	
                `GPS Horizontal Error` = X13,
                `GPS Horizontal Dilution` = X14,
                `GPS Satellite Bitmap` = X15,
                `GPS Satellite Count` = X16,
                `GPS Navigation Time` = X17,
                `Satellite Uplink` = X18,
                Mortality = X19,
                `Iridium Command` = X20,
                `Predeployment Data` = X21,
                Error = X22)

csvdat12$fileName <- tools::file_path_sans_ext(basename(csvdat12$fileName))

# split fileName into collar serial and animal ID
csvdat12$SerialNo <- str_split_fixed(csvdat12$fileName, '_', n=2)[,1] # extract SerialNo from fileName
csvdat12$ET <- str_split_fixed(csvdat12$fileName, '_', n=2)[,2] # extract ET# from fileName
csvdat12$Animal_ID <- gsub('ET', '', csvdat12$ET) # remove 'ET' from ET#

# convert datetime to standard lubridate format, then back to character
csvdat12$DateTime_GMT <- as.character(ymd_hms(csvdat12$`Acquisition Time`, tz = 'GMT'))

# split dateTime into date and time
csvdat12$GMT_Date <- str_split_fixed(csvdat12$DateTime_GMT, ' ', n=2)[,1]
csvdat12$GMT_Time <- str_split_fixed(csvdat12$DateTime_GMT, ' ', n=2)[,2]

names(csvdat12) <- c('fileName', 'XAcquisitionTime', 'XAcquisitionStartTime',
                     'XGPSFixTime', 'GPSFixAttempt', 'LAT', 'LONG', 'XUTMZone',
                     'Easting', 'Northing', 'ALT', 'XGPSSpeed', 'XGPSHeading',
                     'HorizError_M', 'DOP', 'XGPSBitmap', 'Satsused', 'XGPSNavTime',
                     'XSatelliteUplink', 'MortStatus', 'XIridiumCommand', 'XPredeploymentData',
                     'XError', 'Collar_Serial_No', 'ET', 'Animal_ID', 'GMT',
                     'GMT_Date', 'GMT_Time')

csvdat12 <- csvdat12 %>%
  select(-c('XAcquisitionTime', 'XAcquisitionStartTime', 'XGPSFixTime', 'XUTMZone',
            'XGPSSpeed', 'XGPSHeading', 'XGPSBitmap', 'XGPSNavTime', 'XSatelliteUplink',
            'XIridiumCommand', 'XPredeploymentData', 'XError'))

csvdat12$CollarMake <- 'Telonics'


####--------------------------------------------------------------------
#### Combine Dataframes

## rbind all csvs into one data frame
all_csv <- plyr::rbind.fill(csvdat1, csvdat2, csvdat3, csvdat4, csvdat5, csvdat6, 
                            csvdat7, csvdat8, csvdat10, csvdat12)

# add MortStatus by looking for 'Mortality' in Cause_of_Fix 
all_csv$MortStatus = ifelse(all_csv$Cause_of_Fix %in% c('Mortality'), 'Yes', 'No')

# remove Cause_of_Fix
all_csv <- all_csv %>% 
  select(-c('Cause_of_Fix'))


# flag values where LMT is in GMT, rather than MST or PST (or CST)
all_csv$TimeZoneFlag <- ifelse(all_csv$GMT == all_csv$LMT, 1, 0)
all_csv$TimeZoneFlag_Reason <- ifelse(all_csv$GMT == all_csv$LMT,
                                      'GMT and LMT were identical', NA)

# remove all the individual csv data sets to free memory
rm(csvdat1, csvdat2, csvdat3, csvdat4, csvdat5, csvdat6, 
   csvdat7, csvdat8, csvdat10, csvdat12)


# aa <- file_path_sans_ext(basename(list.files('K:/Wildlife/Fogel/Collar Data Processing/Collar csvs new', recursive = T)))
# bb <- unique(all_csv$fileName)
# cc <- file_path_sans_ext(csvs$file.ext)
# bb
# 
# aa[which(!(aa %in% bb))]
# 
# cc[which(!(cc %in% bb))]
# 
# nrow(csvs)
# length(cc)
# length(unique(cc))
# 
# duplicated(cc)
# 
# csvs <- distinct(csvs)
# 
# View(csvs[!duplicated(cc),])

a <- unique(all_csv$fileName)
b <- unique(tools::file_path_sans_ext(csvs$file.ext))

c <- file_path_sans_ext(basename(csvnames8))

# filenames in all_csv that aren't in csvs
a[which(!(b %in% a))]
# filenames in csvs that aren't in all_csv
b[which(!(a %in% b))]
# filenames in csv8 that aren't in all_csv
c[which(!(a %in% c))]
