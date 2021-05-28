#################################
### SORT COLLARS BY FILE TYPE ###
#################################

setwd('K:/Wildlife/Fogel/Collar Data Processing')

## First run Rename_Files.R to clean file names
## Then use 'need_processing_correct_format.csv to get filenames


 
####----------------------------------------------------------------
#### Sort txt files based on collar make and model 
####   -> different collars download in different formats

#### get list of header row from every file for identifying different formats 
# initialize empty vectors
firstrows <- vector(length = nrow(txts))
file <- vector(length = nrow(txts))
txts$firstrow <- NA

## loop through all txt files and pull first row (header)
for (i in 1:nrow(txts)) {
  ## need reader package for this file, in packages script
  ## input is the full path for each file, 'n' argument is number of lines to read in
  ## 'text' object is header row for that file
  text <- n.readLines(txts$path[i], n = 1)
  firstrows[i] <- text
  file <- txts$path[i]
  txts$firstrow[i] <- text
}

## unique list of header rows
headers <- sort(unique(firstrows))

## add 'type' to txts table -- each unique header style is a unique type
## initialize as NA
txts$type <- NA

# assign type
# morts- don't need to process
txts[txts$firstrow == '            GMT Date GMT Time            GMT Date GMT Time',
     ]$type <- 1
txts[txts$firstrow == '      No CollarID   LMT Date     LMT Time       Origin  SCTS Date    SCTS Time    ECEF X    ECEF Y    ECEF Z     Latitude    Longitude   Height  DOP       FixType 3D Error  Sats Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N                Mort. Status Activity  Main Beacon  Temp              Easting             Northing AnimalID  GroupID',
     ]$type <- 2
# VITs and SEP files-- don't need to process
txts[txts$firstrow == '      No CollarID   UTC Date     UTC Time   LMT Date     LMT Time         Origin  SCTS Date    SCTS Time Transmitter   Temp.       Status       Description Activity AnimalID  GroupID' |
       txts$firstrow == '      No CollarID   UTC Date     UTC Time   LMT Date     LMT Time         Origin  SCTS Date    SCTS Time Transmitter Received Alive    Description AnimalID  GroupID' |
       txts$firstrow == '      No CollarID   UTC Date     UTC Time   LMT Date     LMT Time         Origin  SCTS Date    SCTS Time Transmitter RSSI  Alive AnimalID  GroupID',
     ]$type <- 3
txts[txts$firstrow == '      No CollarID   UTC Date     UTC Time   LMT Date     LMT Time       Origin  SCTS Date    SCTS Time    ECEF X    ECEF Y    ECEF Z     Latitude    Longitude   Height  DOP       FixType 3D Error  Sats Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N                Mort. Status Activity  Main Beacon  Temp              Easting             Northing',
     ]$type <- 4
txts[txts$firstrow == '      No CollarID   UTC Date     UTC Time   LMT Date     LMT Time       Origin  SCTS Date    SCTS Time    ECEF X    ECEF Y    ECEF Z     Latitude    Longitude   Height  DOP       FixType 3D Error  Sats Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N                Mort. Status Activity  Main Beacon  Temp              Easting             Northing AnimalID  GroupID',
     ]$type <- 5
txts[txts$firstrow == '   No   GMT Date GMT Time   LMT Date LMT Time    ECEF X    ECEF Y    ECEF Z     Latitude    Longitude   Height  DOP   Nav Validated Sats Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N Sat C/N  Main  Bkup Temp   Remarks',
     ]$type <- 6
# header rows are different but data are the same
txts[txts$firstrow == 'CollarSerialNumber,Year,Month,Day,Hour,Minute,Status,Activity,Temperature,BlockNumber,Latitude,Longitude,HDOP,NumSats,FixTime,2D/3D' |
       txts$firstrow == 'Year,Day,Hour,Minutes,Status,Activity,Temp,Blocknumber,Latitude,Longitude,Hdop,Num_of_sats,Fix_time,2D/3D',
     ]$type <- 7
txts[txts$firstrow == 'No\tCollarID\tLMT_Date\tLMT_Time\tOrigin\tSCTS_Date\tSCTS_Time\tECEF_X [m]\tECEF_Y [m]\tECEF_Z [m]\tLatitude [°]\tLongitude [°]\tHeight [m]\tDOP\tFixType\t3D_Error [m]\tSats\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tMort. Status\tActivity\tMain [V]\tBeacon [V]\tTemp [°C]\tEasting\tNorthing\tAnimalID\tGroupID',
     ]$type <- 8
txts[txts$firstrow == 'No\tCollarID\tUTC Date\tUTC Time\tLMT Date\tLMT Time\tOrigin\tSCTS Date\tSCTS Time\tECEF X\tECEF Y\tECEF Z\tLatitude\tLongitude\tHeight\tDOP\t\tFixType\t3D Error\tSats\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tMort. Status\tActivity\tMain B\teacon\tTemp\tEasting\tNorthing',
     ]$type <- 9
txts[txts$firstrow == 'No\tCollarID\tUTC Date\tUTC Time\tLMT Date\tLMT Time\tOrigin\tSCTS Date\tSCTS Time\tECEF X\tECEF Y\tECEF Z\tLatitude\tLongitude\tHeight\tDOP\tFix\tType\t3D Error\tSats\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tSat\tC/N\tMort. Status\tActivity\tMain\tBeacon\tTemp\tEasting\tNorthing\tAnimalID\tGroupID',
     ]$type <- 10


## write txts to csv so you can load it later on
# write.csv(txts, 'txt_files_to_process.csv')

# write a RDS of txt files
saveRDS(txts, 'RData/txts.RDS')

## write csv of header types for viewing
#write.csv(headers, 'CSVs to use/headers.csv') 
# i also found a 'representative' txt file for each header type so I could open the file and look at it























