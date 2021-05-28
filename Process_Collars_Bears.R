####################################
### IMPORT AND PROCESS BEAR DATA ###
####################################

source("K:/Wildlife/Fogel/Collar Data Processing/Collar_Packages.R")

####-------------------------------------------------------------------------
#### Import data

setwd('K:/Wildlife/Elmer/BearStuff/Bear GPS Locations/CSVs')

# list all locations
set1 <- paste0('Set1/', list.files(path = 'K:/Wildlife/Elmer/BearStuff/Bear GPS Locations/CSVs/Set1'))

set2 <- paste0('Set2/', list.files(path = 'K:/Wildlife/Elmer/BearStuff/Bear GPS Locations/CSVs/Set2'))


####--------------------------------------------------------------------------
#### First type

bear1 <- set1 %>%
  set_names() %>%
  map_df(read_csv, skip = 1, col_names = F, col_types = cols(.default = 'c'),
         .id = 'fileName') %>%
  dplyr::rename(FID = X1,
                UNID = X2, 
                ID = X3,
                LMT_Date = X4,
                Bear_ID = X5,
                XFIX = X6, 
                Hour = X7,
                Minute = X8,
                Time = X9,
                Period = X10,
                Easting = X11,
                Northing = X12,
                LAT = X13,
                LONG = X14,
                Slope = X15,
                Aspect = X16,
                ALT = X17,
                Landcover = X18,
                Canopy = X19,
                Dist_WA = X20,
                Dist_HI = X21,
                Dist_CLS2 = X22, 
                Dens_RDS = X23,
                Steplength = X24,
                Turnangle = X25,
                Bearing = X26,
                BearingPrev = X27,
                NetDisp = X28,
                FixStatus = X29,
                Satsused = X30,
                DOP = X31,
                Dist_CO = X32)

bear1$fileName <- tools::file_path_sans_ext(basename(bear1$fileName))
bear1$Collar_Serial_No <- str_split_fixed(bear1$fileName, '_', n = 2)[,1]
bear1$ET <- str_split_fixed(bear1$fileName, '_', n = 2)[,2]
bear1$Animal_ID <- gsub('ET', '', bear1$ET)

bear1$LMT_Date <- str_split_fixed(bear1$LMT_Date, ' ', n = 2)[,1]
bear1$LMT_Date <- as.character(mdy(bear1$LMT_Date))

bear1$Hour <- str_split_fixed(bear1$Hour, '\\.', n = 2)[,1]
bear1$Minute <- str_split_fixed(bear1$Minute, '\\.', n = 2)[,1]

bear1$Time <- paste(bear1$Hour, bear1$Minute, sep = ':')

bear1$LMT <- ymd_hm(paste(bear1$LMT_Date, bear1$Time))
bear1$LMT <- force_tz(bear1$LMT, tzone = 'America/Los_Angeles')

bear1$GMT <- with_tz(bear1$LMT, tzone = 'GMT')

bear1$LMT <- as.character(bear1$LMT)
bear1$LMT_Time <- str_split_fixed(bear1$LMT, ' ', n = 2)[,2]

bear1$GMT_Date <- str_split_fixed(bear1$GMT, ' ', n = 2)[,1]
bear1$GMT_Time <- str_split_fixed(bear1$GMT, ' ', n = 2)[,2]


bear1 <- bear1 %>%
  select(-c('FID', 'UNID', 'ID', 'Bear_ID', 'XFIX', 'Hour', 'Minute', 'Time',
            'Period', 'Slope', 'Aspect', 'Landcover', 'Canopy', 'Dist_WA', 'Dist_HI',
            'Dist_CLS2', 'Dens_RDS', 'Steplength', 'Turnangle', 'Bearing', 'BearingPrev',
            'NetDisp', 'Dist_CO'))

bear1$Satsused <- str_split_fixed(bear1$Satsused, '\\.', n = 2)[,1] # a bunch of zeroes get added when these get read in
bear1$DOP <- str_split_fixed(bear1$DOP, '\\.', n = 2)[,1]


####--------------------------------------------------------------------------
#### Second type

bear2 <- set2 %>%
  set_names() %>%
  map_df(read_csv, skip = 1, col_names = F, col_types = cols(.default = 'c'),
         .id = 'fileName') %>%
  dplyr::rename(FID = X1,
                FIX = X2,
                TIME_MIN = X3,
                DateTime = X4,
                Easting = X5,
                Northing = X6,
                ALT = X7,
                Time_weird = X8,
                FixStatus = X9,
                Satsused = X10,
                DOP = X11)


bear2$fileName <- tools::file_path_sans_ext(basename(bear2$fileName))
bear2$Collar_Serial_No <- str_split_fixed(bear2$fileName, '_', n = 2)[,1]
bear2$ET <- str_split_fixed(bear2$fileName, '_', n = 2)[,2]
bear2$Animal_ID <- gsub('ET', '', bear2$ET)

## convert UTM to lat long
# convert easting and norhting to doubles
points <- cbind(as.double(bear2$Easting), as.double(bear2$Northing))
# turn them into projected coordinates
v <- vect(points, crs = '+proj=utm +zone=11 +datum=WGS84 +units=m')

# turn easting northing into lat long
y <- project(v, '+proj=longlat +datum=WGS84')

# extract actual values
latlong <- geom(y)[, c('y', 'x')]

# add values to bear2
bear2$LAT <- latlong[,1]
bear2$LONG <- latlong[,2]

# take out quotes from fixstatus
bear2$FixStatus <- gsub('"', '', bear2$FixStatus)

# convert DateTime to LMT
bear2$LMT <- force_tz(mdy_hm(bear2$DateTime), tzone = 'America/Los_Angeles')

# create GMT time
bear2$GMT <- as.character(with_tz(bear2$LMT, tzone = 'GMT'))

bear2$LMT_Date <- str_split_fixed(bear2$LMT, ' ', n=2)[,1]
bear2$LMT_Time <- str_split_fixed(bear2$LMT, ' ', n=2)[,2]

bear2$GMT_Date <- str_split_fixed(bear2$GMT, ' ', n=2)[,1]
bear2$GMT_Time <- str_split_fixed(bear2$GMT, ' ', n=2)[,2]

# remove unnecessary fields
bear2 <- bear2 %>% 
  select(-c('FID', 'FIX', 'TIME_MIN', 'Time_weird', 'DateTime'))

all_bears <- plyr::rbind.fill(bear1, bear2)

















