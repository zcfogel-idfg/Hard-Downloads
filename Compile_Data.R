#################################################
### APPEND PROCESSED DATA TO MAIN DATASET     ###
### LINK HARD DOWNLOADED DATA TO CAPTURE DATA ###
#################################################

####------------------------------------------------------------------
#### Set working directory

setwd('K:/Wildlife/Fogel/Collar Data Processing')


####-------------------------------------------------------------------
#### Combine txt, csv, and xlsx files, then clean them

## might need to do this on the server to have enough memory (or could just do all of thsi on the server from the beginning)
## write RDS files 
saveRDS(all_csv, 'RData/all_csv.RDS')
saveRDS(all_txt, 'RData/all_txt.RDS')
saveRDS(xlsdat, 'RData/xlsdat.RDS')

## now read in RDSs (only need to do this if switching to server)
all_csv <- readRDS('RData/all_csv.RDS')
all_txt <- readRDS('RData/all_txt.RDS')
xlsdat <- readRDS('RData/xlsdat.RDS')

all <- plyr::rbind.fill(all_txt, all_csv)#xlsdat)

all[all$Collar_Serial_No == '36468',]$fileName <- '36468_ET15340(18088)'
all[all$Collar_Serial_No == '36468',]$Animal_ID <- '15340(18088)'
all[all$Collar_Serial_No == '36468',]$ET <- 'ET15340(18088)'

all[all$Collar_Serial_No == '36390',]$fileName <- '36390_ET15459(5642)'
all[all$Collar_Serial_No == '36390',]$ET <- 'ET15459(5642)'
all[all$Collar_Serial_No == '36390',]$Animal_ID <- '15459(5642)'


## clean data
all <- all %>% 
  # change text that says 'N/A' to NA in all columns (need to specify that this is only for character data types)
  mutate_if(is.character, list( ~ ifelse(. %in% c('N/A'), NA, .))) %>%
  # add new columns 
  mutate(LocType = 'Hard',
         DateLocAdded = Sys.time(),
         # change coordinates and other columns to numeric in order to filter by values
         LAT = as.numeric(LAT),
         LONG = as.numeric(LONG),
         Animal_ID = ifelse(is.na(Animal_ID) == T & is.na(ET) == F, ET, Animal_ID),
         #AID_Collar = paste(Animal_ID, Collar_Serial_No, sep = '_'),
         # change GMT to date object
         GMT = ymd_hms(GMT)
  ) %>%
  # check for missing data
  filter(LAT != 0 & LONG != 0 & !is.na(LAT) & !is.na(LONG)) %>%
  # use TZ Flag to set LMT to NA if LMT is same as GMT, and keep it if they're different
  mutate(LMT = ifelse(TimeZoneFlag %in% c(1), NA, LMT)) %>%
  # Convert GMT to MST and make it character data type so R thinks it's 'GMT'
  #   for filtering out locations relative to CapDate, CensorDate, FateDate
  #   (ie, to filter relative to datetimes in different columns, all date-time 
  #   columns must be in same time zone. Otherwise, R thinks the different time 
  #   zones are different data types) ZCF note-- I didn't really understand what's
  #   happening here, but this seems to be important so I kept it in from Brendan's 
  #   code.
  mutate(DTgmt_MST = as.character(with_tz(ymd_hms(GMT, tz = 'GMT'), tzone = 'MST')),
         # set up common date time so you can filter locations relative to capture/necropsy/censor dates.
         #  If DateTime_GMT (really MST) is NOT NA and real GMT is NA, return DateTime_GMT,
         #  otherwise reaturn real GMT as character vector. 
         #  Then set all date-times to GMT timezone. Later, you'll set the capture/necropsy/censor dates
         #  to GMT as well, then filter out locations. 
         # IF LMT is NOT NA and GMT is NA, return LMT (already a character), otherwise, 
         #  return DateTime that was converted from GMT to MST
         #  Set timezone to read as 'GMT' only (do not transform time from MST to GMT!)
         MtnDateTime = ymd_hms(ifelse(!is.na(LMT) & is.na(GMT), LMT, DTgmt_MST), tz = 'GMT')) %>%
  mutate_all(as.character) %>% # convert all columns to character data type
  mutate(MtnDateTime = ymd_hms(MtnDateTime)) %>% # change MtnDateTime back to datetime (this is inefficient but I don't give a hoot)
  # get rid of extra unneeded columns
  dplyr::select(-c('DTgmt_MST')) %>%
  data.frame()

# verify that time manipulation worked
# View(all)
# View(all[is.na(all$MtnDateTime),])
# sum(is.na(all$MtnDateTime)) # should be 0
# 
# # test where GMT is NA
# head(all[is.na(all$GMT),])
# # test where GMT isn't NA
# head(all[!is.na(all$GMT),]) # it worked
# sum(is.na(all$GMT))


####-------------------------------------------------------------------
#### Connect to server and prepare existing data for linking

# ## rerun this if necessary
# source('K:/Wildlife/Fogel/Collar Data Processing/Server_Access.R')
# 
# # should already have 'existing' df from earlier but if not can reload here
# existing <- dbGetQuery(con, 'SELECT * FROM Collars_Hard_Downloads')
# 
# # make mini dataframe-- will need this later on to ensure that new and existing hard downloads are all same object types
# existing_types <- head(existing)
# 
# ## add some new columns to existing for sorting/cleaning data later on
# existing <- existing %>% 
#   mutate_all(as.character) %>%
#   # new identifier to match with new data
#   mutate(#AID_Collar = paste(Animal_ID, Collar_Serial_No, sep = '_'),
#          # flag where GMT and LMT are same
#          TimeZoneFlag = ifelse(GMT == LMT, 1, 0),
#          TimeZoneFlag_Reason = ifelse(TimeZoneFlag == 1, 'GMT and LMT were identical', NA),
#          LMT = ifelse(TimeZoneFlag %in% c(1), NA, LMT)) %>%
#   # convert GMT to MST and make it character data type so you can trick R into thinking it's GMT
#   #   for filtering out locations relative to CapDate, CensorDate, FateDate (ie, to 
#   #   filter relative to date-times in different columns, all date-time columns must 
#   #   be in same time zone. Otherwise, R thinks they're all different data types)
#   mutate(DTgmt_MST = as.character(with_tz(ymd_hms(GMT, tz = 'GMT'), tzone = 'MST')),
#          #set up common date time so you can filter locations relative capture/necropsy/censor dates.
#          # If DateTime_GMT (really MST) is NOT NA and real GMT is NA, return DateTime_GMT, otherwise return real GMT as a character vector. 
#          # Then set all date-times to GMT timezone. Later, you'll set the capture/necropsy/censor dates to GMT as well, then filter out locations.
#          #If LMT is NOT NA and GMT is NA, return LMT (already character), otherwise, return DateTime that was converted from GMT to MST. 
#          # Set timezone to read as "GMT" only (do not transform time from MST to GMT!!)
#          MtnDateTime = ymd_hms(ifelse(!is.na(LMT) & is.na(GMT), LMT, DTgmt_MST), tz = 'GMT')) %>%
#   # get rid of unneeded columns
#   dplyr::select(-c('DTgmt_MST', 'CollarLocID')) %>%
#   data.frame()


####--------------------------------------------------------------------
#### Link new collar data to capture, necropsy, and location data

# Captures
cap <- dbGetQuery(con, "SELECT CaptureID, Animal_ID, Game, GMU, Age_Class, Capture_Date,
                  General_Location, Latitude, Longitude, capture_method, Radio_Frequency,
                  Collar_Serial_No, StudyArea, CollarType, Region, sex, CollarModel, 
                  DateEntered, AssumedAge
                  FROM VU_SAMM_CAPTURE
                  WHERE Collar_Serial_No IS NOT NULL AND CollarType = 'GPS'")

capnames <- dbGetQuery(con, "SELECT * FROM VU_SAMM_CAPTURE")


# rename columns to match what is in existing data
cap <- cap %>%
  rename(CaptureLat = Latitude,
         CaptureLong = Longitude,
         Sex = sex,
         DateAdded = DateEntered) %>%
  mutate(rowID = row_number())

# Necropsies
nec <- dbGetQuery(con, "SELECT CaptureID, FateDate, FateDesc, CensorDate, CensorType,
                    NecID, GenLoc, Latitude, Longitude, GMU, CausCert
                  FROM VU_SAMM_NECROPSY")

# rename columns to match existing data
nec <- nec %>% 
  rename(Latitude_nec = Latitude,
         Longitude_nec = Longitude,
         GenLoc_nec = GenLoc,
         GMU_nec = GMU)


capgroup <- cap %>%
  group_by(Collar_Serial_No) %>%
  # 'row' is how many times that collar has been deployed
  mutate(row = row_number(Collar_Serial_No)) # row = 1 means it's the first deployment, row = 2 means 2nd deployment, row =3 means 3rd deployment, etc

# link to necropsies
capgroupnec <- merge(capgroup, nec, by = 'CaptureID', all.x = T)

datalist <- list()


for (i in 1:max(capgroupnec$row)){
  temp <- filter(capgroupnec, row == i)
  temp1 <- inner_join(temp, all, by = c("Collar_Serial_No"))
  templocs <- subset(temp1,(!is.na(temp1$CensorDate) & !is.na(temp1$FateDate) & temp1$MtnDateTime > (Capture_Date + 1) & temp1$MtnDateTime < temp1$CensorDate)
                     | (!is.na(temp1$CensorDate) & is.na(temp1$FateDate) & temp1$MtnDateTime > (Capture_Date + 1) & temp1$MtnDateTime < temp1$CensorDate)
                     | (is.na(temp1$CensorDate) & !is.na(temp1$FateDate) & temp1$MtnDateTime > (Capture_Date + 1) & temp1$MtnDateTime < temp1$FateDate)
                     | (temp1$MtnDateTime > (Capture_Date + 1) & is.na(temp1$FateDate) & is.na(temp1$CensorDate))
                     )
  datalist[[i]] <- templocs
}

all_full <- do.call(rbind, datalist)

all_full <- subset(all_full, Animal_ID.x == Animal_ID.y)

# nrow(all_full)
# length(unique(all_full$fileName))

a <- unique(all_full$fileName)
b <- unique(all$fileName)

# look for names in b (all) that aren't in a (all_full)
failed <- b[which(!(b %in% a))] # 5/18 checked failed ones, explanations in failed_explanations.csv


write.csv(failed, 'failed_0521.csv')

# add variables from capgroupnec to all df
#all_full <- inner_join(capgroupnec, all, by = "AID_Collar")



# remove duplicate columns, rename columns to match hard download headers
all_full <- all_full %>%
  rename(Animal_ID = Animal_ID.x#,
         #Collar_Serial_No = Collar_Serial_No.x
  ) %>%
  select(-c('Animal_ID.y', 'row', 'GPSFixAttempt', 'MtnDateTime'))

all_full$DataSet <- 'Statewide Monitoring'


# look for names that are in existing and not all_full (need to rename/add missing variables)
names(existing_types)[which(!(names(existing_types) %in% names(all_full)))]
# names that are in all_full and not in existing (need to delete/rename variables)
names(all_full)[which(!(names(all_full) %in% names(existing_types)))]
View(all_full)

# remove unnecessary columns (I thought these had been removed earlier but apparently not)
# all_full <- all_full %>%
#   select(-c('DTgmt_MST'))

####------------------------------------------------------------------------
#### Now check and clean data

## Ensure that none of the new files have already been read in
dupes <- unique(existing$fileName) %in% unique(all_full$fileName) # should be 0
which(dupes)

## Now check for duplicate rows in new data
# distinct() returns only unique rows (like unique() but it assigns new row numbers)
new_distinct <- distinct(all_full, .keep_all = T)  %>%
  mutate(row_names = NA,
         CollarLocID = NA)

## make sure all columns are in correct data format

format1 <- c()
for (i in 1:ncol(existing_types)) {
  format1 <- c(format1, typeof(existing_types[,i]))
}

f1 <- data.frame(vars = names(existing_types), format1 = format1)
f1 <- f1[order(f1$vars),]

format2 <- c()
for (i in 1:ncol(new_distinct)) {
  format2 <- c(format2, typeof(new_distinct[,i]))
}

f2 <- data.frame(vars2 = names(new_distinct), format2 = format2)
f2 <- f2[order(f2$vars2),]

typecheck <- data.frame(f1, f2)
typecheck$match <- typecheck$format1 == typecheck$format2
View(typecheck[typecheck$match == F,])

f2$vars2 %in% f1$vars
f1$vars %in% f2$vars2
f1[c(3, 73),1]



# CollarLocID and row_names seem to be assigned automagically by SQL, so can ignore these

## change certain columns to match original hard downloads
new_distinct$Capture_Date <- as.character(new_distinct$Capture_Date)
#new_distinct$COllarLocID <- as.integer(new_distinct$COllarLocID)
new_distinct$DOP <- as.double(new_distinct$DOP)
new_distinct$LAT <- as.double(new_distinct$LAT)
new_distinct$LONG <- as.double(new_distinct$LONG)
new_distinct$GMT <- ymd_hms(new_distinct$GMT)
new_distinct$LMT <- ymd_hms(new_distinct$LMT)
#new_distinct$row_names <- as.integer(new_distinct$row_names)

# remove COllarLocID -- needed it to compare with existing types but SQL adds it in automatically
new_distinct <- new_distinct %>% select(-c('CollarLocID'))

write_rds(new_distinct, 'RData/New_Collars.rds')


# 
# all[all$Collar_Serial_No == '27063',]$fileName <- '27063_ET19063a'
# all[all$Collar_Serial_No == '27063',]$ET <- 'ET19063a'
# all[all$Collar_Serial_No == '27063',]$Animal_ID <- '19063a'


































