setwd('K:/Wildlife/Fogel/Collar Data Processing')
source('Collar_Packages.R')

source('Server_Access.R')

hard2 <- dbGetQuery(con, 'SELECT * 
                   FROM Collars_Hard_Downloads2')

new <- readRDS('RData/New_Collars.RDS') 

combined <- plyr::rbind.fill(hard2, new)

names(combined)
sort(names(combined))
sort(names(hard))

sum(duplicated(combined))

names(new)[which(!(names(new) %in% names(hard1)))]
names(hard1)[which(!(names(hard1) %in% names(new)))]

View(combined)

new <- new[which(!(new %in% dupes)),]

# saveRDS(combined, 'New_Hard_Downloads.RDS')


dbAppendTable(con, 'Collars_Hard_Downloads2', new)
dbWriteTable(con, 'Collars_Hard_Downloads3', combined)


##### check to see why things didn't append
txtnames <- gsub('.txt', '', txts$file.ext)

txtnames %in% new$AID_Collar

sort(names(new))
sort(names(hard1))

new2 <- anti_join(new, hard2)
nrow(new2)
nrow(new)

hard2 <- hard2 %>%
  select(-c('row_names', 'CollarLocID'))
hard2 <- distinct(hard2)

nrow(unique(hard2))












