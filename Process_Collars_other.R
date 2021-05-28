#############################
### PROCESS 'OTHER' FILES ###
#############################

####-------------------------------------------------------------------------
#### Look for files in Other that aren't in txt, xls, or csv groups

# list of all filenames in new data
all_files <- unique(all_full$fileName)
all_files

# list of all filenames in other data
others <- readRDS('K:/Wildlife/Fogel/Collar Data Processing/RData/others.RDS')
View(others)

others$file <- file_path_sans_ext(others$file.ext)

other_files <- unique(others$file)

# list of filenames in other that aren't in all_full
to.add <- other_files[!which(!(other_files %in% all_files))] # lol they're all already added





















































