# Load libraries
library(dplyr)
library(ecocomDP)
library(arrow)

# URL link to ecocomDP data object
data_list <- readRDS(file = gzcon(url("https://data.cyverse.org/dav-anon/iplant/projects/NEON/ESA2022/macroinverts_neon.ecocomdp.20120.001.001_release2022.RDS")))

# Flatten data
flat_data <- data_list %>% 
  ecocomDP::flatten_data() %>%
  mutate(
    year = datetime %>% lubridate::year(),
    month = datetime %>% lubridate::month())

# set dir name for saving your datastore
data_dir_name <- "neon_ds"

# create if it doesn't exist
if(!dir.exists(data_dir_name)) dir.create("neon_ds")

# Create a hive partitioned datastore
flat_data %>%
  group_by(siteID) %>%
  arrow::write_dataset(
    path = data_dir_name,
    format = "parquet")

# create active binding with dataset
ds <- arrow::open_dataset(data_dir_name)

# get names in the data schema
ds %>% names()

# view siteIDs
# data are only loaded to memory after "collect()"
site_list <- ds %>%
  select(siteID) %>%
  unique() %>%
  collect() %>%
  unlist(use.names = FALSE)
print(site_list)

# Summarize data
# min and max year in data by siteID
site_year_summary <- ds %>%
  group_by(siteID) %>%
  summarize(
    max_year = max(year, na.rm = TRUE),
    min_year = min(year, na.rm = TRUE)) %>%
  collect() %>% as.data.frame()
print(site_year_summary)

# Filter data to a site and year
# then load into memory
ds_COMO_2019 <- ds %>%
  filter(siteID == "COMO",
         year == 2019) %>%
  collect()
head(ds_COMO_2019)
