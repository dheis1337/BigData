library(sparklyr)
library(dplyr)
library(tidyr)
library(ggplot2)
library(data.table)
library(DBI)
library(car)

air <- fread("~/MyStuff/DataScience/BigData/data/AirOnTimeCSV/flightssamp.csv")

config <- spark_config()

config$`sparklyr.shell.driver-memory` <- "5G"
config$`sparklyr.shell.executor-memory` <- "5G"
config$`spark.yarn.executor.memoryOverhead` <- "1G"

sc <- spark_connect(master = "local", config = config)

                  
air.spark <- copy_to(sc, air, "air")

# Clean the CRS_DEP_TIME
air.spark <- air.spark %>% 
  mutate(CRS_DEP_TIME = as.numeric(CRS_DEP_TIME)) %>% 
  mutate(CRS_ARR_TIME = as.numeric(CRS_ARR_TIME)) %>%
  mutate(CRS_ELAPSED_TIME = as.numeric(CRS_ELAPSED_TIME)) %>%
  compute()


# add ARR_LATE_FLAG, add ARR_DEP_FLAG
air.spark <- air.spark %>%
  mutate(ARR_LATE_FLAG = ifelse(ARR_DELAY_NEW > 0, 1, 0)) %>%
  mutate(ARR_EARLY_FLAG = ifelse(ARR_DELAY <= 0, 1, 0)) %>%
  compute()


# add DEP_LATE_FLAG and DEP_EARLY_FLAG
air.spark <- air.spark %>%
  mutate(DEP_EARLY_FLAG = ifelse(DEP_DELAY <= 0, 1, 0)) %>%
  mutate(DEP_LATE_FLAG = ifelse(DEP_DELAY > 0, 1, 0)) %>% 
  compute()
 
# add a DEP_EARLY_COLUMN that is all the times a flight left early and the number
# of minutes it left early. Add the same column but for ARR
air.spark <- air.spark %>%
  mutate(DEP_EARLY = ifelse(DEP_DELAY < 0, abs(DEP_DELAY), NA)) %>%
  mutate(ARR_EARLY = ifelse(ARR_DELAY < 0, abs(ARR_DELAY), NA)) %>%
  compute()


air.spark <- air.spark %>%
  ft_bucketizer(input_col = "CRS_DEP_TIME", output_col = "CRS_DEP_BUCKET_HOUR",
                splits = c(0 ,59, 159, 259, 359, 459, 559, 659,
                           759, 859, 959, 1059, 1159, 1259, 1359,
                           1459, 1559, 1659, 1759, 1859, 1959, 2059, 2159,
                           2259, 2359, 2400)) 

# Bucket CRS_DEP_TIME into three parts
air.spark <- air.spark %>%
  mutate(CRS_DEP_BUCKET_TOD = ifelse(CRS_DEP_TIME < 1200,  "morning",
                                     ifelse(CRS_DEP_TIME >= 1200 & CRS_DEP_TIME < 1700, 
                                            "afternoon", "night")))
  
# Log transformations of some of the predictors
air.spark <- air.spark %>%
  mutate(LOG_ARR_EARLY = log(ARR_EARLY + 1)) %>%
  mutate(LOG_DEP_EARLY = log(DEP_EARLY + 1)) %>%
  mutate(DEP_DELAY_NEW_LOG = log(DEP_DELAY_NEW + 1)) %>%
  mutate(ARR_DELAY_NEW_LOG = log(ARR_DELAY_NEW + 1))

# Add higher powers of DEP_DELAY
air.spark <- air.spark %>%
  mutate(DEP_DELAY_SQ = DEP_DELAY^2) %>%
  mutate(DEP_DELAY_CUBE = DEP_DELAY^3) %>%
  mutate(DEP_DELAY_QUAR = DEP_DELAY^4)

# Let's remove outliers by finding the IQR and setting a cuttoff for ARR_DELAY 
# observations that exceed 2 * IQR. 
air.spark %>% sdf_quantile("ARR_DELAY") 

# IQR Is 19, so our cutoff is 38
air.spark <- air.spark %>% 
  filter(ARR_DELAY < 39)

# one hot encode the DAY_OF_WEEK column
air.spark <- air.spark %>% ft_one_hot_encoder(input_col = "DAY_OF_WEEK", output_col = "ONE_HOT_DOW")

air.part <- air.spark %>%
  select(YEAR, MONTH, DAY_OF_MONTH, DAY_OF_WEEK, FL_DATE,
         UNIQUE_CARRIER, ORIGIN, DEST, CRS_DEP_TIME, DEP_DELAY_NEW,
         DEP_DELAY_GROUP, ARR_TIME, ARR_DELAY, ARR_DELAY_NEW, DEP_DELAY,
         AIR_TIME, DISTANCE, ARR_LATE_FLAG, ARR_EARLY_FLAG, DEP_LATE_FLAG,
         DEP_EARLY, ARR_EARLY, CRS_DEP_BUCKET_HOUR, CRS_DEP_BUCKET_TOD, 
         LOG_ARR_EARLY, LOG_DEP_EARLY, DEP_DELAY_NEW_LOG, ARR_DELAY_NEW_LOG,
         DEP_DELAY_SQ, DEP_DELAY_CUBE, DEP_DELAY_QUAR, ONE_HOT_DOW) %>% 
  sdf_partition(weights = weights, seed = 1)


