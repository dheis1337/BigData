library(sparklyr)
library(dplyr)
library(tidyr)
library(ggplot2)
library(data.table)
library(DBI)
library(chron)


air <- fread("~/MyStuff/DataScience/BigData/data/AirOnTimeCSV/flightssamp.csv")

config <- spark_config()

config$`sparklyr.shell.driver-memory` <- "5G"
config$`sparklyr.shell.executor-memory` <- "5G"
config$`spark.yarn.executor.memoryOverhead` <- "1G"

sc <- spark_connect(master = "local", config = list())

air <- fread("~/MyStuff/DataScience/BigData/data/AirOnTimeCSV/flightssamp.csv")

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



