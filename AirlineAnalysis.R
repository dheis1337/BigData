library(sparklyr)
library(dplyr)
library(ggplot2)
library(data.table)
library(DBI)

# Load data into workspace
air <- fread("~/MyStuff/DataScience/airOT201201.csv")

# Create a spark connection locally
sc <- spark_connect(master = "local")

# Copy data to spark
air.spark <- copy_to(sc, air)

# List data frame just copied
src_tbls(sc)

# Count the number of flights by airline in data set using SQL 
results <- dbGetQuery(sc, "SELECT UNIQUE_CARRIER, COUNT(UNIQUE_CARRIER)
                           FROM AIR
                           GROUP BY UNIQUE_CARRIER")

# Count number of flights by airline in dataset using dplyr
air.spark %>%
  select(UNIQUE_CARRIER) %>%
  group_by(UNIQUE_CARRIER) %>%
  summarise(n = n())

# Calculate mean delay time by airline
air.spark %>%
  select(UNIQUE_CARRIER, DEP_DELAY_NEW) %>%
  group_by(UNIQUE_CARRIER) %>%
  summarise("Mean Delay Time" = mean(DEP_DELAY_NEW, na.rm = TRUE))




