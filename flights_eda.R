library(sparklyr)
library(dplyr)
library(ggplot2)
library(data.table)
library(DBI)
library(lessR)

# Load data into workspace
air <- fread("~/MyStuff/DataScience/BigData/data/AirOnTimeCSV/flightssamp.csv")

# Before copying this to the cluster, I need to change the num variabeles to 
int_cols <- which(sapply(air, is.integer))
num_cols <- which(sapply(air, is.numeric))

# These are the columns that are numeric but not yet integer
change_cols <- y[!(y %in% x)]
change_cols <- names(cols)

# convert the numeric columns to integer
air[, (cols) := lapply(.SD, as.integer), .SDcols = cols]


 # Create a spark connection locally
sc <- spark_connect(master = "local")

# Copy data to spark
air.spark <- copy_to(sc, air, "air")

# List data frame just copied
src_tbls(sc)

# Count the number of flights by airline in data set using SQL 
results <- dbGetQuery(sc, "SELECT UNIQUE_CARRIER, COUNT(UNIQUE_CARRIER)
                           FROM AIR
                           GROUP BY UNIQUE_CARRIER")

# Count number of flights by airline in dataset using dplyr
flights_by_carrier <- air.spark %>%
  select(UNIQUE_CARRIER) %>%
  group_by(UNIQUE_CARRIER) %>%
  summarise(n = n()) %>%
  collect()

flights_by_carrier <- as.data.table(flights_by_carrier)

flights_by_carrier[order(-n)]
flights_by_carrier[1:5, sum(n)] 

# Count the number of flights by year 
air.spark %>%
  select(YEAR) %>%
  group_by(YEAR) %>%
  summarise(n = n())

# Count the number of flights per month
air.spark %>% 
  select(MONTH) %>%
  group_by(MONTH) %>%
  summarise(n = n())

# Count the number of flights per day of week
air.spark %>%
  select(DAY_OF_WEEK) %>%
  group_by(DAY_OF_WEEK) %>%
  summarise(n = n())

# Count the number of flights per day of month
flights_day_per_month <- air.spark %>%
                            select(DAY_OF_MONTH) %>%
                            group_by(DAY_OF_MONTH) %>%
                            summarise(n = n()) %>%
                            collect()

as.data.table(flights_day_per_month)

# Count number of flights per origin airport
flights_per_origin <- air.spark %>%
                        select(ORIGIN) %>%
                        group_by(ORIGIN) %>%
                        summarise(n = n()) %>%
                        collect()

flights_per_origin <- as.data.table(flights_per_origin)
flights_per_origin[order(-n)][1:50]

# Mean and median number of flights per origin
flights_per_origin[, .(mean(n), median(n))]

# Count of flights per destination airport
flights_per_dest <- air.spark %>%
                      select(DEST) %>%
                      group_by(DEST) %>%
                      summarise(n = n()) %>%
                      collect()

flights_per_dest <- as.data.table(flights_per_dest)

# Mean and median number of flights per dest
flights_per_dest[, .(mean(n), median(n))]

# Count of flights by state
flights_per_state <- air.spark %>%
                        select(ORIGIN_STATE_ABR) %>%
                        group_by(ORIGIN_STATE_ABR) %>%
                        summarise(n = n()) %>%
                        collect()

flights_per_state <- as.data.table(flights_per_state)

# Count of flights that have some kind of delay
flights_with_delays <- air.spark %>% 
                          filter(DEP_DELAY >= 1) %>%
                          collect()



flights_with_delays <- as.data.table(flights_with_delays)

# Count of flights that aren't delayed at all
flights_without_delays <- air.spark %>% 
  filter(DEP_DELAY == 0) %>%
  collect()

# Count of flights that depart early
flights_early <- air.spark %>%
  filter(DEP_DELAY < 0) %>%
  collect()

flights_early <- as.data.table(flights_early)

# Count number of flights that have an ARR_DELAY and a DEP_DELAY
dep_arr_delay <- air.spark %>%
  filter(DEP_DELAY > 0 & ARR_DELAY > 0) %>%
  collect()

dep_arr_delay <- as.data.table(dep_arr_delay)

# Proportion of flights with DEP_DELAY that also had ARR_DELAY
nrow(dep_arr_delay) / nrow(flights_with_delays)

# Flights with arr_delay
flights_with_arr_delay <- air.spark %>%
  filter(ARR_DELAY > 0) %>%
  collect()

flights_with_arr_delay <- as.data.table(flights_with_arr_delay)

# Proportion of flights with ARR_DELAY that also had DEP_DELAY
nrow(dep_arr_delay) / nrow(flights_with_arr_delay)

# Calculate mean delay time by airline
delay <-  air.spark %>%
      select(UNIQUE_CARRIER, DEP_DELAY_NEW) %>%
      group_by(UNIQUE_CARRIER) %>%
      summarise(MeanDelayTime = mean(DEP_DELAY_NEW, na.rm = TRUE)) %>%
      collect()

delay$UNIQUE_CARRIER <- factor(delay$UNIQUE_CARRIER)

# Create a visualization of delays
ggplot(delay, aes(x = UNIQUE_CARRIER, y = MeanDelayTime, fill = "#4286f4")) +
  geom_col()


# Delays grouped by UNIQUE_CARRIER and DAY_OF_WEEK
delay.by.day.carrier <- air.spark %>% 
  select(UNIQUE_CARRIER, DEP_DELAY_NEW, DAY_OF_WEEK) %>%
  group_by(UNIQUE_CARRIER, DAY_OF_WEEK) %>%
  summarise(MeanDelayTime = mean(DEP_DELAY_NEW, na.rm = TRUE)) %>%
  collect()

# Create a faceted visualization looking at the mean delay time for each day 
# by carriers
ggplot(delay.by.day.carrier, aes(x = DAY_OF_WEEK, y = MeanDelayTime, fill = DAY_OF_WEEK)) +
  geom_col() +
  facet_wrap(~UNIQUE_CARRIER)

# Mean delay time by ORIGIN airport
delay.origin <- air.spark %>%
  select(ORIGIN, DEP_DELAY_NEW) %>% 
  group_by(ORIGIN) %>%
  summarise(MeanDelayTime = mean(DEP_DELAY_NEW, na.rm = TRUE)) %>%
  arrange(desc(MeanDelayTime)) %>%
  top_n(10) %>%
  collect()

# visualization for above aggregation
ggplot(delay.origin, aes(x = ORIGIN, y = MeanDelayTime)) + 
  geom_col()

# The next thing I want to look at is the mean delay time by the origin-destination
# airport pair. To do this, I'll have to mutate the data.frame and create a column
# which is the ORIGIN-DEST airport combination. 
air.spark <- air.spark %>%
  mutate("ORIGIN_DEST" = paste(ORIGIN, DEST, sep = "-"))

# Now I'll aggregate the data in a similar manner as above
delay.mean.pair <- air.spark %>%
  select(ORIGIN_DEST, DEP_DELAY_NEW) %>%
  group_by(ORIGIN_DEST) %>%
  summarise(MeanDelayTime = mean(DEP_DELAY_NEW, na.rm = TRUE)) %>%
  arrange(desc(MeanDelayTime)) %>%
  collect()

# glimpse the tibble
glimpse(delay.pair)
head(delay.pair, n = 10)

delay.pair.summary <- air.spark %>%
  select(ORIGIN_DEST, DEP_DELAY_NEW) %>%
  group_by(ORIGIN_DEST) %>%
  summarise(min(DEP_DELAY_NEW, na.rm = TRUE), 
            #approxQuantile(DEP_DELAY_NEW, probs = .25, na.rm = TRUE),
            MeanDelayTime = mean(DEP_DELAY_NEW, na.rm = TRUE),
            #median(DEP_DELAY_NEW, na.rm = TRUE),
            #quantile(DEP_DELAY_NEW, probs = .75, na.rm = TRUE),
            max(DEP_DELAY_NEW, na.rm = TRUE)) %>%
  arrange(desc(MeanDelayTime)) %>%
  collect()


# Now let's just gather the DEP_DELAY_NEW column by ORIGIN-DEST
delay.pair <- air.spark %>%
  select(ORIGIN, DEP_DELAY_NEW) %>%
  group_by(ORIGIN_DEST) %>%
  collect()

delay.pair <- as.data.table(delay.pair)
delay.pair <- delay.pair[sample(nrow(delay.pair), 10000)]
delay.pair[, ORIGIN := factor(ORIGIN)]
delay.pair <- delay.pair[ORIGIN %in% c("ATL", "LAX", "ORD", "DFW", "JFK", "DEN", "SFO", "LAS", 
                         "CLT", "SEA")]

delay.pair.stats <- delay.pair[, mean(DEP_DELAY_NEW, na.rm = TRUE), by = ORIGIN]
delay.median <- delay.pair[, median(DEP_DELAY_NEW, na.rm = TRUE), by = ORIGIN][, V1]
delay.sd <- delay.pair[, sd(DEP_DELAY_NEW, na.rm = TRUE), by = ORIGIN][, V1]
delay.se <- delay.sd / delay.pair[, .N, by = ORIGIN][, N]


delay.pair.stats <- cbind(delay.pair.stats, delay.sd)
delay.pair.stats <- cbind(delay.pair.stats, delay.se)


names(delay.pair.stats) <- c("ORIGIN", "MEAN", "MEDIAN", "SD")


# visualize the DEP_DELAY_NEW by ORIGIN 
ggplot(delay.pair, aes(x = DEP_DELAY_NEW, y = ORIGIN)) +
  geom_point(position = "jitter", color = "#3769dd", alpha = .5) +
  geom_point(data = delay.pair.stats, aes(x = MEAN)) +
  xlim(0, 50)

# Look at DISTANCE vs ARR_DELAY
ggplot(air, aes(x = ARR_DELAY, y = DISTANCE)) +
  geom_point(alpha = .01) +
  scale_x_continuous(limits = c(-10, 100))


# Look at DISTANCE vs DEP_DELAY
ggplot(air, aes(x = DEP_DELAY, y = DISTANCE)) +
  geom_point(alpha = .1)

# jittered scatter plot of DEP_DELAY by DISTANCE_GROUP with various alphas
# alpha = 1 (good for detecting where the outliers are for DEP_DELAY)
ggplot(air, aes(x = DEP_DELAY, y = DISTANCE_GROUP)) +
  geom_point(position = "jitter", alpha = 1)

# alpha = .1 
ggplot(air, aes(x = DEP_DELAY, y = DISTANCE_GROUP)) +
  geom_point(position = "jitter", alpha = .1)

# alpha = .05
ggplot(air, aes(x = DEP_DELAY, y = DISTANCE_GROUP)) +
  geom_point(position = "jitter", alpha = .05)

# alpha = .01
ggplot(air, aes(x = DEP_DELAY, y = DISTANCE_GROUP)) +
  geom_point(position = "jitter", alpha = .01)

# jittered scatter plot of DEP_DELAY by state
ggplot(air[air$ORIGIN_STATE_ABR %in% unique(air$ORIGIN_STATE_ABR)[1:10]], aes(x = DEP_DELAY, y = ORIGIN_STATE_ABR)) +
  geom_point(position = "jitter", alpha = .1)

# jittered scatter plot for DEP_DELAY by month
ggplot(air, aes(x = DEP_DELAY, y = MONTH)) +
  geom_point(position = "jitter", alpha = .1)


# jittered scatter plot for DEP_DELAY by month
ggplot(air, aes(x = DEP_DELAY, y = MONTH)) +
  geom_point(position = "jitter", alpha = .01)

# scatter plot of ARR_DELAY vs DEP_DELAY
ggplot(air, aes(x = DEP_DELAY, y = ARR_DELAY)) +
  geom_point(alpha = .05)

# jittered scatter plot of ARR_DELAY by DISTANCE GROUP
ggplot(air, aes(x = ARR_DELAY, y = DISTANCE_GROUP)) +
  geom_point(position = "jitter", alpha = .1)

################## Let's look at some density plots ############################
# DEP_DELAY
ggplot(air, aes(x = DEP_DELAY)) +
  geom_density() 

# DEP_DELAY with limited axis -10 to 120
ggplot(air, aes(x = DEP_DELAY)) +
  geom_density() +
  scale_x_continuous(limits = c(-10, 120)) +
  geom_vline(xintercept = mean(air$DEP_DELAY, na.rm = TRUE)) +
  geom_vline(xintercept = median(air$DEP_DELAY, na.rm = TRUE))


# DEP_DELAY with limited axis -10 to 60
ggplot(air, aes(x = DEP_DELAY)) +
  geom_density() +
  scale_x_continuous(limits = c(-10, 60))


# DEP_DELAY with limited axis -10 to 20
ggplot(air, aes(x = DEP_DELAY)) +
  geom_density() +
  scale_x_continuous(limits = c(-10, 20))


# DEP_DELAY with limited axis -10 to 10
ggplot(air, aes(x = DEP_DELAY)) +
  geom_density() +
  scale_x_continuous(limits = c(-10, 10))

# DEP_DELAY with limited axis 0 to 5
ggplot(air, aes(x = DEP_DELAY)) +
  geom_density() +
  scale_x_continuous(limits = c(0, 5))


# DEP_DELAY with limited axis 1 to 5 (1 means there was at least some delay)
ggplot(air, aes(x = DEP_DELAY)) +
  geom_density() +
  scale_x_continuous(limits = c(1, 5))

# AIR_TIME
ggplot(air, aes(x = AIR_TIME)) +
  geom_density() +
  scale_x_continuous(limits = c(0, max(air$AIR_TIME))) +
  geom_vline(xintercept = mean(air$AIR_TIME, na.rm = TRUE), color = "red") +
  geom_vline(xintercept = median(air$AIR_TIME, na.rm = TRUE))


# DISTANCE
ggplot(air, aes(x = DISTANCE)) +
  geom_density()

# CRS_DEP_TIME
ggplot(air, aes(x = as.numeric(CRS_DEP_TIME))) +
  geom_density() +
  geom_vline(xintercept = mean(as.numeric(air$CRS_DEP_TIME), na.rm = TRUE)) +
  geom_vline(xintercept = median(as.numeric(air$CRS_DEP_TIME), na.rm = TRUE), color = "red")

# CRS_ELAPSED_TIME 
ggplot(air, aes(x = CRS_ELAPSED_TIME)) +
  geom_density()

# CRS_DEP_TIME from 700 to 1700
ggplot(air, aes(x = as.numeric(CRS_DEP_TIME))) +
  geom_density() +
  scale_x_continuous(limits = c(700, 1700)) +
  geom_vline(xintercept = c(700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700))

# For the next one I'm going to pull the necessary columns and do some manipulation
dep_delay_per_dep_time <- air.spark %>%
                            select(DEP_DELAY, CRS_DEP_TIME) %>%
                            collect()

dep_delay_per_dep_time <- as.data.table(dep_delay_per_dep_time)

# Now I want to create a column which has a binned version of the CRS_DEP_TIME
dep_delay_per_dep_time[, DEP_TIME_BIN := ifelse(CRS_DEP_TIME < 1200,  "morning",
                                                ifelse(CRS_DEP_TIME >= 1200 & CRS_DEP_TIME < 1700, 
                                                       "afternoon", "night"))]

# Proportion of flights that have departure delays based on DEP_TIME_BIN
dep_delay_per_dep_time[DEP_DELAY > 0 & DEP_TIME_BIN == "night", .N] / dep_delay_per_dep_time[DEP_TIME_BIN == "morning", .N] 
dep_delay_per_dep_time[DEP_DELAY > 0 & DEP_TIME_BIN == "afternoon", .N] / dep_delay_per_dep_time[DEP_TIME_BIN == "afternoon", .N] 
dep_delay_per_dep_time[DEP_DELAY > 0 & DEP_TIME_BIN == "night", .N] / dep_delay_per_dep_time[DEP_TIME_BIN == "night", .N] 


#  


# Now I'm going to get densities for each of the binned DEP_TIME
ggplot(dep_delay_per_dep_time, aes(x = DEP_DELAY, color = DEP_TIME_BIN)) +
  geom_density() +
  scale_x_continuous(limits = c(1, 60))

dep_delay_per_dep_time[DEP_DELAY > 20][, .N, by = DEP_TIME_BIN]

# jittered scatter of DEP_DELAY by DEP_TIME_BIN
ggplot(dep_delay_per_dep_time, aes(x = DEP_DELAY, y = DEP_TIME_BIN)) +
  geom_point(position = "jitter", alpha = .1)


# Density plots using the flights_with_delays
# DEP_DELAY_NEW by MONTH
ggplot(flights_with_delays[MONTH %in% unique(MONTH)[1:6]], aes(x = DEP_DELAY_NEW, color = factor(MONTH))) +
  geom_density() +
  scale_x_continuous(limits = c(1, 60))


ggplot(flights_with_delays[MONTH %in% unique(MONTH)[7:12]], aes(x = DEP_DELAY_NEW, color = factor(MONTH))) +
  geom_density() +
  scale_x_continuous(limits = c(1, 60))


# DEP_DELAY_NEW by DAY_OF_WEEK
ggplot(flights_with_delays, aes(x = DEP_DELAY_NEW, color = factor(DAY_OF_WEEK))) +
  geom_density() +
  scale_x_continuous(limits = c(1, 60))

flights_with_delays %>% filter(LATE_AIRCRAFT_DELAY > 1) %>% 
  summarise("mean_delay_time" = mean(DEP_DELAY_NEW, na.rm = TRUE),
            "mean_late_aircraft" = mean(LATE_AIRCRAFT_DELAY))

# jittered scatter of ARR_DELAY by DISTANCE GROUP
ggplot(air, aes(x = ARR_DELAY, y = factor(DISTANCE_GROUP))) +
  geom_point(position = "jitter", alpha = .1)

# density plots of ARR_DELAY for each distance group
ggplot(air, aes(x = ARR_DELAY, color = factor(DISTANCE_GROUP))) +
  geom_density() +
  scale_x_continuous(limits = c(-10, 50)) +
  geom_vline(xintercept = mean(air[DISTANCE_GROUP == 11]$ARR_DELAY, na.rm = TRUE)) +
  geom_vline(xintercept = mean(air[DISTANCE_GROUP == 11]$ARR_DELAY, na.rm = TRUE), color = "red")

# scatter plot of ARR_DELAY by DISTANCE
ggplot(air, aes(x = DISTANCE, y = ARR_DELAY, color = factor(DISTANCE_GROUP))) +
  geom_point() 
  


########### Let's do some time-series plots ###################################
mean_delays_year <- air.spark %>%
          group_by(YEAR) %>%
          collect()

mean_delays_year <- mean_delays_year %>% summarise("mean_dep_delay" = mean(DEP_DELAY, na.rm = TRUE),
                                   "mean_dep_delay_new" = mean(DEP_DELAY_NEW, na.rm = TRUE),
                                   "mean_arr_delay_new" = mean(ARR_DELAY_NEW, na.rm = TRUE),
                                   "mean_arr_delay" = mean(ARR_DELAY, na.rm = TRUE)) 


mean_delays_year <- as.data.table(mean_delays_year)

# mean_dep_delay and arr_delay by YEAR
ggplot(mean_delays_temporal, aes(x = YEAR)) +
  geom_line(aes(y = mean_dep_delay), color = "red") +
  geom_line(aes(y = mean_arr_delay)) +
  geom_line(aes(y = mean_arr_delay_new), color = "blue") +
  geom_line(aes(y = mean_dep_delay_new), color = "pink")


# Find correlation among numeric variables
num_data <- air.spark %>% 
  select(YEAR, MONTH, DAY_OF_MONTH, DAY_OF_WEEK, DEP_DELAY,
         ARR_DELAY, CRS_ELAPSED_TIME, ACTUAL_ELAPSED_TIME, 
         DISTANCE) %>%
  na.omit() %>%
  collect()


cor_mat <- cor(num_data)
cor_mat <- corReorder(cor_mat)


# Now I want to find missing data information
nas <- air.spark %>% 
  mutate_all(is.na) %>%
  mutate_all(as.numeric) %>%
  summarise_all(sum) %>%
  collect()


nas <- as.data.table(nas)

# 

