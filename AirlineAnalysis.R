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
air.spark <- copy_to(sc, air, "air")

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


delay.pair.stats <- cbind(delay.pair.stats, delay.pair[, sd(DEP_DELAY_NEW, na.rm = TRUE), by = ORIGIN][, 2])


names(delay.pair.stats) <- c("ORIGIN", "MEAN", "MEDIAN", "SD")


# visualize the DEP_DELAY_NEW by ORIGIN pair
ggplot(delay.pair, aes(x = DEP_DELAY_NEW, y = ORIGIN)) +
  geom_point(position = "jitter", color = "#3769dd", alpha = .5) +
  geom_point(data = delay.pair.stats, aes(x = MEAN)) +
  xlim(0, 50)





