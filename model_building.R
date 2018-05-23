library(data.table)
library(ggplot2)
library(e1071)
library(sparklyr)


config <- spark_config()

config$`sparklyr.shell.driver-memory` <- "4G"
config$`sparklyr.shell.executor-memory` <- "4G"
config$`spark.yarn.executor.memoryOverhead` <- "1G"

sc <- spark_connect(master = "local")

air <- fread("~/MyStuff/DataScience/BigData/data/AirOnTimeCSV/flightssamp.csv")

air.spark <- copy_to(sc, air, "air")

air.spark <- air.spark %>%
  select(ARR_DELAY, DEP_DELAY, DAY_OF_WEEK, DISTANCE, DIVERTED, CRS_DEP_TIME) %>%
  na.omit() 



# fit model
fit <- air.spark %>%
  ml_linear_regression(response = "ARR_DELAY", 
                       features = c("DEP_DELAY", "DAY_OF_WEEK", "DISTANCE", 
                                    "DIVERTED", "CRS_DEP_TIME"))

preds <- sdf_predict(air.spark, fit) %>%
  collect()

resids <- sdf_residuals(fit) %>%
  collect()

err.dt <- data.table("fitted_vals" = preds, 
                     "residuals" = resids)


ggplot(err.dt, aes(x = fitted_vals.prediction, y = residuals.residuals)) +
  geom_point(alpha = .1) +
  scale_x_continuous(limits = c(-10, 100)) +
  scale_y_continuous(limits = c(-50, 200))

