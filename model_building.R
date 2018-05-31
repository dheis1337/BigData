library(data.table)
library(ggplot2)
library(e1071)
library(sparklyr)

source("~/MyStuff/DataScience/BigData/flights_cleaning.R")


# basic fit
lin.fit <- air.part$training %>%
  select(YEAR, DAY_OF_WEEK, CRS_DEP_TIME, DEST, DEP_DELAY, CRS_DEP_TIME, ARR_DELAY, ORIGIN, UNIQUE_CARRIER, 
         DISTANCE, DEP_DELAY_SQ, DEP_DELAY_CUBE) %>%
  na.omit() %>%
  ml_linear_regression(features = c("YEAR", "DAY_OF_WEEK", "DEP_DELAY", "CRS_DEP_TIME",
                                    "DEST", "ORIGIN", "UNIQUE_CARRIER", "DISTANCE", "DEP_DELAY_SQ",
                                    "DEP_DELAY_CUBE"), response = "ARR_DELAY")


lasso.fit <- air.part$training %>%
  select(YEAR, DAY_OF_WEEK, CRS_DEP_TIME, DEST, DEP_DELAY, CRS_DEP_TIME, ARR_DELAY, ORIGIN, UNIQUE_CARRIER, 
         DISTANCE, DEP_DELAY_SQ, DEP_DELAY_CUBE) %>%
  na.omit() %>%
  ml_linear_regression(features = c("YEAR", "DAY_OF_WEEK", "DEP_DELAY", "CRS_DEP_TIME",
                                    "DEST", "ORIGIN", "UNIQUE_CARRIER", "DISTANCE", "DEP_DELAY_SQ",
                                    "DEP_DELAY_CUBE"), response = "ARR_DELAY", 
                       alpha = 1, lambda = 1)

ml_summary(lasso.fit)

ridge.fit <- air.part$training %>%
  select(YEAR, DAY_OF_WEEK, CRS_DEP_TIME, DEST, DEP_DELAY, CRS_DEP_TIME, ARR_DELAY, ORIGIN, UNIQUE_CARRIER, 
         DISTANCE, DEP_DELAY_SQ, DEP_DELAY_CUBE) %>%
  na.omit() %>%
  ml_linear_regression(features = c("YEAR", "DAY_OF_WEEK", "DEP_DELAY", "CRS_DEP_TIME",
                                    "DEST", "ORIGIN", "UNIQUE_CARRIER", "DISTANCE", "DEP_DELAY_SQ",
                                    "DEP_DELAY_CUBE"), response = "ARR_DELAY",
                       alpha = 0, lambda = 1)

pipeline <- ml_pipeline(sc, uid = "test_pipe") %>%
  ft_r_formula(ARR_DELAY ~ DEP_DELAY + YEAR + DAY_OF_WEEK, uid = "test_form") %>%
  ml_linear_regression(uid = "test_lin", alpha = 1)

test.cvm <- air.spark %>% ml_cross_validator(estimator = pipeline, 
                                 estimator_param_maps = list("test_lin" = list(
                                   "lambda" = c(.001, .01, .1, 1, 10, 100))),
                                 evaluator = ml_regression_evaluator(sc))
                                 
