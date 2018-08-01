library(data.table)
library(ggplot2)
library(e1071)
library(sparklyr)

source("~/MyStuff/DataScience/BigData/flights_cleaning.R")



# pipline for random forest
# start of a pipeline for lamba parameter tuning for the lasso model
pipeline <- ml_pipeline(sc, uid = "test_pipe") %>%
  ft_r_formula(ARR_DELAY ~ DEP_DELAY + YEAR + ONE_HOT_DOW + DEP_DELAY_SQ + DEP_DELAY_CUBE +
                 CRS_DEP_TIME  + DISTANCE + DEP_DELAY_DISTANCE + DEP_DELAY_ELAPSED_TIME +
                 DEP_DELAY_AIR_TIME + DIVERTED, uid = "test_form") %>%
  ml_random_forest_regressor(uid = "test_rf")

# cross-validation for lasso model
test.cvm <- air.spark %>% 
  select(ARR_DELAY, DEP_DELAY, YEAR, ONE_HOT_DOW, DEP_DELAY_SQ, DEP_DELAY_CUBE, 
         CRS_DEP_TIME, DISTANCE, DEP_DELAY_DISTANCE, DEP_DELAY_ELAPSED_TIME, 
         DEP_DELAY_AIR_TIME, CRS_ELAPSED_TIME, CRS_DEP_TIME, DEP_DEL15, DIVERTED) %>%
  na.omit() %>%
  ml_cross_validator(estimator = pipeline, num_folds = 5,
                                             estimator_param_maps = list("test_rf" = list(
                                               "num_trees" = c(100, 250, 500),
                                               "max_depth" = c(2, 3, 4))),
                                             evaluator = ml_regression_evaluator(sc))



# summary of above cross-validation
ml_validation_metrics(test.cvm)

# fit model using optimal num_trees = 250 and max_depth = 4
rf <- air.part$training %>% ml_random_forest_regressor(response = "ARR_DELAY",
                                                       features = c("DEP_DELAY", "ONE_HOT_DOW", "YEAR", "MONTH",
                                                                    "DEP_DELAY_CUBE", "DEP_DELAY_SQ", "CRS_DEP_TIME",
                                                                    "DEP_DELAY_DISTANCE", "DEP_DELAY_ELAPSED_TIME",
                                                                    "DIVERTED", "DEP_DEL15"),
                                                       num_trees = 250,
                                                       max_depth = 4)


# feature importance
rf$model$feature_importances

rf$.features

feat_imp <- data.table(features = rf$.features,
                       importance = rf$model$feature_importances)


p <- ggplot(feat_imp, aes(x = importance, y = features)) +
  geom_col()





