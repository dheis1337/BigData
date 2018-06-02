library(data.table)
library(ggplot2)
library(e1071)
library(sparklyr)

source("~/MyStuff/DataScience/BigData/flights_cleaning.R")


# basic fit
lin.fit <- air.part$training %>%
  select(YEAR, ONE_HOT_DOW, CRS_DEP_TIME, DEST, DEP_DELAY, CRS_DEP_TIME, ARR_DELAY, ORIGIN, UNIQUE_CARRIER, 
         DISTANCE, DEP_DELAY_SQ, DEP_DELAY_CUBE) %>%
  na.omit() %>%
  ml_linear_regression(features = c("YEAR", "ONE_HOT_DOW", "DEP_DELAY", "CRS_DEP_TIME",
                                    "DEST", "ORIGIN", "UNIQUE_CARRIER", "DISTANCE", "DEP_DELAY_SQ",
                                    "DEP_DELAY_CUBE"), response = "ARR_DELAY")

# summary of linear fit
ml_summary(lin.fit)

# lasso fit
lasso.fit <- air.part$training %>%
  select(YEAR, ONE_HOT_DOW, CRS_DEP_TIME, DEST, DEP_DELAY, CRS_DEP_TIME, ARR_DELAY, ORIGIN, UNIQUE_CARRIER, 
         DISTANCE, DEP_DELAY_SQ, DEP_DELAY_CUBE) %>%
  na.omit() %>%
  ml_linear_regression(features = c("YEAR", "ONE_HOT_DOW", "DEP_DELAY", "CRS_DEP_TIME",
                                    "DEST", "ORIGIN", "UNIQUE_CARRIER", "DISTANCE", "DEP_DELAY_SQ",
                                    "DEP_DELAY_CUBE"), response = "ARR_DELAY", 
                       alpha = 1, lambda = .1)

# lasso summary
ml_summary(lasso.fit)

# ridge fit
ridge.fit <- air.part$training %>%
  select(YEAR, ONE_HOT_DOW, CRS_DEP_TIME, DEST, DEP_DELAY, CRS_DEP_TIME, ARR_DELAY, ORIGIN, UNIQUE_CARRIER, 
         DISTANCE, DEP_DELAY_SQ, DEP_DELAY_CUBE) %>%
  na.omit() %>%
  ml_linear_regression(features = c("YEAR", "ONE_HOT_DOW", "DEP_DELAY", "CRS_DEP_TIME",
                                    "DEST", "ORIGIN", "UNIQUE_CARRIER", "DISTANCE", "DEP_DELAY_SQ",
                                    "DEP_DELAY_CUBE"), response = "ARR_DELAY",
                       alpha = 0, lambda = .1)


# generalized linear model
gen.fit <- air.part$training %>% 
  select(YEAR, ONE_HOT_DOW, CRS_DEP_TIME, DEST, DEP_DELAY, CRS_DEP_TIME, ARR_DELAY, ORIGIN, UNIQUE_CARRIER, 
         DISTANCE, DEP_DELAY_SQ, DEP_DELAY_CUBE) %>%
  na.omit() %>%
  ml_generalized_linear_regression(features = c("YEAR", "ONE_HOT_DOW", "DEP_DELAY", "CRS_DEP_TIME",
                                     "DEST", "ORIGIN", "UNIQUE_CARRIER", "DISTANCE", "DEP_DELAY_SQ",
                                     "DEP_DELAY_CUBE"), response = "ARR_DELAY")
  
# summary of generalized linear model
ml_summary(gen.fit)

# create a list of all the models
ml_models <- list(
  "linear" = lin.fit,
  "lasso" = lasso.fit,
  "ridge" = ridge.fit, 
  "general" = gen.fit
)

# Create a function that can be used to conduct k-fold CV
ml_regression_cross_validation <- function(
  data,
  response, 
  features = NULL,
  model_fun, 
  k, 
  ...) {
  
  # create weights for the partitioning
  weights <- rep(1 / k, times = k)
  
  # create names for the weights vector
  names(weights) <- paste0("fold", as.character(1:k))
  
  # partition data using weights
  cv_folds <- sdf_partition(data, weights = weights)
  
  # get the indices for the training sets
  K <- 1:k
  indices <- purrr::map(K, ~ K[-.x])
  
  # create the training sets by binding the individual folds together. The return
  # object is a list where each element is a dataframe corresponding to the training 
  # data
  data_splits <- purrr::map(indices, ~ sdf_bind_rows(cv_folds[.x]))
  
  # create the vector of feature names if it isn't supplied
  if (is.null(features)) {
    # get column names
    cols <- colnames(data_splits[[1]])
  
  # create vector of feature names
  features <- cols[cols != response]
  }
  
  # map the model_fun over the training sets
  #fits <- purrr::map(.x = data_splits, 
   #                 .f = purrr::as_mapper(model_fun),
    #                response = response, 
     #               features = features)
  
  fits <- vector("list", length = length(data_splits))
  
  for (i in 1:length(data_splits)) {
    fits[[i]] <- data_splits[[i]] %>% model_fun(response = response,
                                                           features = features)
  }
  
  # calculate predictions
  preds <- purrr::map2(fits, K, ~ sdf_predict(.x, cv_folds[[.y]]))

 # evaluate predictions
  evals <- purrr::map(preds, 
                      ml_regression_evaluator, 
                      label_col = response)

  list(fits = fits,
       predictions = preds,
       evals = evals)
}

ml_regression_cross_validation(data = air.spark, 
                               response = "ARR_DELAY",
                               features = "DEP_DELAY",
                               k = 5, 
                               model_fun = ml_linear_regression)



fits <- vector("list", length = 2)

for (i in 1:length(data_splits)) {
  fits[[i]] <- data_splits[[i]] %>% ml_linear_regression(response = "ARR_DELAY",
                             features = "DEP_DELAY")
}

preds <- purrr::map2(fits, K, ~ sdf_predict(.x, data_splits[1:2][[.y]]))











# start of a pipeline for lamba parameter tuning for the lasso model
pipeline <- ml_pipeline(sc, uid = "test_pipe") %>%
  ft_r_formula(ARR_DELAY ~ DEP_DELAY + YEAR + ONE_HOT_DOW + DEP_DELAY_SQ + DEP_DELAY_CUBE +
                 CRS_DEP_TIME, uid = "test_form") %>%
  ml_linear_regression(uid = "test_lin")

# cross-validation for lasso model
test.cvm <- air.spark %>% ml_cross_validator(estimator = pipeline, num_folds = 5,
                                 estimator_param_maps = list("test_lin" = list(
                                   "lambda" = c(.001, .01, .1, 1, 10, 100))),
                                 evaluator = ml_regression_evaluator(sc))
                                 
# summary of above cross-validation
ml_validation_metrics(test.cvm)

