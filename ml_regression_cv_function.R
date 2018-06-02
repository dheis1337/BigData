# This is a script that contains a generic function that can 
# be used for cross validating regression models. 
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
