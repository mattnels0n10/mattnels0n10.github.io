####Load Packages
library(tidyverse)
library(caret)
library(xgboost)
library(DBI)
library(odbc)
library(ParBayesianOptimization)



setwd("C:/Users/baseball/Desktop/MLB App/")

# ==== Load and Clean Data ====
con <- DBI::dbConnect(
  odbc::odbc(),
  Driver   = "ODBC Driver 17 for SQL Server",  # Ensure you have the correct driver installed
  Server   = "YEMEN",               # Replace with your SQL Server name
  Database = "MLB App",                         # Name of the database
  Trusted_Connection = "Yes"                   # Use "Yes" for Windows Authentication, or specify UID and PWD for SQL Server Authentication
)

# Your SQL query
major_query <- "SELECT  m.*,e.Elevation
      FROM(
      	SELECT player_name, pitch_name, game_type, plate_x, plate_z, batter, game_date, game_year, pitcher, vx0, vy0, vz0,
      	release_speed, release_pos_z, release_extension, release_pos_x, spin_axis, release_spin_rate, description, pfx_x,pfx_z,balls, strikes, 
      	p_throws, stand, bat_speed, delta_run_exp,on_3b,on_2b,on_1b,outs_when_up,arm_angle,n_thruorder_pitcher, n_priorpa_thisgame_player_at_bat,
      	pitcher_days_since_prev_game,batter_days_since_prev_game,	pitcher_days_until_next_game,sz_top,sz_bot, game_pk, pitch_number
      	FROM MajorLeagueRawData
      	WHERE game_year = 2024) m
      JOIN
      	(SELECT DISTINCT home_id, game_pk FROM Gamelogs WHERE game_year = 2024 and game_type = 'Regular Season') g
      	ON m.game_pk = g.game_pk
      JOIN 
      	(SELECT team_id, Elevation FROM stadium_elevation) e	
      ON g.home_id = e.team_id"

# Execute the query and save the results in a DataFrame
major_df <- dbGetQuery(con, major_query)

dbDisconnect(con)


# Load clean function from file
all_clean_func <- readRDS("all_clean_func.rds")

# Clean dataframe 
statcast1 <- all_clean_func(major_df)


# ==== xWHIFF TRAINING DATA =====

set.seed(8)
xwhiff_training_data <- statcast1 %>% 
  filter(swing == "1",game_year == "2024",game_type == "R") %>%
  select(pfx_x, pfx_z, spin_axis, release_extension, release_pos_x, release_pos_z, release_speed,
         release_spin_rate, balls, strikes, p_throws, stand, max_velo, 
         seam_z, seam_x,swstr, Elevation,n_priorpa_thisgame_player_at_bat, n_thruorder_pitcher, pitch_number)%>%  ## Removing Bat Speed Data *bat_speed_diff_from_avg*
  na.omit()

# Save xWhiff Training Data
saveRDS(xwhiff_training_data, 
        file = "C:/Users/baseball/Desktop/MLB App/xwhiff2024_training_data.rds")

# ==== xLOCATION TRAINING DATA ====

#xLocation Training Data
set.seed(8)
xlocation_training_data <- statcast1 %>%
  filter(game_year == "2024",game_type == "R",pfx_x != 0 , pfx_z !=0,release_speed > 60) %>%
  select(pitch_name, plate_x, plate_z, balls, strikes, p_throws, stand, delta_run_exp, outs_when_up, 
         on_3b, on_2b, on_1b, sz_top, sz_bot, Elevation, n_priorpa_thisgame_player_at_bat, n_thruorder_pitcher, pitch_number) %>% 
  na.omit() 

#Save Location Training Data
saveRDS(xlocation_training_data, 
        file = "C:/Users/baseball/Desktop/MLB App/xlocation2024_training_data.rds")


# ====xPitch TRAINING DATA ====

xpitch_training_data <- statcast1 %>%
  filter(game_year == "2024",game_type == "R",pfx_x != 0 , pfx_z !=0,release_speed > 60) %>% 
  select(pfx_x, pfx_z, spin_axis, release_extension, release_pos_x, release_pos_z, release_speed, vx0, vy0, vz0,
         release_spin_rate, balls, strikes, p_throws, stand, max_velo, plate_z, plate_x, seam_z, 
         seam_x, delta_run_exp, outs_when_up, on_3b, on_2b, on_1b, swing,sz_top,sz_bot,Elevation,
         n_priorpa_thisgame_player_at_bat, n_thruorder_pitcher, pitch_number)%>%  #bat_speed_diff_from_avg
  na.omit()
# 
saveRDS(xpitch_training_data, 
        file = "C:/Users/baseball/Desktop/MLB App/xpitch2024_training_data.rds")

# ==== xPitch 2nd Model -- Early Stopping w/ Elevation ====

# 2. Data Split (80:20 Split)
set.seed(42)

# Assuming 'df' is your dataset and 'target' is the outcome variable
train_index <- createDataPartition(xpitch_training_data$delta_run_exp, p = 0.8, list = FALSE)
train_data <- xpitch_training_data[train_index, ]
test_data <- xpitch_training_data[-train_index, ]

# Convert categorical variables using model.matrix (removes intercept column)
train_matrix <- model.matrix(delta_run_exp ~ . - 1, data = train_data)  # '- 1' removes intercept
test_matrix <- model.matrix(delta_run_exp ~ . - 1, data = test_data)

# Convert to DMatrix format
dtrain <- xgb.DMatrix(data = train_matrix, label = train_data$delta_run_exp)
dtest <- xgb.DMatrix(data = test_matrix, label = test_data$delta_run_exp)

param_grid <- expand.grid(
  eta = seq(0.005, 0.025, length.out = 10),           # focus lower eta
  max_depth = seq(6, 11, length.out = 6),            # limit max_depth to 6-11
  min_child_weight = seq(4, 7, length.out = 4),      # narrow range near best
  subsample = seq(0.5, 0.8, length.out = 4),        # try higher subsample (good)
  colsample_bytree = seq(0.6, 0.9, length.out = 4),  # focus on higher colsample
  gamma = 0,                                          # fix gamma at 0 (best)
  lambda = c(0, 0.1, 1, 2, 5, 10),            # around current best 1.0
  alpha = seq(0, 1, length.out = 10)                 # smaller alpha near 0-0.15
) %>% sample_n(50) 

# Train XGBoost Model with Early Stopping and Hyperparameter Grid Search
num_rounds <- 7500  # Large since early stopping will prevent unnecessary iterations
early_stopping_rounds <- 75

# Store results
x_pitch_results <- data.frame()
old <- Sys.time()
for (i in 1:nrow(param_grid)) {
  
  params <- as.list(param_grid[i, ])  # Get parameters for the current iteration
  
  cat(paste0("Iteration ", i, "/", nrow(param_grid), " — eta: ", params$eta, "\n"))
  
  # Train model with early stopping
  model <- xgb.train(
    params = params,
    data = dtrain,
    nrounds = num_rounds,
    watchlist = list(train = dtrain, eval = dtest),
    early_stopping_rounds = early_stopping_rounds,
    objective = "reg:squarederror",  # Use "binary:logistic" for classification
    eval_metric = "rmse",  # Use "mae" if you want Mean Absolute Error
    verbose = 1  # Set to 1 for progress updates
  )
  
  # Store results
  x_pitch_results <- rbind(x_pitch_results, data.frame(
    eta = params$eta,
    max_depth = params$max_depth,
    min_child_weight = params$min_child_weight,
    subsample = params$subsample,
    colsample_bytree = params$colsample_bytree,
    gamma = params$gamma,
    lambda = params$lambda,
    alpha = params$alpha,
    nrounds = model$best_iteration,
    eval_rmse = min(model$evaluation_log$eval_rmse, na.rm = TRUE)  # Best RMSE
  ))
}

# Sort results by lowest RMSE
x_pitch_results <- x_pitch_results %>% arrange(eval_rmse)

# 5. Train Final Model with Best Hyperparameters
best_params <- x_pitch_results[1, ]  # Select best hyperparameter set


# CROSS VALIDATE BEST MODEL TO ENSURE HYPER TUNING DOES NOT CREATE OVER FITTING
train_control <- trainControl(
  method = "cv",  # Cross-validation
  number = 5,     # 5-fold cross-validation
  verboseIter = TRUE  # Optional: Show progress of training
)

# Train the model using caret::train()
final_xpitch_model2 <- train(delta_run_exp ~ .,
                            data = xpitch_training_data,
                            method = "xgbTree",
                            preProcess = c("center", "scale"),
                            verbosity = 1,
                            trControl = train_control,
                            tuneGrid = expand.grid(
                              nrounds = best_params$nrounds,  # Use the best number of boosting rounds
                              max_depth = best_params$max_depth,
                              eta = best_params$eta,
                              gamma = best_params$gamma,
                              colsample_bytree = best_params$colsample_bytree,
                              min_child_weight = best_params$min_child_weight,
                              subsample = best_params$subsample),
                            lambda = best_params$lambda,  
                            alpha = best_params$alpha,    
                            metric = "RMSE")

time_elapsed <- Sys.time() - old
print(time_elapsed)

#Best Params 
#0.005 11 5.50 0.7 0.8 0.00 1.00000000 0.15789474 1607 0.2311796

# 6. Save Model & Make Predictions
saveRDS(final_xpitch_model2,  "C:/Users/baseball/Desktop/MLB App/best____elevation____pitch_model2.rds")

# ==== xWhiff Model 2 Early Stopping w/ Elevation ====

# 2. Data Split (80:20 Split)
set.seed(42)

# Assuming 'df' is your dataset and 'target' is the outcome variable
xwhiff_training_data$swstr <- as.numeric(xwhiff_training_data$swstr)-1
train_index <- createDataPartition(xwhiff_training_data$swstr, p = 0.8, list = FALSE)
train_data <- xwhiff_training_data[train_index, ]
test_data <- xwhiff_training_data[-train_index, ]

#print(glimpse(xwhiff_training_data))

# Convert categorical variables using model.matrix (removes intercept column)
train_matrix <- model.matrix(swstr ~ . - 1, data = train_data)  # '- 1' removes intercept
test_matrix <- model.matrix(swstr ~ . - 1, data = test_data)

# Convert to DMatrix format
dtrain <- xgb.DMatrix(data = train_matrix, label = train_data$swstr)
dtest <- xgb.DMatrix(data = test_matrix, label = test_data$swstr)

# 3. Generate Hyperparameter Grid (400 combinations)
set.seed(42)

param_grid <- expand.grid(
  eta = seq(0.001, 0.025, length.out = 10),           # focus lower eta
  max_depth = seq(6, 11, length.out = 6),            # limit max_depth to 6-11
  min_child_weight = seq(4, 7, length.out = 4),      # narrow range near best
  subsample = seq(0.5, 0.8, length.out = 4),        # try higher subsample (good)
  colsample_bytree = seq(0.6, 0.9, length.out = 4),  # focus on higher colsample
  gamma = 0,                                          # fix gamma at 0 (best)
  lambda = c(0, 0.1, 1, 2, 5, 10),            # around current best 1.0
  alpha = seq(0, 1, length.out = 10)                 # smaller alpha near 0-0.15
) %>% sample_n(200) 

# Train XGBoost Model with Early Stopping and Hyperparameter Grid Search
num_rounds <- 7500  # Large since early stopping will prevent unnecessary iterations
early_stopping_rounds <- 75

# Store results
results_whiff <- data.frame()
old <- Sys.time()
for (i in 1:nrow(param_grid)) {
  
  params <- as.list(param_grid[i, ])  # Get parameters for the current iteration
  
  cat(paste0("Iteration ", i, "/", nrow(param_grid), " — eta: ", params$eta, "\n"))
  
  # Train model with early stopping
  model <- xgb.train(
    params = params,
    data = dtrain,
    nrounds = num_rounds,
    watchlist = list(train = dtrain, eval = dtest),
    early_stopping_rounds = early_stopping_rounds,
    objective = "binary:logistic",  # Use "binary:logistic" for binary classification
    eval_metric = "error",
    verbose = 1  # Set to 1 for progress updates
  )
  
  # Store results
  results_whiff <- rbind(results_whiff, data.frame(
    eta = params$eta,
    max_depth = params$max_depth,
    min_child_weight = params$min_child_weight,
    subsample = params$subsample,
    colsample_bytree = params$colsample_bytree,
    gamma = params$gamma,
    lambda = params$lambda,
    alpha = params$alpha,
    nrounds = model$best_iteration,
    eval_error = min(model$evaluation_log$eval_error, na.rm = TRUE)
  ))
}

# Sort results by lowest RMSE
results_whiff <- results_whiff %>% arrange(eval_error)

# 5. Train Final Model with Best Hyperparameters
best_params <- results_whiff[1, ]  # Select best hyperparameter set

# CROSS VALIDATE BEST MODEL TO ENSURE HYPER TUNING DOES NOT CREATE OVER FITTING
train_control <- trainControl(
  method = "cv",  # Cross-validation
  number = 5,     # 5-fold cross-validation
  verboseIter = TRUE  # Optional: Show progress of training
)

#convert swstr back to a factor
xwhiff_training_data$swstr <- factor(xwhiff_training_data$swstr, levels = c(0, 1))


# Train the model using caret::train()
final_xwhiff_model2 <- train(swstr ~ .,
                            data = xwhiff_training_data,
                            method = "xgbTree",
                            preProcess = c("center", "scale"),
                            verbosity = 1,
                            trControl = train_control,
                            tuneGrid = expand.grid(
                              nrounds = best_params$nrounds,  # Use the best number of boosting rounds
                              max_depth = best_params$max_depth,
                              eta = best_params$eta,
                              gamma = best_params$gamma,
                              colsample_bytree = best_params$colsample_bytree,
                              min_child_weight = best_params$min_child_weight,
                              subsample = best_params$subsample),
                            lambda = best_params$lambda,  
                            alpha = best_params$alpha,    
                            metric = "Accuracy")



time_elapsed <- Sys.time() - old
print(time_elapsed)

#importance_matrix 


#Current Best Accuracy --- 0.6500503

# 6. Save Model & Make Predictions
saveRDS(final_xwhiff_model2,  "C:/Users/baseball/Desktop/MLB App/best____elevation____whiff_model2.rds")


# ==== xLocation 2nd Model -- Early Stopping w/ Elevation ====


# 2. Data Split (80:20 Split)
set.seed(42)

# Assuming 'df' is your dataset and 'target' is the outcome variable
train_index <- createDataPartition(xlocation_training_data$delta_run_exp, p = 0.8, list = FALSE)
train_data <- xlocation_training_data[train_index, ]
test_data <- xlocation_training_data[-train_index, ]

# Convert categorical variables using model.matrix (removes intercept column)
train_matrix <- model.matrix(delta_run_exp ~ . - 1, data = train_data)  # '- 1' removes intercept
test_matrix <- model.matrix(delta_run_exp ~ . - 1, data = test_data)

# Convert to DMatrix format
dtrain <- xgb.DMatrix(data = train_matrix, label = train_data$delta_run_exp)
dtest <- xgb.DMatrix(data = test_matrix, label = test_data$delta_run_exp)

# 3. Generate Hyperparameter Grid (250 combinations)

param_grid <- expand.grid(
  eta = seq(0.001, 0.025, length.out = 10),           # focus lower eta
  max_depth = seq(6, 11, length.out = 6),            # limit max_depth to 6-11
  min_child_weight = seq(4, 7, length.out = 4),      # narrow range near best
  subsample = seq(0.5, 0.8, length.out = 4),        # try higher subsample (good)
  colsample_bytree = seq(0.6, 0.9, length.out = 4),  # focus on higher colsample
  gamma = 0,                                          # fix gamma at 0 (best)
  lambda = c(0, 0.1, 1, 2, 5, 10),            # around current best 1.0
  alpha = seq(0, 1, length.out = 10)                 # smaller alpha near 0-0.15
) %>% sample_n(50)

# Train XGBoost Model with Early Stopping and Hyperparameter Grid Search
num_rounds <- 7500  # Large since early stopping will prevent unnecessary iterations
early_stopping_rounds <- 50

# Store results
xlocation_results <- data.frame()
old <- Sys.time()
for (i in 1:nrow(param_grid)) {
  
  params <- as.list(param_grid[i, ])  # Get parameters for the current iteration
  
  cat(paste0("Iteration ", i, "/", nrow(param_grid), " — eta: ", params$eta, "\n"))
  
  # Train model with early stopping
  model <- xgb.train(
    params = params,
    data = dtrain,
    nrounds = num_rounds,
    watchlist = list(train = dtrain, eval = dtest),
    early_stopping_rounds = early_stopping_rounds,
    objective = "reg:squarederror",  # Use "binary:logistic" for classification
    eval_metric = "rmse",  # Use "mae" if you want Mean Absolute Error
    verbose = 1  # Set to 1 for progress updates
  )
  
  # Store results
  xlocation_results <- rbind(xlocation_results, data.frame(
    eta = params$eta,
    max_depth = params$max_depth,
    min_child_weight = params$min_child_weight,
    subsample = params$subsample,
    colsample_bytree = params$colsample_bytree,
    gamma = params$gamma,
    lambda = params$lambda,
    alpha = params$alpha,
    nrounds = model$best_iteration,
    eval_rmse = min(model$evaluation_log$eval_rmse, na.rm = TRUE)  # Best RMSE
  ))
}

# Sort results by lowest RMSE
xlocation_results <- xlocation_results %>% arrange(eval_rmse)

# 5. Train Final Model with Best Hyperparameters
best_params <- xlocation_results[1, ]  # Select best hyperparameter set


# CROSS VALIDATE BEST MODEL TO ENSURE HYPER TUNING DOES NOT CREATE OVER FITTING
train_control <- trainControl(
  method = "cv",  # Cross-validation
  number = 5,     # 5-fold cross-validation
  verboseIter = TRUE  # Optional: Show progress of training
)

# Train the model using caret::train()
final_xlocation_model2 <- train(delta_run_exp ~ .,
                             data = xlocation_training_data,
                             method = "xgbTree",
                             preProcess = c("center", "scale"),
                             verbosity = 1,
                             trControl = train_control,
                             tuneGrid = expand.grid(
                               nrounds = best_params$nrounds,  # Use the best number of boosting rounds
                               max_depth = best_params$max_depth,
                               eta = best_params$eta,
                               gamma = best_params$gamma,
                               colsample_bytree = best_params$colsample_bytree,
                               min_child_weight = best_params$min_child_weight,
                               subsample = best_params$subsample),
                             lambda = best_params$lambda,  
                             alpha = best_params$alpha,    
                             metric = "RMSE")

time_elapsed <- Sys.time() - old
print(time_elapsed)

# 6. Save Model & Make Predictions
saveRDS(final_xlocation_model2,  "C:/Users/baseball/Desktop/MLB App/best____elevation____location_model.rds")
