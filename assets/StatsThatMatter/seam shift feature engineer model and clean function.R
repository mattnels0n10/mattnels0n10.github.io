library(tidyverse)
library(ggplot2)
library(ggformula)
library(caret)
library(readr)
library(dplyr)
library(xgboost)
library(GGally)
library(viridis)
library(gridExtra)
library(nnet)
library(kableExtra)
library(zoo)
library(plotly)
library(knitr)
library(rmarkdown)
library(DBI)
library(odbc)
library(doParallel)






####Load Data
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
      	SELECT player_name, pitch_name, game_type, plate_x, plate_z, batter, game_date, game_year, pitcher,
      	release_speed, release_pos_z, release_extension, release_pos_x, spin_axis, release_spin_rate, description, pfx_x,pfx_z,balls, strikes, 
      	p_throws, stand, bat_speed, delta_run_exp,on_3b,on_2b,on_1b,outs_when_up,arm_angle,n_thruorder_pitcher, n_priorpa_thisgame_player_at_bat,
      	pitcher_days_since_prev_game,batter_days_since_prev_game,	pitcher_days_until_next_game,sz_top,sz_bot, game_pk
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

# Display the first few rows of the DataFrame
head(major_df)

dbDisconnect(con)

setwd("C:/Users/baseball/Desktop/MLB App/")

############################### Training Data for Seam Shit Models #########################
set.seed(8)
# sample the data for model / remove non predictable columns
xshift <- major_df %>%
  filter(game_year == "2024",game_type == "R") %>% 
  select(pfx_x, pfx_z, spin_axis, release_extension, release_speed, release_pos_x, 
         release_spin_rate, p_throws, release_pos_z, Elevation)%>% 
  na.omit() 


############################### Seam Shit Models #######################################
ctrl <- trainControl(method = "cv", number = 5,search = "random",verboseIter = TRUE)
# train function to cycle through splits for fitting XG Boost model
fit_pfx_z <- train(pfx_z ~ release_extension + release_speed + release_spin_rate + 
                     spin_axis + release_pos_x + release_pos_z,
                   data = xshift,
                   method = "xgbTree",
                   preProcess = c("center", "scale"),
                   verbosity = 0,
                   tuneLength = 50,
                   trControl = ctrl,
                   metric = "MAE")

print(fit_pfx_z$results$MAE)
print(varImp(fit_pfx_z))
print(fit_pfx_z$bestTune)

fit_pfx_x <- train(pfx_x ~ p_throws + release_extension + release_speed + release_spin_rate + spin_axis + 
                     release_pos_z + release_pos_x,
                   data = xshift,
                   method = "xgbTree",
                   preProcess = c("center", "scale"),
                   verbosity = 0,
                   tuneLength = 50,
                   trControl = ctrl,
                   metric = "MAE")


print(fit_pfx_x$results$MAE)
print(varImp(fit_pfx_x))

# Save Model Seam Shift Models
saveRDS(fit_pfx_z, file = "C:/Users/baseball/Desktop/MLB App/seam_z_model.rds")
saveRDS(fit_pfx_x, file = "C:/Users/baseball/Desktop/MLB App/seam_x_model.rds")



#######################################################################################
########## --------------  ADDING IN ELEVATION AND MORE TRAINING ---------#############
#######################################################################################

#Train Seam Z

seam_z_df <- xshift %>% select(release_extension, release_speed, release_spin_rate,
                                 spin_axis, release_pos_x, release_pos_z, pfx_z, Elevation)

# Assuming 'df' is your dataset and 'target' is the outcome variable
train_index <- createDataPartition(seam_z_df$pfx_z, p = 0.8, list = FALSE)
train_data <- seam_z_df[train_index, ]
test_data <- seam_z_df[-train_index, ]

# Convert categorical variables using model.matrix (removes intercept column)
train_matrix <- model.matrix(pfx_z ~ . - 1, data = train_data)  # '- 1' removes intercept
test_matrix <- model.matrix(pfx_z ~ . - 1, data = test_data)

# Convert to DMatrix format
dtrain <- xgb.DMatrix(data = train_matrix, label = train_data$pfx_z)
dtest <- xgb.DMatrix(data = test_matrix, label = test_data$pfx_z)

# 3. Generate Hyperparameter Grid (250 combinations)

param_grid <- expand.grid(
  eta = seq(0.1, .3, length.out = 10),
  max_depth = seq(3, 10, by = 1),
  min_child_weight = seq(1, 10, length.out = 5),
  subsample = seq(0.6, 1.0, length.out = 5),
  colsample_bytree = seq(0.6, 1.0, length.out = 5),
  gamma = seq(0, 5, length.out = 5),
  lambda = seq(0, 1, length.out = 20),
  alpha = seq(0, 1, length.out = 20)
) %>%
  sample_n(50)

# Train XGBoost Model with Early Stopping and Hyperparameter Grid Search
num_rounds <- 5000  # Large since early stopping will prevent unnecessary iterations
early_stopping_rounds <- 25

# Store results
results_z <- data.frame()
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
  results_z <- rbind(results_z, data.frame(
    eta = params$eta,
    max_depth = params$max_depth,
    min_child_weight = params$min_child_weight,
    subsample = params$subsample,
    colsample_bytree = params$colsample_bytree,
    gamma = params$gamma,
    lambda = params$lambda,
    alpha = params$alpha,
    best_iteration = model$best_iteration,
    eval_rmse = min(model$evaluation_log$eval_rmse, na.rm = TRUE)  # Best RMSE
  ))
}

# Sort results by lowest RMSE
results_z <- results_z %>% arrange(eval_rmse)

# 5. Train Final Model with Best Hyperparameters
best_params <- results_z[1, ]  # Select best hyperparameter set


# CROSS VALIDATE BEST MODEL TO ENSURE HYPER TUNING DOES NOT CREATE OVER FITTING
train_control <- trainControl(
  method = "cv",  # Cross-validation
  number = 5,     # 5-fold cross-validation
  verboseIter = TRUE  # Optional: Show progress of training
)

# Train the model using caret::train()
fit_pfx_z2 <- train(pfx_z ~ .,
                    data = seam_z_df,
                    method = "xgbTree",
                    preProcess = c("center", "scale"),
                    verbosity = 1,
                    trControl = train_control,
                    tuneGrid = expand.grid(
                      nrounds = best_params$best_iteration,  # Use the best number of boosting rounds
                      max_depth = best_params$max_depth,
                      eta = best_params$eta,
                      gamma = best_params$gamma,
                      colsample_bytree = best_params$colsample_bytree,
                      min_child_weight = best_params$min_child_weight,
                      subsample = best_params$subsample),
                    lambda = best_params$lambda,  
                   alpha = best_params$alpha,    
                    metric = "RMSE")

# Save Model Seam Shift Models
saveRDS(fit_pfx_z2, file = "C:/Users/baseball/Desktop/MLB App/seam_z_model2.rds")


################### -----------Train Seam X ----------- ##########################
 
seam_x_df <- xshift %>% select(pfx_x, p_throws, release_extension, release_speed, release_spin_rate, spin_axis, 
                                 release_pos_z, release_pos_x, Elevation)

# Assuming 'df' is your dataset and 'target' pfx_x the outcome variable
train_index <- createDataPartition(seam_x_df$pfx_x, p = 0.8, list = FALSE)
train_data <- seam_x_df[train_index, ]
test_data <- seam_x_df[-train_index, ]

# Convert categorical variables using model.matrix (removes intercept column)
train_matrix <- model.matrix(pfx_x ~ . - 1, data = train_data)  # '- 1' removes intercept
test_matrix <- model.matrix(pfx_x ~ . - 1, data = test_data)

# Convert to DMatrix format
dtrain <- xgb.DMatrix(data = train_matrix, label = train_data$pfx_x)
dtest <- xgb.DMatrix(data = test_matrix, label = test_data$pfx_x)

# 3. Generate Hyperparameter Grid (250 combinations)

param_grid <- expand.grid(
  eta = seq(0.1, 0.3, length.out = 10),
  max_depth = seq(6, 12, by = 1),
  min_child_weight = seq(1, 10, length.out = 5),
  subsample = seq(0.6, 1.0, length.out = 5),
  colsample_bytree = seq(0.6, 1.0, length.out = 5),
  gamma = seq(0, 5, length.out = 5),
  lambda = seq(0, 1, length.out = 20),
  alpha = seq(0, 1, length.out = 20)
) %>%
  sample_n(50)

# Train XGBoost Model with Early Stopping and Hyperparameter Grid Search
num_rounds <- 5000  # Large since early stopping will prevent unnecessary iterations
early_stopping_rounds <- 25

# Store results
results_x <- data.frame()
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
  results_x <- rbind(results_x, data.frame(
    eta = params$eta,
    max_depth = params$max_depth,
    min_child_weight = params$min_child_weight,
    subsample = params$subsample,
    colsample_bytree = params$colsample_bytree,
    gamma = params$gamma,
    lambda = params$lambda,
    alpha = params$alpha,
    best_iteration = model$best_iteration,
    eval_rmse = min(model$evaluation_log$eval_rmse, na.rm = TRUE)  # Best RMSE
  ))
}

# Sort results by lowest RMSE
results_x <- results_x %>% arrange(eval_rmse)

# 5. Train Final Model with Best Hyperparameters
best_params <- results_x[1, ]  # Select best hyperparameter set


# CROSS VALIDATE BEST MODEL TO ENSURE HYPER TUNING DOES NOT CREATE OVER FITTING
train_control <- trainControl(
  method = "cv",  # Cross-validation
  number = 5,     # 5-fold cross-validation
  verboseIter = TRUE  # Optional: Show progress of training
)

# Train the model using caret::train()
fit_pfx_x2 <- train(pfx_x ~ .,
                    data = seam_x_df,
                    method = "xgbTree",
                    preProcess = c("center", "scale"),
                    verbosity = 1,
                    trControl = train_control,
                    tuneGrid = expand.grid(
                      nrounds = best_params$best_iteration,  # Use the best number of boosting rounds
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

saveRDS(fit_pfx_x2, file = "C:/Users/baseball/Desktop/MLB App/seam_x_model2.rds")

################################################################################
################################################################################



############################### Clean Function #######################################

all_clean_func <- function(df) {
  
  
  seam_x_model <- readRDS(file = "C:/Users/baseball/Desktop/MLB App/seam_x_model2.rds")
  seam_z_model <- readRDS(file = "C:/Users/baseball/Desktop/MLB App/seam_z_model2.rds")
  
  required_columns <- c(
    'release_speed', 'release_pos_x', 'release_pos_z', 'batter',
    'pitcher', 'description', 'game_type', 'stand', 'p_throws', 'balls',
    'strikes', 'pfx_x', 'pfx_z', 'plate_x', 'plate_z', 'sz_top', 'sz_bot',
    'release_spin_rate', 'release_extension',  'spin_axis','Elevation'
  )
  
  statcast2 <- df %>%
    filter(if_all(all_of(required_columns), ~ !is.na(.)))
  
  statcast2 <- statcast2 %>%
    mutate(
      seam_x = predict(seam_x_model, newdata = statcast2) - pfx_x,
      seam_z = predict(seam_z_model, newdata = statcast2) - pfx_z,
      swstr = case_when(description == "swinging_strike" ~ 1,
                        description == "swinging_strike_blocked" ~ 1,
                        TRUE ~ 0),
      swing = case_when(swstr == 1 ~ 1,
                        description == "hit_into_play" ~ 1,
                        TRUE ~ 0),
      pitch_name = case_when(pitch_name == "Slurve" ~ "Sweeper",
                             pitch_name == "Knuckle Curve" ~ "Curveball",
                             pitch_name == "Slow Curve" ~ "Curveball",
                             pitch_name == "Screwball" ~ "Changeup",
                             pitch_name == "Forkball" ~ "Split-Finger",
                             pitch_name == "2-Seam Fastball" ~ "Sinker",
                             TRUE ~ pitch_name),
      pitch_family = case_when(pitch_name %in% c("Sweeper","Curveball","Slider") ~ "Breaking",
                               pitch_name %in% c("4-Seam Fastball", "Sinker", "Cutter") ~ "Fastball",
                               TRUE ~ "Offspeed"),
      on_1b = ifelse(is.na(on_1b), 0, 1),
      on_2b = ifelse(is.na(on_2b), 0, 1),
      on_3b = ifelse(is.na(on_3b), 0, 1),
      runners_on = on_1b + on_2b + on_3b)%>%
    filter(release_speed > 70, balls != 4, strikes !=3,
           !pitch_name %in% c("Knuckleball","Eephus","Other","Pitch Out"),
           pfx_x != 0 , pfx_z !=0,!is.na(pitch_name))%>%
    mutate(across(c("balls","strikes","p_throws","stand","swstr","batter","game_type","pitch_name",
                    "game_year","on_1b","on_2b","on_3b","outs_when_up","runners_on","swing"),as.factor)) %>%
    select(-c(description))
  
  
  ####Create Lag Features and bind to data frame (Max Velo and Bat Speed Lag)
  statcast3 <- statcast2 %>% 
    group_by(pitcher,game_year) %>% 
    mutate(max_velo = max(release_speed,na.rm = TRUE)) %>% ungroup() %>%
    group_by(batter,game_year) %>% 
    mutate(n_swings = n(),
           bat_speed_mean = mean(bat_speed,na.rm = TRUE)) %>% ungroup()
  
  statcast3 <- statcast3 %>% 
    mutate(bat_speed_diff_from_avg = bat_speed_mean - bat_speed,
           bat_speed_diff_from_avg = ifelse(is.na(bat_speed),0,bat_speed_diff_from_avg),
           bat_speed_mean = ifelse(is.na(bat_speed), 72,bat_speed_mean))
  
  # Group by player_name and pitch_name, then summarize data
  statcast_summary <- statcast3 %>%
    group_by(pitch_name) %>%
    summarise(
      mean_spin_rate = mean(release_spin_rate, na.rm = TRUE),
      sd_spin_rate = sd(release_spin_rate, na.rm = TRUE)
    ) %>%
    ungroup()
  
  # Join summary statistics with the original data
  statcast1 <- statcast3 %>%
    left_join(statcast_summary, by = c("pitch_name"))
  
  # Filter out rows where release_spin_rate is more than 5 standard deviations away from the mean
  statcast1 <- statcast1 %>%
    filter(abs(release_spin_rate - mean_spin_rate) <= 5 * sd_spin_rate) %>%
    select(-c(mean_spin_rate,sd_spin_rate))
  
  return(statcast1)
}

saveRDS(all_clean_func, file = "all_clean_func.rds")


