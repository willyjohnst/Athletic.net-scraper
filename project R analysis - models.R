library(dplyr)
library(ggplot2)
library(tidyr)
library(broom)
library(patchwork)

if(!dir.exists("athlete_plots")) dir.create("athlete_plots")

run_linear_regression <- function(data, event) {
  
  # Build the per-athlete summary: one row per athlete with HS PB and College PB
  regression_data <- data |>
    filter(standard_event == event) |>
    group_by(athlete_id, gender) |>
    summarize(
      HS_PB = if(any(grade >= 9 & grade <= 12, na.rm = TRUE))
        min(mark_seconds[grade >= 9 & grade <= 12], na.rm = TRUE)
      else NA_real_,
      College_PB = if(any(grade >= 21 & grade <= 25, na.rm = TRUE))
        min(mark_seconds[grade >= 21 & grade <= 25], na.rm = TRUE)
      else NA_real_,
      .groups = "drop"
    ) |>
    filter(!is.na(HS_PB) & !is.na(College_PB))
  
  results_list <- list()
  
  for (g in c("F", "M")) {
    
    gdata <- regression_data |> filter(gender == g)
    n <- nrow(gdata)
    
    cat("\n====================================================================\n")
    cat(sprintf("LINEAR REGRESSION: %s | Gender: %s | N = %d\n", event, g, n))
    cat("====================================================================\n")
    
    # --- Train/Test Split (80/20) ---
    set.seed(42) # Reproducibility
    train_idx <- sample(1:n, size = floor(0.8 * n))
    train <- gdata[train_idx, ]
    test  <- gdata[-train_idx, ]
    
    cat(sprintf("Train set: %d | Test set: %d\n\n", nrow(train), nrow(test)))
    
    # --- Fit the model on training data ---
    model <- lm(College_PB ~ HS_PB, data = train)
    
    # --- Full summary ---
    s <- summary(model)
    print(s)
    
    # --- Key metrics ---
    train_preds <- predict(model, train)
    test_preds  <- predict(model, test)
    
    train_rmse <- sqrt(mean((train$College_PB - train_preds)^2))
    test_rmse  <- sqrt(mean((test$College_PB - test_preds)^2))
    
    # R² on test set (manually, since summary() only gives train R²)
    ss_res <- sum((test$College_PB - test_preds)^2)
    ss_tot <- sum((test$College_PB - mean(test$College_PB))^2)
    test_r2 <- 1 - (ss_res / ss_tot)
    
    cat("\n--- KEY METRICS ---\n")
    cat(sprintf("Train R²:   %.4f\n", s$r.squared))
    cat(sprintf("Test  R²:   %.4f\n", test_r2))
    cat(sprintf("Train RMSE: %.2f seconds\n", train_rmse))
    cat(sprintf("Test  RMSE: %.2f seconds\n", test_rmse))
    cat(sprintf("Intercept:  %.2f seconds\n", coef(model)[1]))
    cat(sprintf("Slope:      %.4f\n", coef(model)[2]))
    cat(sprintf("Interpretation: For every 1-second improvement in HS PB,\n"))
    cat(sprintf("  college PB improves by ~%.2f seconds on average.\n", coef(model)[2]))
    
    if (coef(model)[2] < 1) {
      cat("  (Slope < 1 = regression to the mean: slower HS runners \n")
      cat("   improve more, faster HS runners improve less.)\n")
    }
    cat("====================================================================\n")
    
    # Store for later use
    results_list[[g]] <- list(
      model = model, train = train, test = test,
      train_rmse = train_rmse, test_rmse = test_rmse,
      test_r2 = test_r2, full_data = gdata
    )
  }
  
  return(results_list)
}

plot_residuals <- function(results, event) {
  
  plots <- list()
  
  for (g in c("F", "M")) {
    r <- results[[g]]
    
    # Get residuals and fitted values from FULL data refit for cleaner plot
    full_model <- lm(College_PB ~ HS_PB, data = r$full_data)
    diag_df <- data.frame(
      fitted = fitted(full_model),
      residuals = resid(full_model)
    )
    
    plots[[g]] <- ggplot(diag_df, aes(x = fitted, y = residuals)) +
      geom_point(alpha = 0.4, color = "darkblue") +
      geom_hline(yintercept = 0, color = "red", linetype = "dashed") +
      geom_smooth(method = "loess", se = FALSE, color = "orange", linewidth = 0.8) +
      theme_minimal() +
      labs(
        title = if(g == "F") paste("Residual Diagnostics:", event) else NULL,
        subtitle = if(g == "F") "Residuals vs. Fitted Values" else NULL,
        x = "Fitted College PB (Seconds)",
        y = "Residual (Seconds)"
      ) +
      annotate("text", x = Inf, y = Inf, label = g, hjust = 1.2, vjust = 1.5, 
               size = 6, fontface = "bold")
  }
  
  final <- plots[["F"]] + plots[["M"]]
  return(final)
}

run_logistic_regression <- function(data, event) {
  
  # Build one row per athlete: senior season best + did they go to college?
  logistic_data <- data |>
    filter(standard_event == event, grade == 12) |>
    group_by(athlete_id, gender) |>
    summarize(
      senior_best = min(mark_seconds, na.rm = TRUE),
      went_to_college = as.integer(any(went_to_college == TRUE)),
      .groups = "drop"
    )
  
  results_list <- list()
  
  for (g in c("F", "M")) {
    
    gdata <- logistic_data |> filter(gender == g)
    n <- nrow(gdata)
    n_college <- sum(gdata$went_to_college)
    
    cat("\n====================================================================\n")
    cat(sprintf("LOGISTIC REGRESSION: %s | Gender: %s | N = %d\n", event, g, n))
    cat(sprintf("  Went to college: %d (%.1f%%) | Did not: %d (%.1f%%)\n",
                n_college, 100*n_college/n, n - n_college, 100*(n - n_college)/n))
    cat("====================================================================\n")
    
    # --- Train/Test Split (80/20) ---
    set.seed(42)
    train_idx <- sample(1:n, size = floor(0.8 * n))
    train <- gdata[train_idx, ]
    test  <- gdata[-train_idx, ]
    
    # --- Fit the model ---
    # NOTE: Because faster = LOWER seconds, the coefficient will be NEGATIVE
    # (lower time → higher probability of college). This is correct.
    model <- glm(went_to_college ~ senior_best, data = train, family = binomial)
    
    s <- summary(model)
    print(s)
    
    # --- Predictions on test set ---
    test$pred_prob <- predict(model, test, type = "response")
    test$pred_class <- ifelse(test$pred_prob >= 0.5, 1, 0)
    
    # --- Confusion matrix ---
    conf <- table(Predicted = test$pred_class, Actual = test$went_to_college)
    accuracy <- sum(diag(conf)) / sum(conf)
    
    cat("\n--- CONFUSION MATRIX (Test Set) ---\n")
    print(conf)
    cat(sprintf("\nAccuracy: %.1f%%\n", 100 * accuracy))
    
    # --- AUC (manual calculation, no extra package needed) ---
    # Sort by predicted probability descending
    test_sorted <- test[order(-test$pred_prob), ]
    n_pos <- sum(test_sorted$went_to_college == 1)
    n_neg <- sum(test_sorted$went_to_college == 0)
    
    # Wilcoxon-Mann-Whitney AUC
    ranks <- rank(test_sorted$pred_prob)
    auc <- (sum(ranks[test_sorted$went_to_college == 1]) - n_pos*(n_pos+1)/2) / (n_pos * n_neg)
    
    cat(sprintf("AUC: %.4f\n", auc))
    
    # --- Odds ratio interpretation ---
    or_10sec <- exp(coef(model)["senior_best"] * 10)
    cat(sprintf("\nOdds Ratio (per 10-second increase in time): %.3f\n", or_10sec))
    cat(sprintf("Interpretation: Every 10 seconds SLOWER in senior year\n"))
    cat(sprintf("  multiplies the odds of competing in NCAA by %.3f\n", or_10sec))
    cat(sprintf("  (i.e., reduces odds by %.1f%%).\n", (1 - or_10sec) * 100))
    cat("====================================================================\n")
    
    results_list[[g]] <- list(
      model = model, train = train, test = test,
      accuracy = accuracy, auc = auc, full_data = gdata
    )
  }
  
  return(results_list)
}

plot_roc_curve <- function(logistic_results, event) {
  
  roc_data <- data.frame()
  
  for (g in c("F", "M")) {
    r <- logistic_results[[g]]
    test <- r$test
    
    # Calculate ROC points at many thresholds
    thresholds <- seq(0, 1, by = 0.01)
    
    for (thresh in thresholds) {
      pred <- ifelse(test$pred_prob >= thresh, 1, 0)
      tp <- sum(pred == 1 & test$went_to_college == 1)
      fp <- sum(pred == 1 & test$went_to_college == 0)
      fn <- sum(pred == 0 & test$went_to_college == 1)
      tn <- sum(pred == 0 & test$went_to_college == 0)
      
      tpr <- if((tp + fn) > 0) tp / (tp + fn) else 0
      fpr <- if((fp + tn) > 0) fp / (fp + tn) else 0
      
      roc_data <- rbind(roc_data, data.frame(
        gender = g, threshold = thresh, TPR = tpr, FPR = fpr
      ))
    }
  }
  
  ggplot(roc_data, aes(x = FPR, y = TPR, color = gender)) +
    geom_line(linewidth = 1.2) +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "gray50") +
    theme_minimal() +
    labs(
      title = paste("ROC Curve: Predicting NCAA Participation from Senior Year", event, "Time"),
      subtitle = paste0("AUC — F: ", round(logistic_results[["F"]]$auc, 3),
                        " | M: ", round(logistic_results[["M"]]$auc, 3)),
      x = "False Positive Rate",
      y = "True Positive Rate",
      color = "Gender"
    ) +
    scale_color_manual(values = c("F" = "#e74c3c", "M" = "#2c3e50")) +
    coord_equal()
}

plot_probability_curve <- function(logistic_results, event) {
  
  plots <- list()
  
  for (g in c("F", "M")) {
    r <- logistic_results[[g]]
    model <- r$model
    gdata <- r$full_data
    
    # Generate prediction curve across the full range of observed times
    time_range <- seq(min(gdata$senior_best), max(gdata$senior_best), length.out = 300)
    pred_df <- data.frame(senior_best = time_range)
    pred_df$prob <- predict(model, pred_df, type = "response")
    
    plots[[g]] <- ggplot() +
      # Show actual data points (jittered vertically for visibility)
      geom_jitter(data = gdata, aes(x = senior_best, y = went_to_college),
                  alpha = 0.15, height = 0.03, width = 0, color = "gray40", size = 0.8) +
      # Predicted probability curve
      geom_line(data = pred_df, aes(x = senior_best, y = prob),
                color = "#e74c3c", linewidth = 1.3) +
      # Mark the 50% threshold
      geom_hline(yintercept = 0.5, linetype = "dashed", color = "gray60") +
      theme_minimal() +
      labs(
        title = if(g == "F") paste("Probability of NCAA Participation by Senior", event, "Time") else NULL,
        x = "Grade 12 Season Best (Seconds)",
        y = "P(Competed in NCAA)"
      ) +
      scale_x_reverse() +  # Faster times (lower seconds) on right
      annotate("text", x = Inf, y = 0.95, label = g, hjust = -0.3, size = 6, fontface = "bold")
  }
  
  final <- plots[["F"]] + plots[["M"]]
  return(final)
}

print_summary_table <- function(lm_results, logistic_results, event) {
  
  cat("\n\n")
  cat("╔══════════════════════════════════════════════════════════════════════╗\n")
  cat(sprintf("║  MODEL SUMMARY TABLE: %s %s\n", event, paste(rep(" ", 42 - nchar(event)), collapse="")))
  cat("╠══════════════════════════════════════════════════════════════════════╣\n")
  cat("║  LINEAR REGRESSION: College PB ~ HS PB                             ║\n")
  cat("╠═══════════════════╦═══════════════════╦══════════════════════════════╣\n")
  cat("║ Metric            ║ Female            ║ Male                         ║\n")
  cat("╠═══════════════════╬═══════════════════╬══════════════════════════════╣\n")
  
  for (g in c("F", "M")) {
    r <- lm_results[[g]]
    s <- summary(r$model)
  }
  
  rf <- lm_results[["F"]]; rm <- lm_results[["M"]]
  sf <- summary(rf$model); sm <- summary(rm$model)
  
  cat(sprintf("║ N (train/test)    ║ %d / %d %s║ %d / %d %s║\n",
              nrow(rf$train), nrow(rf$test), paste(rep(" ", 11 - nchar(paste0(nrow(rf$train), " / ", nrow(rf$test)))), collapse=""),
              nrow(rm$train), nrow(rm$test), paste(rep(" ", 22 - nchar(paste0(nrow(rm$train), " / ", nrow(rm$test)))), collapse="")))
  cat(sprintf("║ Train R²          ║ %.4f            ║ %.4f                        ║\n", sf$r.squared, sm$r.squared))
  cat(sprintf("║ Test  R²          ║ %.4f            ║ %.4f                        ║\n", rf$test_r2, rm$test_r2))
  cat(sprintf("║ Train RMSE (sec)  ║ %.2f            ║ %.2f                        ║\n", rf$train_rmse, rm$train_rmse))
  cat(sprintf("║ Test  RMSE (sec)  ║ %.2f            ║ %.2f                        ║\n", rf$test_rmse, rm$test_rmse))
  cat(sprintf("║ Slope             ║ %.4f            ║ %.4f                        ║\n", coef(rf$model)[2], coef(rm$model)[2]))
  
  cat("╠═══════════════════════════════════════════════════════════════════════╣\n")
  cat("║  LOGISTIC REGRESSION: P(NCAA) ~ Senior Year Time                    ║\n")
  cat("╠═══════════════════╬═══════════════════╬══════════════════════════════╣\n")
  
  lf <- logistic_results[["F"]]; lm_r <- logistic_results[["M"]]
  
  cat(sprintf("║ N (train/test)    ║ %d / %d %s║ %d / %d %s║\n",
              nrow(lf$train), nrow(lf$test), paste(rep(" ", 11 - nchar(paste0(nrow(lf$train), " / ", nrow(lf$test)))), collapse=""),
              nrow(lm_r$train), nrow(lm_r$test), paste(rep(" ", 22 - nchar(paste0(nrow(lm_r$train), " / ", nrow(lm_r$test)))), collapse="")))
  cat(sprintf("║ Accuracy          ║ %.1f%%             ║ %.1f%%                        ║\n", 100*lf$accuracy, 100*lm_r$accuracy))
  cat(sprintf("║ AUC               ║ %.4f            ║ %.4f                        ║\n", lf$auc, lm_r$auc))
  
  cat("╚══════════════════════════════════════════════════════════════════════╝\n\n")
}

# 1500m (primary) and 800m (secondary)

events_to_model <- c("1500m", "800m")

for (event in events_to_model) {
  
  cat("\n\n###############################################################\n")
  cat(sprintf("###  FULL ANALYSIS: %s\n", event))
  cat("###############################################################\n")
  
  # --- Linear Regression ---
  lm_results <- run_linear_regression(clean_df, event)
  
  # Residual plot
  p_resid <- plot_residuals(lm_results, event)
  ggsave(filename = paste0("athlete_plots/", event, "_6_residuals.png"),
         plot = p_resid, width = 12, height = 5)
  
  # --- Logistic Regression ---
  logistic_results <- run_logistic_regression(clean_df, event)
  
  # ROC curve
  p_roc <- plot_roc_curve(logistic_results, event)
  ggsave(filename = paste0("athlete_plots/", event, "_7_roc.png"),
         plot = p_roc, width = 8, height = 7)
  
  # Probability curve
  p_prob <- plot_probability_curve(logistic_results, event)
  ggsave(filename = paste0("athlete_plots/", event, "_8_probability.png"),
         plot = p_prob, width = 12, height = 5)
  
  # --- Summary Table ---
  print_summary_table(lm_results, logistic_results, event)
}

cat("\n\nPhase 1 complete. New plots saved to athlete_plots/\n")
cat("New files:\n")
cat("  *_6_residuals.png  — Residual diagnostics for linear model\n")
cat("  *_7_roc.png        — ROC curve for logistic classifier\n")
cat("  *_8_probability.png — Predicted probability of NCAA participation\n")