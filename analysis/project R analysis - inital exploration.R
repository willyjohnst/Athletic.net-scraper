library(DBI)
library(RPostgres)
library(dplyr)
library(ggplot2)

con <- dbConnect(RPostgres::Postgres(), dbname = "athletic.net", host = "localhost", user = "postgres", password = "postgres")

# Pull all clean collegiate timelines and their races
query <- "
  SELECT 
    p.athlete_id, 
    a.gender,
    t.hs_graduated,
    t.hs_end_year,
    t.college_start_year,
    r.name_raw AS event_name, 
    p.mark_seconds, 
    m.date_start,
    EXTRACT(YEAR FROM m.date_start) AS race_year
  FROM performances p
  RIGHT JOIN athlete_timelines t ON p.athlete_id = t.athlete_id
  JOIN races r ON p.race_id = r.id
  JOIN meets m ON r.meet_id = m.id
  JOIN athletes a ON p.athlete_id = a.id
  WHERE p.mark_seconds IS NOT NULL
  AND t.hs_graduated = 'TRUE';
"
df <- dbGetQuery(con, query)

library(stringr)
library(tidyr)
library(patchwork)

# 1. Clean the data and calculate the relative "Grade"
prepare_athlete_data <- function(raw_df) {
  raw_df |>
    # A. Standardize Events (Your exact logic)
    mutate(raw_lower = tolower(event_name)) |>
    mutate(
      standard_event = case_when(
        str_detect(raw_lower, "100m|100 meters|100 dash") & !str_detect(raw_lower, "hurdle") ~ "100m",
        str_detect(raw_lower, "800m|800 meters|800 run") ~ "800m",
        str_detect(raw_lower, "1500m|1500 meters|1500 run") ~ "1500m",
        str_detect(raw_lower, "1600m|1600 meters|1600 run") ~ "1600m",
        str_detect(raw_lower, "1mile|1 mile|1609m") ~ "1 mile",
        str_detect(raw_lower, "5000m|5000 meters|5k") ~ "5000m",
        str_detect(raw_lower, "10000m|10000 meters|10k") ~ "10000m",
        # Add the rest of your case_when statements here...
        TRUE ~ "Other/Relay"
      )
    ) |>
    filter(standard_event != "Other/Relay") |>
    select(-raw_lower) |>
    
    # B. Calculate Grade and College Status dynamically
    mutate(
      # If they have a college start year, they went to college
      went_to_college = !is.na(college_start_year),
      
      # Calculate the grade mathematically based on the race year vs timeline
      grade = case_when(
        # High School: 12 - (Grad Year - Race Year)
        !is.na(hs_end_year) & race_year <= hs_end_year ~ 12 - (hs_end_year - race_year),
        
        # College: 21 + (Race Year - College Start Year)
        !is.na(college_start_year) & race_year >= college_start_year ~ 21 + (race_year - college_start_year),
        
        # Gap years or bad data
        TRUE ~ NA_real_
      ),
      
      # Assign a clean text label for plots
      Level = case_when(
        grade >= 9 & grade <= 12 ~ "High School",
        grade >= 21 & grade <= 25 ~ "College",
        TRUE ~ NA_character_
      )
    ) |>
    # Filter out weird gap year races (e.g., grade 13) or races without enough timeline info
    filter(!is.na(grade) & grade %in% c(9:12, 21:25))
}

current_year <- 2025
clean_df <- clean_df |>
  filter(hs_end_year <= (current_year - 1))  # Graduated 2024 or earlier

cat(sprintf("After removing current HS athletes: %d rows\n", nrow(clean_df)))

clean_df <- clean_df |>
  filter(
    case_when(
      # --- 800m ---
      # M HS record: ~1:46.5 (106.5s). Floor at 100s.
      # F HS record: ~2:00 (120s). Floor at 112s.
      # M Pro WR: ~1:40.9 (100.9s). College runners won't beat this.
      # F Pro WR: ~1:53.3 (113.3s).
      standard_event == "800m" & gender == "M" ~ mark_seconds >= 100 & mark_seconds <= 180,
      standard_event == "800m" & gender == "F" ~ mark_seconds >= 118 & mark_seconds <= 220,
      
      # --- 1500m (includes converted mile and 1600m) ---
      # M HS record (mile equiv): ~3:52 = 232s. As 1500m equiv: ~215s. Floor at 210s.
      # F HS record (mile equiv): ~4:28 = 268s. As 1500m equiv: ~248s. Floor at 240s.
      # M college: NCAA champ level ~3:34 = 214s. Sub-210 essentially impossible.
      # F college: NCAA champ level ~4:05 = 245s. Sub-240 essentially impossible.
      standard_event == "1500m" & gender == "M" ~ mark_seconds >= 210 & mark_seconds <= 420,
      standard_event == "1500m" & gender == "F" ~ mark_seconds >= 240 & mark_seconds <= 480,
      
      # --- 5000m ---
      standard_event == "5000m" & gender == "M" ~ mark_seconds >= 780 & mark_seconds <= 1500,
      standard_event == "5000m" & gender == "F" ~ mark_seconds >= 900 & mark_seconds <= 1800,
      
      # --- 10000m ---
      standard_event == "10000m" & gender == "M" ~ mark_seconds >= 1620 & mark_seconds <= 3600,
      standard_event == "10000m" & gender == "F" ~ mark_seconds >= 1860 & mark_seconds <= 4200,
      
      # Keep everything else as-is
      TRUE ~ TRUE
    )
  )

cat(sprintf("After floor/ceiling filters: %d rows\n", nrow(clean_df)))

cat("\n--- Post-filter summary for 1500m Males ---\n")
clean_df |>
  filter(standard_event == "1500m", gender == "M") |>
  summarize(
    n = n(),
    min_time = min(mark_seconds),
    q1 = quantile(mark_seconds, 0.25),
    median = median(mark_seconds),
    q3 = quantile(mark_seconds, 0.75),
    max_time = max(mark_seconds)
  ) |>
  print()

cat("\n--- Post-filter summary for 800m Females ---\n")
clean_df |>
  filter(standard_event == "800m", gender == "F") |>
  summarize(
    n = n(),
    min_time = min(mark_seconds),
    q1 = quantile(mark_seconds, 0.25),
    median = median(mark_seconds),
    q3 = quantile(mark_seconds, 0.75),
    max_time = max(mark_seconds)
  ) |>
  print()

cat("\n--- College linkage rates after cleaning ---\n")
clean_df |>
  filter(grade == 12) |>
  group_by(standard_event, gender) |>
  summarize(
    n_seniors = n_distinct(athlete_id),
    n_college = n_distinct(athlete_id[went_to_college == TRUE]),
    pct_college = round(100 * n_college / n_seniors, 1),
    .groups = "drop"
  ) |>
  print(n = 20)

cat("\nIf pct_college is between 15-40%, the went_to_college variable is plausible.\n")
cat("If it's under 10%, the false negative contamination is likely too severe.\n")
cat("If it's over 50%, you may have survivorship bias in who gets scraped.\n")

# Plot 1: Boxplots of all times by academic year/grade (CORRECTED)
plot_annual_boxplots <- function(data, event) {
  data |>
    filter(standard_event == event) |>
    ggplot(aes(x = factor(grade), y = mark_seconds, fill = Level)) +
    geom_boxplot(outlier.alpha = 0.4) +
    facet_wrap(~ gender) + 
    theme_minimal() +
    labs(
      title = paste("Distribution of", event, "Times by Grade"),
      x = "Grade Level (9-12 = HS, 21-25 = College)",
      y = "Time (Seconds)"
    ) +
    scale_y_reverse()
}

# Plot 2: Regression of all times by academic year/grade
plot_hs_vs_college_regression <- function(data, event) {
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
  
  ggplot(regression_data, aes(x = HS_PB, y = College_PB)) +
    geom_point(alpha = 0.5, color = "darkblue") +
    geom_smooth(method = "lm", color = "red", se = TRUE) +
    facet_wrap(~ gender) + # Will now work perfectly
    theme_minimal() +
    labs(
      title = paste("Predicting College PB from High School PB:", event),
      subtitle = paste("Sample Size:", nrow(regression_data), "Athletes"),
      x = "High School Personal Best (Seconds)",
      y = "College Personal Best (Seconds)"
    ) +
    coord_fixed(ratio = 1) + 
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "gray")
}

# Plot 3: Survivorship of 12th grade times
plot_survivorship_density <- function(data, event) {
  
  data |>
    filter(standard_event == event, grade == 12) |>
    group_by(athlete_id, gender, went_to_college) |>
    summarize(senior_best = min(mark_seconds, na.rm = TRUE), .groups = "drop") |>
    mutate(
      college_status = if_else(went_to_college == TRUE, "Competed in NCAA", "Did Not Compete")
    ) |>
    ggplot(aes(x = senior_best, fill = college_status)) +
    geom_density(alpha = 0.6, trim = TRUE) + 
    facet_wrap(~ gender) + 
    theme_minimal() +
    scale_fill_manual(
      values = c("Competed in NCAA" = "#2ca02c", "Did Not Compete" = "#d62728")
    ) +
    labs(
      title = paste("Grade 12", event, "Times: The Collegiate Filter"),
      subtitle = "Comparing senior year times of future collegiate runners vs. those who stopped",
      x = "Grade 12 Season Best (Seconds)",
      y = "Density",
      fill = "Collegiate Career"
    ) +
    scale_x_reverse()
}

# 4. Progression Trajectories 
plot_progression_trajectories <- function(data, event, sample_size = 200) {
  progression_data <- data |>
    filter(standard_event == event) |>
    group_by(athlete_id, grade, gender, went_to_college) |>
    summarize(season_best = min(mark_seconds, na.rm = TRUE), .groups = "drop") |>
    # Group by athlete again to count their total seasons
    group_by(athlete_id) |>
    # Drop athletes who only have 1 data point (prevents the geom_smooth warning)
    filter(n() > 1) |>
    ungroup()
  
  college_athletes <- unique(progression_data$athlete_id[progression_data$went_to_college == TRUE])
  actual_sample_size <- min(sample_size, length(college_athletes))
  sampled_athletes <- sample(college_athletes, actual_sample_size)
  
  progression_data |>
    filter(athlete_id %in% sampled_athletes) |>
    ggplot(aes(x = grade, y = season_best, group = athlete_id)) +
    geom_line(alpha = 0.15, color = "blue") +
    geom_smooth(aes(group = 1), method = "loess", color = "black", linewidth = 1.5, se = FALSE) +
    geom_vline(xintercept = 12.5, linetype = "dashed", color = "red") +
    facet_wrap(~ gender) +
    scale_x_continuous(breaks = c(9, 10, 11, 12, 21, 22, 23, 24, 25)) +
    theme_minimal() +
    labs(
      title = paste("Individual Progression Trajectories:", event),
      subtitle = paste("Sample of", actual_sample_size, "NCAA athletes"),
      x = "Grade Level (9-12 = HS, 21-25 = College)",
      y = "Season Best Time (Seconds)"
    ) +
    scale_y_reverse()
}

plot_presentation_quartiles <- function(data, event, y_bounds_m, y_bounds_f) {
  
  # 1. Wrangle the data
  quartile_data <- data |>
    filter(standard_event == event) |>
    group_by(athlete_id, gender) |>
    summarize(
      pb_9 = if(any(grade == 9, na.rm = TRUE)) min(mark_seconds[grade == 9], na.rm = TRUE) else NA_real_,
      pb_10 = if(any(grade == 10, na.rm = TRUE)) min(mark_seconds[grade == 10], na.rm = TRUE) else NA_real_,
      pb_11 = if(any(grade == 11, na.rm = TRUE)) min(mark_seconds[grade == 11], na.rm = TRUE) else NA_real_,
      pb_12 = if(any(grade == 12, na.rm = TRUE)) min(mark_seconds[grade == 12], na.rm = TRUE) else NA_real_,
      
      college_pb = if(any(grade >= 21 & grade <= 25, na.rm = TRUE)) 
        min(mark_seconds[grade >= 21 & grade <= 25], na.rm = TRUE) else NA_real_,
      went_to_college = any(grade >= 21 & grade <= 25, na.rm = TRUE),
      .groups = "drop"
    ) |>
    pivot_longer(cols = starts_with("pb_"), names_to = "hs_grade", names_prefix = "pb_", values_to = "hs_season_best") |>
    filter(!is.na(hs_season_best)) |>
    mutate(
      hs_grade = as.numeric(hs_grade),
      # FIX 1: Lock the chronological ordering of the facets
      Grade_Label = factor(paste("Grade", hs_grade), levels = c("Grade 9", "Grade 10", "Grade 11", "Grade 12"))
    )
  
  # 2. Calculate Quartiles and Percentages
  plot_data <- quartile_data |>
    group_by(gender, hs_grade) |>
    mutate(quartile = ntile(hs_season_best, 4)) |>
    group_by(gender, hs_grade, quartile) |>
    mutate(
      total_in_quartile = n(),
      ncaa_pct = round((sum(went_to_college) / total_in_quartile) * 100, 1),
      q_name = paste0("Q", quartile),
      pct_label = paste0(ncaa_pct, "%") # Standalone label for floating text
    ) |>
    ungroup() |>
    filter(went_to_college == TRUE & !is.na(college_pb))
  
  # 3. Base Plotting Function
  build_gender_plot <- function(gender_filter, y_limits, title_text, hide_x = FALSE) {
    p <- plot_data |> 
      filter(gender == gender_filter) |>
      ggplot(aes(x = q_name, y = college_pb, fill = as.factor(quartile))) +
      geom_boxplot(outlier.alpha = 0.4) +
      facet_grid(~ Grade_Label) +
      scale_fill_brewer(palette = "Blues", direction = -1) +
      theme_bw() +
      # FIX 2: Exact axis bounds using scale_y_reverse (max first, min second)
      scale_y_reverse(limits = c(y_limits[2], y_limits[1])) +
      theme(
        legend.position = "none",
        axis.title.x = if(hide_x) element_blank() else element_text(),
        axis.text.x = if(hide_x) element_blank() else element_text(),
        plot.title = element_text(face = "bold")
      ) +
      labs(title = title_text, y = paste(gender_filter, "College PB"))
    
    return(p)
  }
  
  # 4. Build Male and Female plots separately with exact bounds
  p_f <- build_gender_plot("F", y_bounds_f, paste("Does early dominance predict a higher collegiate ceiling?:", event), hide_x = TRUE)
  p_m <- build_gender_plot("M", y_bounds_m, NULL, hide_x = FALSE) + labs(x = "High School Speed Quartile")
  
  # 5. Stack them together using patchwork
  final_plot <- p_f / p_m 
  return(final_plot)
}

library(dplyr)
library(broom) # Helps clean up statistical outputs if you want to save them

run_quartile_anova <- function(data, event, target_grade = 12, target_gender = "M") {
  
  # 1. Wrangle the data for the specific grade and gender
  anova_data <- data |>
    filter(standard_event == event, gender == target_gender) |>
    group_by(athlete_id) |>
    summarize(
      # Get their HS season best for the target grade
      hs_season_best = if(any(grade == target_grade, na.rm = TRUE)) 
        min(mark_seconds[grade == target_grade], na.rm = TRUE) 
      else NA_real_,
      
      # Get their overall College PB
      college_pb = if(any(grade >= 21 & grade <= 25, na.rm = TRUE)) 
        min(mark_seconds[grade >= 21 & grade <= 25], na.rm = TRUE) 
      else NA_real_,
      .groups = "drop"
    ) |>
    # Keep only athletes who have BOTH a time in that HS grade and went to college
    filter(!is.na(hs_season_best) & !is.na(college_pb)) |>
    
    # 2. Assign them to Quartiles
    mutate(
      quartile = as.factor(ntile(hs_season_best, 4)) # Q1 = Fastest, Q4 = Slowest
    )
  
  # Check if we have enough data to run the test
  if(nrow(anova_data) < 20) {
    cat("Not enough data for", event, target_gender, "Grade", target_grade, "\n")
    return(NULL)
  }
  
  # 3. Run the ANOVA Model
  model <- aov(college_pb ~ quartile, data = anova_data)
  
  # 4. Run the Tukey Post-Hoc Test
  tukey_results <- TukeyHSD(model)
  
  # --- PRINT CLEAN RESULTS TO THE CONSOLE ---
  cat("\n====================================================\n")
  cat(sprintf("STATISTICAL REPORT: %s | Grade %d | Gender: %s\n", event, target_grade, target_gender))
  cat(sprintf("Sample Size: %d NCAA Athletes\n", nrow(anova_data)))
  cat("====================================================\n")
  
  # Get the overall ANOVA p-value
  overall_p <- summary(model)[[1]][["Pr(>F)"]][1]
  
  if(overall_p < 0.05) {
    cat("OVERALL ANOVA: SIGNIFICANT (p =", format.pval(overall_p, eps = 0.001), ")\n")
    cat("Conclusion: High school speed quartile heavily influences college outcomes.\n\n")
  } else {
    cat("OVERALL ANOVA: NOT SIGNIFICANT (p =", format.pval(overall_p, eps = 0.001), ")\n")
    cat("Conclusion: High school speed did not statistically differentiate college outcomes here.\n\n")
  }
  
  cat("--- HEAD-TO-HEAD MATCHUPS (Tukey HSD) ---\n")
  # Convert the Tukey results to a readable dataframe
  tukey_df <- as.data.frame(tukey_results$quartile)
  tukey_df$comparison <- rownames(tukey_df)
  
  # We care most about how Q1 compares to the others (2-1, 3-1, 4-1)
  for(i in 1:nrow(tukey_df)) {
    comp <- tukey_df$comparison[i]
    p_adj <- tukey_df$`p adj`[i]
    diff_sec <- tukey_df$diff[i]
    
    # Simple logic to explain it in plain English
    significance <- if(p_adj < 0.05) "STATISTICALLY DIFFERENT" else "NO DIFFERENCE"
    
    # Note: Positive difference means the first group in the comparison (e.g., Q2 in "2-1") was SLOWER (higher seconds)
    cat(sprintf("Group %s : %s (p = %s)\n", 
                comp, 
                significance, 
                format.pval(p_adj, eps = 0.001)))
    
    if (p_adj < 0.05) {
      cat(sprintf("   -> Difference: %.2f seconds\n", abs(diff_sec)))
    }
  }
  cat("====================================================\n\n")
  
  return(tukey_results)
}

results <- run_quartile_anova(clean_df, event = "1500m", target_grade = 12, target_gender = "M")

# Create a folder to save the plots if it doesn't exist
if(!dir.exists("athlete_plots")) {
  dir.create("athlete_plots")
}

# Define the events you want to analyze
events_to_analyze <- c("800m", "1500m")

# Loop through each event and generate/save the plots
for (event in events_to_analyze) {
  
  cat("Processing visualizations for", event, "...\n")
  
  # 1. Boxplots
  p1 <- plot_annual_boxplots(clean_df, event)
  ggsave(filename = paste0("athlete_plots/", event, "_1_boxplots.png"), 
         plot = p1, width = 8, height = 6)
  
  # 2. Regression
  # (Wrapped in tryCatch in case there isn't enough overlapping data for a specific event)
  tryCatch({
    p2 <- plot_hs_vs_college_regression(clean_df, event)
    ggsave(filename = paste0("athlete_plots/", event, "_2_regression.png"), 
           plot = p2, width = 8, height = 8)
  }, error = function(e) { cat("  -> Not enough data for regression on", event, "\n") })
  
  # 3. Survivorship Density
  p3 <- plot_survivorship_density(clean_df, event)
  ggsave(filename = paste0("athlete_plots/", event, "_3_survivorship.png"), 
         plot = p3, width = 8, height = 6)
  
  # 4. Trajectories
  tryCatch({
    p4 <- plot_progression_trajectories(clean_df, event, sample_size = 4000)
    ggsave(filename = paste0("athlete_plots/", event, "_4_trajectories.png"), 
           plot = p4, width = 10, height = 6)
  }, error = function(e) { cat("  -> Not enough data for trajectories on", event, "\n") })
  
  tryCatch({
    # Set standard bounds dynamically based on the event
    if (event == "1500m") {
      bounds_m <- c(200, 325)
      bounds_f <- c(275, 375)
    } else if (event == "800m") {
      bounds_m <- c(100, 150)
      bounds_f <- c(120, 180)
    } else {
      # Fallback for other events
      bounds_m <- c(NA, NA) 
      bounds_f <- c(NA, NA)
    }
    
    p5 <- plot_presentation_quartiles(clean_df, event, bounds_m, bounds_f)
    ggsave(filename = paste0("athlete_plots/", event, "_5_quartiles.png"), 
           plot = p5, width = 12, height = 8)
  }, error = function(e) { cat("  -> Error generating quartiles on", event, "\n") })
}

cat("All plots generated successfully!\n")