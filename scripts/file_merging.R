rm(list = ls())
gc()

library(tidyverse)
library(arrow)
library(data.table)

# courts <- read_csv("data/courts-2024-03-11.csv") |>
#   filter(jurisdiction == "S", in_use, has_opinion_scraper, str_detect(full_name, "Supreme")) |>
#   select(id)
#
# dockets <- open_csv_dataset("data/dockets-2024-03-11.csv") |>
#   select(id, date_modified, case_name, docket_number, court_id, assigned_to_id, cause, jurisdiction_type) |>
#   inner_join(courts, by = c("court_id" = "id")) |>
#   rename(date_modified_docket = date_modified)
#
# clusters <- open_csv_dataset("data/opinion-clusters-2024-03-11.csv",
#   parse_options = csv_parse_options(newlines_in_values = TRUE)
# ) |>
#   select(
#     id, judges, date_modified, case_name, attorneys, nature_of_suit, posture, syllabus,
#     citation_count, precedential_status, docket_id, headnotes, history, other_dates, summary
#   ) |>
#   inner_join(dockets, by = c("docket_id" = "id")) |>
#   rename(date_modified_clusters = date_modified)
#
# open_csv_dataset("data/opinions-2024-03-11.csv",
#   parse_options = csv_parse_options(newlines_in_values = TRUE)
# ) |>
#   select(id, date_modified, type, plain_text, author_id, cluster_id, page_count, author_str) |>
#   inner_join(clusters, by = c("cluster_id" = "id")) |>
#   rename(date_modified_opinion = date_modified) |>
#   write_dataset("data/merged/", format = "parquet")

read_csv_arrow("data/opinions-2024-03-11.csv", 
               read_options = list(block_size = 41943040L),
               parse_options = list(newlines_in_values = TRUE)) |> 
  select(id, date_modified, type, plain_text, author_id, cluster_id, page_count, author_str) |> 
  write_csv_arrow("data/opinion-clusters-2024-03-11-filtered.csv")

# courts <- fread("data/courts-2024-03-11.csv")[
#   jurisdiction == "S" &
#     in_use == "t" &
#     has_opinion_scraper == "t" &
#     str_detect(full_name, "Supreme"),
#   id
# ]
# 
# message("Loading Dockets")
# 
# dockets <- fread("data/dockets-2024-03-11.csv",
#   index = c("id", "court_id"),
#   showProgress = TRUE,
#   select = c(
#     "id", "case_name", "docket_number",
#     "court_id", "assigned_to_id", "cause", "jurisdiction_type"
#   )
# )[
#   court_id %in% courts
# ]
# 
# message("Loading Clusters")
# 
# clusters <- fread("data/opinion-clusters-2024-03-11.csv",
#   index = c("id", "docket_id"),
#   showProgress = TRUE,
#   select = c(
#     "id", "judges", "case_name", "attorneys",
#     "nature_of_suit", "posture", "syllabus", "citation_count",
#     "precedential_status", "docket_id", "headnotes", "history",
#     "other_dates", "summary"
#   )
# )[
#   dockets,
#   on = c(docket_id = "id"), nomatch = NULL
# ]
# 
# message("Loading Opinions")
# 
# opinions <- fread("data/opinions-2024-03-11.csv",
#   index = "cluster_id",
#   showProgress = TRUE,
#   select = c(
#     "id", "date_modified", "type", "plain_text", "author_id",
#     "cluster_id", "page_count", "author_str"
#   )
# )[
#   clusters,
#   on = c(cluster_id = "id"), nomatch = NULL
# ]
# 
# message("Writing Opinions")
# 
# write_parquet(opinions, "data/merged.parquet")
