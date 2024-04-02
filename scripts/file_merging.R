rm(list = ls())
gc()

library(tidyverse)
library(arrow)
library(data.table)

# f <- function(x, pos) write_csv(x, str_c("data/opinion_chunk_", pos, ".csv"))
# f <- function(x, pos) x

# read_csv_chunked("data/opinions-2024-03-11.csv",
#                  DataFrameCallback$new(f),
#                  col_types = cols_only(
#                    "id" = "i",
#                    "date_modified" = "T",
#                    "type" = "c",
#                    "plain_text" = "c",
#                    "author_id" = "i",
#                    "cluster_id" = "i"
#                  ),
#                  chunk_size = 1000000)

# courts <- fread("data/courts-2024-03-11.csv")[
#   jurisdiction == "S" &
#     in_use == "t" &
#     has_opinion_scraper == "t" &
#     str_detect(full_name, "Supreme"),
#   id
# ]

# message("Loading Dockets")
# 
# dockets <- read_parquet("data/dockets.parquet")
# setDT(dockets)
# dockets[, id := as.integer(id)]

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
# write_parquet(dockets, "data/dockets.parquet")

message("Loading Clusters")

clusters <- read_parquet("data/opinion-clusters.parquet")
setDT(clusters)

# clusters <- fread("data/opinion-clusters-2024-03-11.csv",
#   index = c("id", "docket_id"),
#   showProgress = TRUE,
#   select = c(
#     "id", "judges", "case_name", "attorneys", "date_filed",
#     "nature_of_suit", "posture", "syllabus", "citation_count",
#     "precedential_status", "docket_id", "headnotes", "history",
#     "other_dates", "summary"
#   )
# )[
#   dockets,
#   on = c(docket_id = "id"), nomatch = NULL
# ]

clusters[, id := as.integer(id)]

# write_parquet(clusters, "data/opinion-clusters.parquet")

message("Loading Opinions")

opinions <- fread(
  path = "data/opinions.csv",
  index = "cluster_id",
  showProgress = TRUE,
  fill = TRUE,
  col.names = c("id", "date_modified", "type", "plain_text", "author_id", "cluster_id")
)[
  clusters,
  on = c(cluster_id = "id"), nomatch = NULL
]

message("Writing Opinions")

write_parquet(opinions, "data/merged.parquet")
