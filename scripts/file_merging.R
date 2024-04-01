rm(list=ls())
gc()

library(tidyverse)
library(arrow)

courts <- read_csv("data/courts-2024-03-11.csv") |> 
  filter(jurisdiction == "S", in_use, has_opinion_scraper, str_detect(full_name, "Supreme")) |> 
  select(id)

dockets <- open_csv_dataset("data/dockets-2024-03-11.csv",
                            schema = schema(
                              id = int32(),
                              date_modified = timestamp(),
                              case_name = string(),
                              docket_number = string(),
                              court_id = string(),
                              assigned_to_id = int32(),
                              cause = string(),
                              jurisdiction_type = string()
                            )) |> 
  inner_join(courts, by = c("court_id" = "id")) |> 
  rename(date_modified_docket = date_modified)

clusters <- open_csv_dataset("data/opinion-clusters-2024-03-11.csv",
                             schema = schema(
                               id = int32(),
                               judges = string(),
                               date_modified = timestamp(),
                               case_name = string(),
                               attorneys = string(),
                               nature_of_suit = string(),
                               posture = string(),
                               syllabus = string(),
                               citation_count = int32(),
                               precedential_status = string(),
                               docket_id = int32(),
                               headnotes = string(),
                               history = string(),
                               other_dates = string(),
                               summary = string()
                             )) |> 
  inner_join(dockets, by = c("docket_id" = "id")) |> 
  rename(date_modified_clusters = date_modified)


open_csv_dataset("data/opinions-2024-03-11.csv",
                             schema = schema(
                               id = int32(),
                               date_modified = timestamp(),
                               type = string(),
                               plain_text = string(),
                               author_id = int32(),
                               cluster_id = int32(),
                               page_count = int32(),
                               author_str = string()
                              )) |> 
  inner_join(clusters, by = c("cluster_id" = "id")) |> 
  rename(date_modified_opinion = date_modified) |> 
  write_dataset("data/merged/", format = "parquet")
