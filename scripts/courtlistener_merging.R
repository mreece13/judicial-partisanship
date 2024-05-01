rm(list = ls())
gc()

library(tidyverse)
library(arrow)
library(data.table)
library(Matrix)

generate_chunks <- FALSE
cached <- TRUE

if (generate_chunks) {
  f <- function(x, pos) write_csv(x, str_c("data/opinion_chunk_", pos, ".csv"))
  
  read_csv_chunked("data/opinions-2024-03-11.csv",
                   SideEffectChunkCallback$new(f),
                   col_types = cols_only(
                     "id" = "i",
                     "date_modified" = "T",
                     "type" = "c",
                     "plain_text" = "c",
                     "author_id" = "i",
                     "cluster_id" = "i"
                   ),
                   chunk_size = 1000000
  )
}

if (cached) {
  
  clusters <- read_parquet("data/opinion-clusters.parquet") |> mutate(id = as.character(id))
  setDT(clusters)
  
} else {
  courts <- fread("data/courts-2024-03-11.csv")[
    jurisdiction == "S" &
      in_use == "t" &
      has_opinion_scraper == "t" &
      str_detect(full_name, "Supreme"),
    id
  ]
  
  message("Loading Dockets")
  
  dockets <- fread("data/dockets-2024-03-11.csv",
                   index = c("id", "court_id"),
                   showProgress = TRUE,
                   select = c(
                     "id", "case_name", "docket_number",
                     "court_id"
                   )
  )[
    court_id %in% courts
  ]
  
  write_parquet(dockets, "data/dockets.parquet")
  
  message("Loading Clusters")
  
  clusters <- fread("data/opinion-clusters-2024-03-11.csv",
                    index = c("id", "docket_id"),
                    showProgress = TRUE,
                    select = c(
                      "id", "judges", "date_filed",
                      "nature_of_suit", "syllabus", "citation_count",
                      "precedential_status", "docket_id"
                    )
  )[
    dockets,
    on = c(docket_id = "id"), nomatch = NULL
  ]
  
  write_parquet(clusters, "data/opinion-clusters.parquet")
}

message("Loading People")

people <- fread(
  file = "data/people-db-people-2024-03-11.csv",
  index = "id",
  select = c("id", "slug", "name_first", "name_middle", "name_last", "name_suffix", "gender"),
  colClasses = list(character = 1:7)
)

message("Loading Opinions")

opinions <- fread(
  file = "data/opinions.csv",
  index = "cluster_id",
  showProgress = TRUE,
  fill = TRUE,
  colClasses = list(character = 1:6),
  col.names = c("id", "date_modified", "type", "plain_text", "author_id", "cluster_id")
)[
  !is.na(plain_text)
][
  clusters,
  on = c(cluster_id = "id"), nomatch = NULL
][
  people,
  on = c(author_id = "id")
][
  judges != "" & !is.na(judges),
][
  ,
  plain_text := str_remove_all(plain_text, "\\\\n|<[^>]+>") |> str_squish()
][
  ,
  id := as.integer(id)
]

citations <- fread("data/citation-map-2024-03-11.csv", drop = "id")

cite_lookup <- tibble(
  old_id = unique(c(
    unique(citations$citing_opinion_id), 
    unique(citations$cited_opinion_id),
    unique(opinions$id))),
  new_id = 1:length(old_id)
) |> 
  mutate(old_id = as.integer(old_id))
setDT(cite_lookup)

citations = citations[
  cite_lookup,
  on = c(citing_opinion_id = "old_id"), nomatch = NULL
][
  cite_lookup,
  on = c(cited_opinion_id = "old_id"), nomatch = NULL
][
  is.na(depth),
  depth := 0
]
setnames(citations, c("new_id", "i.new_id"), c("citing_opinion_newid", "cited_opinion_newid"))

cites_matrix <- sparseMatrix(citations$citing_opinion_newid, citations$cited_opinion_newid, x = citations$depth)

merged <- opinions[
  cite_lookup,
  on = c(id = "old_id"), nomatch = NULL
][
  ,
  cites := list(as.list(cites_matrix[new_id]))
]

message("Writing Opinions")

write_dataset(merged, "data/merged/", format = "parquet", partitioning = "court_id")

sessionInfo()