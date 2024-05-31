rm(list=ls())
gc()

library(tidyverse)
library(stm)
library(data.table)
library(quanteda)

opinions = fread("data/opinions_merged.csv", select = c("id", "state", "annotated_text"))

dfm = opinions |> 
  corpus(text_field = "annotated_text") |> 
  tokens(remove_punct = TRUE, remove_numbers = TRUE, remove_url = TRUE, remove_separators = TRUE, remove_symbols = TRUE) |> 
  tokens_remove(pattern = stopwords()) |> 
  tokens_wordstem() |> 
  dfm()

drops = featnames(dfm) |> str_subset("^.$|\\d+")
dfm_cleaned = dfm_remove(dfm, drops)

model = stm(
  dfm_cleaned,
  prevalence = ~ state,
  init.type = "Spectral",
  K = 0, seed = 17806,
  verbose = TRUE
)

labelTopics(model)$prob


opinions = fread("data/opinions_merged.csv")

opinions[]

distinct(opinions, state, author, gender) |>
  filter(author != ",") |> 
  mutate(
    election_type = NA,
    election_date = NA
  ) |> 
  write_csv("data/judges.csv")
