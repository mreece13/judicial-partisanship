rm(list = ls())
gc()

library(tidyverse)
library(arrow)
library(data.table)
library(Matrix)

# clean votes some more
read_parquet("data/hall and windett/main.parquet", col_select = -contains("Code")) |>
  mutate(
    across(Majority_Opinion_Author:J11_Name, ~ str_squish(str_to_upper(as.character(.x))))
  ) |>
  rename(
    majority_author = Majority_Opinion_Author,
    dissent_author = Dissent_Author
  ) |>
  mutate(
    majority_coauthors = str_c(
      Majority_Author_2, Majority_Author_3,
      Majority_Author_4, Majority_Author_5,
      Majority_Author_6, Majority_Author_7
    ),
    dissent_coauthors = str_c(
      Dissent_Author_2, Dissent_Author_3,
      Dissent_Author_4, Dissent_Author_5,
      Dissent_Author_6, Dissent_Author_7
    )
  ) |>
  select(-Majority_Author_2:-Majority_Author_7, -Dissent_Author_2:-Dissent_Author_7) |>
  mutate(
    across(contains("Vote"), as.character),
    across(starts_with("J"), ~ replace_na(.x, ""))
  ) |>
  mutate(
    J1 = str_c(J1_Vote, J1_Name, sep = "||"),
    J2 = str_c(J2_Vote, J2_Name, sep = "||"),
    J3 = str_c(J3_Vote, J3_Name, sep = "||"),
    J4 = str_c(J4_Vote, J4_Name, sep = "||"),
    J5 = str_c(J5_Vote, J5_Name, sep = "||"),
    J6 = str_c(J6_Vote, J6_Name, sep = "||"),
    J7 = str_c(J7_Vote, J7_Name, sep = "||"),
    J8 = str_c(J8_Vote, J8_Name, sep = "||"),
    J9 = str_c(J9_Vote, J9_Name, sep = "||"),
    J10 = str_c(J10_Vote, J10_Name, sep = "||"),
    J11 = str_c(J11_Vote, J11_Name, sep = "||")
  ) |>
  select(-J1_Vote:-J11_Name) |>
  pivot_longer(cols = starts_with("J")) |>
  separate_wider_delim(cols = value, delim = "||", names = c("vote", "judge")) |>
  filter(judge != "") |>
  mutate(
    authored_majority = judge == majority_author,
    coauthored_majority = str_detect(majority_coauthors, judge),
    authored_dissent = judge == dissent_author,
    coauthored_dissent = str_detect(dissent_coauthors, judge),
  ) |>
  mutate(across(where(is.character), ~ na_if(.x, ""))) |>
  select(-name) |>
  write_parquet("data/hall and windett/cleaned.parquet")

windett_ideals <- read_tsv("data/windett et al/windett_et_al_judicial_scores.tab",
  col_names = c(
    "id", "name", "state", "fips", "stateab",
    "year", "cfscore", "unscaledideal", "scaledideal"
  )
)

opinions <- open_dataset("~/Dropbox (MIT)/Research/judicial-partisanship/data/merged/") |>
  filter(year(date_filed) > min(windett_ideals$year, na.rm = TRUE), judges != "") |>
  mutate(across(c(name_last, name_first, name_middle), ~ ifelse(is.na(.x), "", .x))) |>
  mutate(author = str_c(name_last, ", ", name_first, " ", name_middle, sep = "")) |>
  select(
    id, new_id, court_id, case_name, date_filed, judges, author,
    gender, cites, plain_text, name_last, name_middle, name_first
  ) |>
  rename(state = court_id) |> 
  collect() |>
  as_tibble() |>
  mutate(
    author = str_squish(author),
    state = case_match(
      state,
      .default = str_to_upper(state),
      "ala" ~ "AL",
      "cal" ~ "CA",
      "colo" ~ "CO",
      "ark" ~ "AR",
      "ariz" ~ "AZ",
      "alaska" ~ "AK",
      "fla" ~ "FL",
      "conn" ~ "CN",
      "haw" ~ "HI",
      "del" ~ "DE",
      "idaho" ~ "ID",
      "ill" ~ "IL",
      "kan" ~ "KA",
      "iowa" ~ "IA",
      "ind" ~ "IN",
      "mass" ~ "MA",
      "minn" ~ "MN",
      "mich" ~ "MI",
      "nev" ~ "NV",
      "ohio" ~ "OH",
      "tex" ~ "TX",
      "neb" ~ "NE",
      "utah" ~ "UT",
      "tenn" ~ "TN",
      "wash" ~ "WA",
      "wis" ~ "WI",
      "wva" ~ "WV",
      "wyo" ~ "WY",
      "mont" ~ "MO"
    ),
    name_last = case_when(
      name_last == "" ~ str_extract(judges, "^([^,]*)(?:,|$)") |> str_remove(","),
      .default = name_last
    )
  )

write_csv(opinions, "data/opinions.csv")

dime <- fread("data/dime_raw/dime_recipients_1979_2022.csv",
  drop = c(
    "title", "suffix", "distcyc", "dwdime", "dwnom1", "dwnom2",
    "ps.dwnom1", "irt.cfscore", "ind.exp.support", "ind.exp.oppose",
    "ICPSR", "ICPSR2", "Cand.ID", "FEC.ID", "NID", "before.switch.ICPSR",
    "after.switch.ICPSR"
  )
)[seat == "state:judicial" & str_detect(nimsp.office, "supreme")]

library(fastLink)

setnames(dime, c("lname", "fname", "mname", "cand.gender"), c("name_last", "name_first", "name_middle", "gender"))

opinion_names <- distinct(opinions, name_last, name_first, name_middle, state) |>
  mutate(across(everything(), str_to_upper)) |>
  mutate(id_opin = 1:n())

dime_names <- distinct(dime, name_last, name_first, name_middle, state) |>
  mutate(across(everything(), str_to_upper)) |>
  mutate(id_dime = 1:n())

out <- fastLink(
  opinion_names, dime_names,
  varnames = c("name_last", "name_first", "name_middle"),
  stringdist.match = c("name_last", "name_first", "name_middle")
)

matches <- getMatches(
  opinion_names, dime_names,
  fl.out = out
) |>
  select(-gamma.1:-posterior) |>
  rename(id_dime = "dfB.match[, names.dfB]")

dime_names[
  ,
  .(id_dime)
][
  matches,
  on = c("id_dime")
]
