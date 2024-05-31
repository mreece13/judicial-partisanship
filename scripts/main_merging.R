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

opinions <- open_dataset("data/merged/") |>
  filter(judges != "") |>
  mutate(across(c(name_last, name_first, name_middle), ~ ifelse(is.na(.x), "", .x))) |>
  mutate(
    author = str_c(name_last, ", ", name_first, " ", name_middle, sep = "")
    ) |>
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
  ) |> 
  separate_longer_delim(cols = plain_text, delim = "\n") |> 
  mutate(
    plain_text = str_remove_all(plain_text, "<[^>]*>|https?://\\S+|www\\.\\S+|http?://\\S+|[[:punct:]]|\\d+"),
    plain_text = str_replace_all(plain_text, " +", " "),
    plain_text = str_to_lower(plain_text),
    plain_text = str_squish(plain_text)
  ) |> 
  filter(plain_text != "", plain_text != " ", !is.na(plain_text))

write_csv(opinions, "data/opinions_cleaned.csv")

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

#####

elections = readxl::read_excel("data/kritzer results/ElectionDataset.xlsx") |> 
  select(state, year, CourtType, electtype, TotalVoteCount, CandName0:Party24) |> 
  mutate(
    CandName1 = coalesce(CandName0, CandName1),
    CandType1 = coalesce(CandType0, CandType1),
    IncumType1 = coalesce(IncumType0, IncumType1),
    YrAppointed1 = coalesce(YrAppointed0, YrAppointed1)
  ) |> 
  select(-CandName0, -CandType0, -IncumType0, -YrAppointed0)

make_wide <- function(col){
  elections |> 
    select(starts_with(col)) |> 
    pivot_longer(cols = starts_with(col), values_to = col) |> 
    select(-name)
}

cols = c("CandName", "votes", "CandType", "IncumType", "YrAppointed", "Party")

long = elections |> 
  select(state:TotalVoteCount, starts_with("CandName")) |> 
  pivot_longer(starts_with("CandName")) |> 
  select(-name, -value) |> 
  bind_cols(
    map(cols, make_wide) |> list_cbind()
  ) |> 
  drop_na(CandName)


d = read_csv("data/opinions_cleaned.csv")

d |> 
  select(plain_text) |> 
  reframe(l = map_int(plain_text, str_length)) |> 
  pull(l) |> 
  summary(na.rm = TRUE)

judges = read_csv("data/judges.csv")

d2 = d |> 
  inner_join(judges, join_by(state, name_last, name_first)) |> 
  mutate(name_last = coalesce(new_last, name_last)) |> 
  select(-new_last) |> 
  mutate(party = replace_na(party, "-1"))

d2 |> write_csv("data/opinions_semisupervised.csv")

d = read_csv("data/opinions_semisupervised.csv")

d |> 
  mutate(party = ifelse(party == "-1", NA, "yes")) |> 
  count(party) |> 
  mutate(perc = n/sum(n))
