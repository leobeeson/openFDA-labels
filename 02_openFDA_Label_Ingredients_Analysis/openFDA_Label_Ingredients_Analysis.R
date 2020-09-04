library(downloader)
library(jsonlite)
library(tidyverse)
library(mongolite)
library(quanteda)
library(tictoc)
library(lubridate)

# Set parallelisation parameters for Quanteda threading:
quanteda_options(threads = (RcppParallel::defaultNumThreads() - 1))
cat(paste0("Quanteda using ", quanteda_options("threads"), " parallel processing threads.\nLeaving 1 available for you to browse for cat pictures while you wait..."))

# Download and unzip openFDA drug label datasets:
for (f in 1:9) {
  url = paste0("https://download.open.fda.gov/drug/label/drug-label-000", f ,"-of-0009.json.zip")
  download(url, dest = "json.zip")
  unzip("json.zip", exdir = "datasets_json")
}

# Housecleaning:
file.remove("json.zip")

#######################
##### MONGO SETUP #####
#######################

# Create Mongo Client and initiallise collection for all-manufacturer's dataset:
coll_all <- mongo(collection = "all_manufacturers", db = "openFDA", url = "mongodb://127.0.0.1:27017/")


####################################
##### EXTRACT, TRANSFORM, LOAD #####
####################################

# Function for converting list columns to vector and clean NULL and NA:
transform_list_column <- function(list_field) {
  temp <- sapply(list_field, function (x) paste0(x, collapse = ", "))
  temp <- na_if(temp, "NULL")
  temp <- replace_na(temp, "")
}

# Function for transforming a json file from openFDA to a dataframe with the variables of interest:
transform_results_object_to_df <- function(data_object, 
                                           outer_vector_columns, 
                                           outer_list_columns, 
                                           outer_df_column, 
                                           inner_df_list_columns) {
  outer_all_columns <- c(outer_vector_columns, outer_list_columns)  
  outer_df <- data_object[ , outer_all_columns] %>% 
    mutate_at(outer_list_columns, transform_list_column)
  
  inner_df <- data_object %>% 
    pull(all_of(outer_df_column)) %>% 
    select(all_of(inner_df_list_columns)) %>% 
    mutate_at(inner_df_list_columns, transform_list_column)
  
  data_df <- bind_cols(inner_df, outer_df) %>% 
    relocate(set_id, effective_time, brand_name, generic_name, substance_name)
}

# Parameters for selecting variables of interest from openFDA drug label dataset:
outer_vector_columns <- c("set_id", "effective_time")
outer_list_columns <- c("active_ingredient", "inactive_ingredient", "spl_product_data_elements", "description", "purpose", "drug_interactions")
outer_df_column <- "openfda"
inner_df_list_columns <- c("manufacturer_name", "generic_name", "brand_name", "substance_name", "route", "product_type")

# Define path to dataset json files:
path_to_datasets_json <- "datasets_json/"
datasets_json <- list.files(path_to_datasets_json)

# Execute ETL:
tic("ETL Full Process")
for (i in 1:length(datasets_json)) {
  tic(paste0("ETL part ", i))
  dataset_temp <- fromJSON(paste0(path_to_datasets_json, datasets_json[i]))$results    # Extract
  dataset_temp <- transform_results_object_to_df(dataset_temp,                         # Transform
                                                 outer_vector_columns, 
                                                 outer_list_columns, 
                                                 outer_df_column, 
                                                 inner_df_list_columns)
  print(paste0("Estimated memory size dataset part ", i, ": ", format(object.size(dataset_temp), units = "Mb")))
  coll_all$insert(dataset_temp)                                                        # Load
  toc()
}
toc()

# console output:
# [1] "Estimated memory size dataset part 1: 37.8 Mb"
# ETL part 1: 30.38 sec elapsed
# [1] "Estimated memory size dataset part 2: 37.1 Mb"
# ETL part 2: 31.56 sec elapsed
# [1] "Estimated memory size dataset part 3: 39.2 Mb"
# ETL part 3: 34.45 sec elapsed
# [1] "Estimated memory size dataset part 4: 40.1 Mb"
# ETL part 4: 38.78 sec elapsed
# [1] "Estimated memory size dataset part 5: 39.6 Mb"
# ETL part 5: 35.34 sec elapsed
# [1] "Estimated memory size dataset part 6: 39.9 Mb"
# ETL part 6: 34.13 sec elapsed
# [1] "Estimated memory size dataset part 7: 37.3 Mb"
# ETL part 7: 31.29 sec elapsed
# [1] "Estimated memory size dataset part 8: 37.6 Mb"
# ETL part 8: 31.77 sec elapsed
# [1] "Estimated memory size dataset part 9: 27.9 Mb"
# ETL part 9: 20.58 sec elapsed
# ETL Full Process: 288.35 sec elapsed

# Housecleaning:
rm(list=base::setdiff(ls(), c("coll_all")))


##############################
##### FEATURE EXTRACTION #####
##############################

##### STOPWORDS:

# Define stopwords:
stopwords <- c(stopwords("english"))#, stopwords("french"), stopwords("portuguese"), stopwords("spanish"), stopwords("german"), stopwords("italian"))

# Stopwords you want to remove from the default stopwords list:
sw_to_exclude <- c("no", "not", "un", "eu", "são")
if (exists("sw_to_exclude")) {
  stopwords <- setdiff(stopwords, sw_to_exclude)
}

# Words you don't want leading or trailing a MWE, but which are acceptable inside the MWE:
sw_to_include <- c()
if (exists("sw_to_include")) {
  stopwords <- c(stopwords, sw_to_include)
}


##### TOKENIZATION AND COLLOCATION EXTRACTION FUNCTIONS:

tokenize_corpus <- function(corpus_obj) {
  # Description:
  # - Function for tokenizing a quanteda corpus object:
  # Params:
  # - corpus_obj: a Quanteda corpus object
  # Returns:
  # - tokens_obj: a Quanteda tokens object
  
  tic("Tokenize corpus")
  # Tokenize without removing any characters, and convert to lowercase:
  tokens_obj <- tokens_tolower(tokens(x = corpus_obj,
                                      split_hyphens = FALSE,
                                      remove_punct = FALSE,
                                      remove_numbers = FALSE,
                                      remove_symbols = FALSE))
  
  # Filter tokens for only those used for natural language (in latin script plus diacritics, ligatures, etc.):
  tokens_obj <- tokens_select(tokens_obj,
                              pattern = "^[A-Za-zšœÀ-ÖØ-öø-ÿ0-9&_\\(\\)-]+$",
                              valuetype = "regex",
                              selection = "keep",
                              padding = TRUE)
  
  
  # remove tokens consisting of only 1 letter:
  tokens_obj <- tokens_select(tokens_obj,
                              pattern = "(\\w{2,})",
                              valuetype = "regex",
                              selection = "keep",
                              padding = TRUE)
  
  # Print # of unique tokens:
  tokens_obj %>% 
    types() %>% 
    length() %>% 
    paste0("Number of unique tokens: ", .) %>% 
    print()
    
  toc()
  return(tokens_obj)
}


estimate_mwe <- function(tokens_obj, min_length, max_length, min_count) {
  # Description:
  # - Function for estimating collocations (Multi-word Expressions, MWE)
  # Params:
  # - tokens_obj: a Quanteda tokens object
  # - min_length: minimum n-gram size
  # - max_length: maximum n-gram size
  # - min_count: minimum number of occurrences of a term in the corpus
  # Returns:
  # - mwe_obj: a Quanteda collocations object, with collocations, frequency counts, nested counts, and PMI statistics.
  
  tic("Estimate collocations")
  
  # Estimate collocations using Quanteda library:
  mwe_obj <- textstat_collocations(x = tokens_obj, size = min_length:max_length, min_count = min_count)
  
  # Function for vectorization of stopword matching:
  match_sw <- function(x, stopwords){
    x %in% stopwords
  }
  
  # Flag leading stopwords
  mwe_obj$leading_sw <- sapply(X = word(string = mwe_obj$collocation, start = 1L), FUN = match_sw, stopwords)
  
  # Flag trailing stopwords
  mwe_obj$trailing_sw <- sapply(X = word(string = mwe_obj$collocation, start = -1L),FUN = match_sw, stopwords)
  
  # Remove leading and trailing stopwords
  mwe_obj <- mwe_obj %>%
    filter(leading_sw == FALSE & trailing_sw == FALSE)
  
  # Remove dummy columns:
  mwe_obj$leading_sw <- NULL
  mwe_obj$trailing_sw <- NULL
  
  # Print number of extracted collocations after filtering:
  mwe_obj$collocation %>%
    length() %>%
    paste0("Number of collocations remaining: ",.) %>%
    print()
  
  toc()
  return(mwe_obj)
}


##### EXTRACT AZ-SPECIFIC COLLOCATIONS:

# Retrieve AZ documents from MongoDB, selecting only the fields conducive for learning AZ ingredient-specific collocations:
corpus_df <- coll_all$find(query = '{"manufacturer_name" : "AstraZeneca Pharmaceuticals LP"}', 
                           fields = '{"set_id":true, 
                                    "manufacturer_name":true,
                                    "brand_name":true,
                                    "generic_name":true,
                                    "substance_name":true,
                                    "active_ingredient":true, 
                                    "inactive_ingredient":true,
                                    "description":true,
                                    "_id":false}',
                           limit = 1000)

# Concatenate all relevant fields into a single string to pass as documents to Quanteda's collocation extraction function:
# I include the "description" field as it contains AZ's description of active and inactive ingredients:
corpus_df <- corpus_df %>% 
  unite("text", c(brand_name, 
                  generic_name, 
                  substance_name,
                  active_ingredient,
                  inactive_ingredient,
                  description), remove = FALSE, na.rm = TRUE, sep = ", ") %>% 
  select(set_id, manufacturer_name, brand_name, text) 

# Create Quanteda corpus object:
corpus_obj <- corpus(x = corpus_df, text_field = "text", docid_field = "set_id")

# Tokenize:
tokens_obj <- tokenize_corpus(corpus_obj)
# [1] "Number of unique tokens: 1196"
# Tokenize corpus: 0.13 sec elapsed

# Explore a documents tokenization result:
tokens_obj[[sample(ndoc(tokens_obj), 1)]]

# Estimate collocations:
mwe_obj <- estimate_mwe(tokens_obj, min_length = 2, max_length = 3, min_count = 3)
# [1] "Number of collocations remaining: 493"
# Estimate collocations: 0.28 sec elapsed

# Explore top collocations:
mwe_obj %>% 
  head(30)

# Ad-hoc post-processing:
mwe_az <- mwe_obj %>%
  filter(! grepl(pattern = "^([0-9]|mg|mcg).*$", .$collocation)) %>% 
  filter(! grepl(pattern = "\\btablet(s)*\\b|\\bdose(s)*\\b|\\bfilm\\b|\\bfollowing\\b", .$collocation))

# Explore curated top collocations for AZ:
mwe_az %>% 
  head(30)


##### EXTRACT ALL-MANUFACTURERS COLLOCATIONS:

# Retrieve all documents from MongoDB:
corpus_df <- coll_all$find(query = '{}', 
                           fields = '{"set_id":true, 
                                     "manufacturer_name":true,
                                     "brand_name":true,
                                     "generic_name":true,
                                     "substance_name":true,
                                     "active_ingredient":true, 
                                     "inactive_ingredient":true,
                                     "_id":false}')
# Explore corpus:
corpus_df %>% 
  head() %>% 
  as_tibble()

# Remove recurrent phrases "active ingredient" and "inactive ingredient" to reduce noise:
active_ingredient_pattern = c("ingredient( active)*|active i\\s*nd*gre+di*e+nt(s|s:|:)*|active ingrediet|active in*gredien\\s*t|active ingrede*int|active lngredient|active ingredien|active ingredinet|active ingreditents*")
inactive_ingredient_pattern = c("ingredient( inactive)*|inactive i\\s*nd*gre+di*e+nt(s|s:|:)*|inactive ingrediet|inactive in*gredien\\s*t|inactive ingrede*int|inactive lngredient|inactive ingredien|inactive ingredinet|inactive ingreditents*")

tic("Pre-process all manufacturers corpus")
corpus_df <- corpus_df %>% 
  mutate(active_ingredient = gsub(pattern = active_ingredient_pattern, replacement = "", x = .$active_ingredient, ignore.case = TRUE)) %>% 
  mutate(inactive_ingredient = gsub(pattern = inactive_ingredient_pattern, replacement = "", x = .$inactive_ingredient, ignore.case = TRUE))
toc()
# Pre-process all manufacturers corpus: 8.65 sec elapsed

# Concatenate all relevant fields into a single string. 
# I remove the description field to reduce vocabulary and model complexity: 
tic("Process all manufacturers corpus")
corpus_df <- corpus_df %>% 
  unite("text", c(brand_name, 
                  generic_name, 
                  substance_name,
                  active_ingredient,
                  inactive_ingredient
  ), remove = FALSE, na.rm = TRUE, sep = ", ") %>% 
  select(set_id, manufacturer_name, brand_name, text)
toc()
# Process all manufacturers corpus: 1.72 sec elapsed

# Create Quanteda corpus object:
corpus_obj <- corpus(x = corpus_df, text_field = "text", docid_field = "set_id")

# Tokenize:
tokens_obj <- tokenize_corpus(corpus_obj)
# [1] "Number of unique tokens: 49259"
# Tokenize corpus: 14.73 sec elapsed

# Explore a documents tokenization result:
tokens_obj[[1]]

# Estimate collocations:
mwe_obj <- estimate_mwe(tokens_obj, min_length = 2, max_length = 3, min_count = 20)
# [1] "Number of collocations remaining: 13772"
# Estimate collocations: 148.82 sec elapsed

# Explore top collocations:
mwe_obj %>% 
  head(50)

# Apply same Ad-hoc post-processing as for AZ collocations:
mwe_all <- mwe_obj %>%
  filter(! grepl(pattern = "^([0-9]|mg|mcg).*$", .$collocation)) %>% 
  filter(! grepl(pattern = "\\btablet(s)*\\b|\\bdose(s)*\\b|\\bfilm\\b|\\bfollowing\\b", .$collocation))

# Explore curated top collocations for all manufacturers:
mwe_all %>% 
  head(50)

# Create collocations set from union of fine-grained AZ-specific collocations and broad all-manufacturer collocations:
mwe_obj <- c(mwe_az$collocation, mwe_all$collocation) %>% 
  unique() %>% 
  .[!grepl(pattern = "^.+\\s{1}and\\s{1}.+$", x = .)]


########################################
##### UNIQUE INGREDIENT EXTRACTION #####
########################################

# Retrieve AZ documents from MongoDB, including the "spl_product_data_elements" field for extracting and quantifying average ingredients per medication:
corpus_df <- coll_all$find(query = '{"manufacturer_name": "AstraZeneca Pharmaceuticals LP"}', 
                           fields = '{"set_id":true, 
                                      "effective_time":true,
                                      "manufacturer_name":true,
                                      "brand_name":true,
                                      "generic_name":true,
                                      "substance_name":true,
                                      "spl_product_data_elements":true,
                                      "_id":false}')

# Pre-process corpus and add year feature:
corpus_df %<>%
  mutate(date_nchar = nchar(effective_time)) %>%     # Count the number of characters in the "effective_date" column
  filter(date_nchar == 8) %>%                        # validate that the "effective_date" columns values have an 8 character format
  select(-date_nchar) %>% 
  mutate(effective_time = ymd(effective_time)) %>%   # Convert "effective_date" column to date format
  mutate(year = year(effective_time)) %>% 
  relocate(set_id, year)

# Create Quanteda corpus object:
corpus_obj <- corpus(x = corpus_df, text_field = "spl_product_data_elements", docid_field = "set_id")

# Tokenize:
tokens_obj <- tokenize_corpus(corpus_obj)
# [1] "Number of unique tokens: 280"
# Tokenize corpus: 0.03 sec elapsed

# Explore a documents tokenization result:
tokens_obj[[1]]

# Compound MWE in spl_product_data_elements to create multi-word tokens:
tic("Compound AZ ingredients tokens")
tokens_compounded <- tokens_compound(tokens_obj,
                                     pattern = phrase(mwe_obj),
                                     join = TRUE)
toc()
# Compound AZ ingredients tokens: 0.53 sec elapsed

# Explore a document's compounding of collocations:
tokens_compounded[[1]]


identify_unique_ingredients <- function(compounded_doc, ingredient_stopwords, brand_name, generic_name, substance_name) {
  # Description:
  # - Funciton for identifying unique ingredients in a label's document.
  # Params:
  # - compounded_doc: a Quanteda tokens object, with compounded collocations
  # - ingredient_stopwords: user-curated ingredient stopwords for noisy terms
  # - brand_name: medicine's brand name
  # - generic_name: edicine's generic name
  # - substance_name: edicine's substance name
  # Returns:
  # - List of unique tokens
  temp_bl <- c(ingredient_stopwords, brand_name, generic_name, substance_name) %>% 
    unique() %>% 
    tolower() %>% 
    str_replace_all(" ", "_") %>% 
    paste0(., collapse = "$|^") %>% 
    paste0("^", ., "$", collapse = "")
  
  temp_toks <- compounded_doc %>% 
    unique() %>% 
    .[!grepl(pattern = "^[0-9]+$", x = .)] %>% 
    .[nchar(.) > 3] %>% 
    .[!grepl(pattern = temp_bl, x = .)]
}

# Define list of user-curated stopwords:
ingredient_stopwords <- c("unspecified", "unspecified_form", "biconvex", "biconvenx", "wamw", "and", "pale", "light", "light_-*", "dark", "capsule", "round", "yellow", "blue", "red", "beige", "brown", "pink", "white", "black")

# Extract unique ingredients per medicine:
tic("Unique ingredients ist")
unique_ingredients_list <- vector("list", length = nrow(corpus_df))
for (i in 1:nrow(corpus_df)) {
  brand_name <- corpus_df$brand_name[i]
  generic_name <- corpus_df$generic_name[i]
  substance_name <- corpus_df$substance_name[i]
  
  temp_comp_toks <- tokens_compounded[[i]]
  unique_ingredients <- identify_unique_ingredients(temp_comp_toks, ingredient_stopwords, brand_name, generic_name, substance_name)
  unique_ingredients_list[[i]] <- unique_ingredients
}
toc()
# Unique ingredients ist: 0.09 sec elapsed

# Add unique ingredients list to corpus dataframe:
tic()
corpus_df$unique_ingredients <- unique_ingredients_list %>% 
  lapply(., function(x) paste0(x, collapse = ", ")) %>% 
  unlist()
toc()
# 0.01 sec elapsed

# Calculate the number of unique ingredients per medicine:
tic()
corpus_df$unique_ingredients_count <- unique_ingredients_list %>% 
  lapply(., function(x) length(x)) %>% 
  unlist()
toc() 
# 0.03 sec elapsed

########################################################################
# Calculate the average number of ingredients of AZ medicines per year #
########################################################################
task_a_results <- corpus_df %>% 
  group_by(year) %>% 
  summarise(drug_names = paste0(brand_name %>%
                                  str_to_title(), collapse = ", "),
            avg_number_of_ingredients = mean(unique_ingredients_count) %>% 
              round(digits = 2))

# Visualize results:
ggplot(task_a_results, mapping = aes(x = year, y = avg_number_of_ingredients, label = drug_names)) +
  geom_bar(stat = "Identity") +
  geom_label() +
  xlab("Year") +
  ylab("Mean # of Ingredients") +
  ggtitle("Ingredients in AstraZeneca Medications")



# Retrieve all documents from MongoDB, including the "spl_product_data_elements" field for extracting and quantifying average ingredients per medication:
corpus_df <- coll_all$find(query = '{}', 
                           fields = '{"set_id":true, 
                                      "effective_time":true,
                                      "manufacturer_name":true,
                                      "brand_name":true,
                                      "generic_name":true,
                                      "substance_name":true,
                                      "spl_product_data_elements":true,
                                      "route":true,
                                      "_id":false}')

# Pre-process corpus, add year feature, and filter for recent medicines:
corpus_df %<>%
  mutate(date_nchar = nchar(effective_time)) %>%                          # Count the number of characters in the "effective_date" column
  filter(date_nchar == 8) %>%                                             # validate that the "effective_date" columns values have an 8 character format
  select(-date_nchar) %>% 
  mutate(effective_time = ymd(effective_time)) %>%                        # Convert "effective_date" column to date format
  mutate(year = year(effective_time)) %>%
  filter(year >= 2009) %>%                                                # Filter only for medications from 2009 onwards, as there are only very few medications before that year in the dataset.
  mutate(route = if_else(route == "", "NOT SPECIFIED", route)) %>%        # Replace missing "route" values with "not_specified".
  relocate(set_id, year)

# Create Quanteda corpus object:
corpus_obj <- corpus(x = corpus_df, text_field = "spl_product_data_elements", docid_field = "set_id")

# Tokenize:
tokens_obj <- tokenize_corpus(corpus_obj)
# [1] "Number of unique tokens: 48397"
# Tokenize corpus: 16.63 sec elapsed

# Explore a documents tokenization result:
tokens_obj[1:5]

# Compound MWE in spl_product_data_elements to create multi-word tokens:
tic("Compound all manufacturers ingredients tokens")
tokens_compounded <- tokens_compound(tokens_obj,
                                     pattern = phrase(mwe_obj),
                                     join = TRUE)
toc()
# Compound all manufacturers ingredients tokens: 2.8 sec elapsed

# Explore a document's compounding of collocations:
tokens_compounded[1:5]

# For all manufacturers I removed the brand, generic, and substance name, trading noise for speed:
identify_unique_ingredients_all_manufacturers <- function(compounded_doc, ingredient_stopwords) {
  # Description:
  # - Funciton for identifying unique ingredients in a label's document.
  # Params:
  # - compounded_doc: a Quanteda tokens object, with compounded collocations
  # - ingredient_stopwords: user-curated ingredient stopwords for noisy terms
  # Returns:
  # - List of unique tokens
  temp_bl <- c(ingredient_stopwords) %>% 
    unique() %>% 
    tolower() %>% 
    str_replace_all(" ", "_") %>% 
    paste0(., collapse = "$|^") %>% 
    paste0("^", ., "$", collapse = "")
  
  temp_toks <- compounded_doc %>% 
    unique() %>% 
    .[!grepl(pattern = "^[0-9]+$", x = .)] %>% 
    .[nchar(.) > 3] %>% 
    .[!grepl(pattern = temp_bl, x = .)]
}

# Extract unique ingredients per medicine:
tic("Unique ingredients list all manufacturers")
unique_ingredients_list <- vector("list", length = nrow(corpus_df))
for (i in 1:nrow(corpus_df)) {
  temp_comp_toks <- tokens_compounded[[i]]
  unique_ingredients <- identify_unique_ingredients_all_manufacturers(temp_comp_toks, ingredient_stopwords)
  unique_ingredients_list[[i]] <- unique_ingredients
}
toc()
# Unique ingredients list all manufacturers: 1226.58 sec elapsed

# Add unique ingredients list to corpus dataframe:
tic()
corpus_df$unique_ingredients <- unique_ingredients_list %>% 
  lapply(., function(x) paste0(x, collapse = ", ")) %>% 
  unlist()
toc()
# 1.72 sec elapsed

# Calculate the number of unique ingredients per medicine:
tic()
corpus_df$unique_ingredients_count <- unique_ingredients_list %>% 
  lapply(., function(x) length(x)) %>% 
  unlist()
toc() 
# 0.19 sec elapsed

#####################################################################################################
# Calculate the average number of ingredients of all medicines per year per route of administration #
#####################################################################################################
task_b_results <- corpus_df %>% 
  group_by(year, route) %>% 
  summarise(avg_number_of_ingredients = mean(unique_ingredients_count) %>% 
              round(digits = 2))

ggplot(task_b_results %>% 
         filter(year > 2014, year <= 2020) %>% 
         filter(route != "NOT SPECIFIED") %>% 
         mutate(num_routes = str_count(string = route, pattern = ",") + 1) %>% 
         filter(num_routes == 1), mapping = aes(x = year, y = avg_number_of_ingredients, label = route)) +
  geom_point() +
  geom_label() +
  xlab("Year") +
  ylab("Mean # of Ingredients") +
  ggtitle("Ingredients in All Manufacturers Medications")

