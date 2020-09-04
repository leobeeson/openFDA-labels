# Analysis of Ingredients in the openFDA Drug Label Dataset

## Instructions:
1. First review the jupyter notebook at *01_openFDA_API_Exploration/QueryOpenFDA-API.ipynb* for the openFDA API exploration analysis.
2. Then review the analysis of ingredients in the openFDA drug label dataset, either as an html file, an Rmd file, or a pure R script:
* 02_openFDA_Label_Ingredients_Analysis/openFDA_Label_Ingredients_Analysis.html
* 02_openFDA_Label_Ingredients_Analysis/openFDA_Label_Ingredients_Analysis.Rmd
* 02_openFDA_Label_Ingredients_Analysis/openFDA_Label_Ingredients_Analysis.R

### Considerations:
* The datasets are not stored along with the code.
* If you want to run the 02_openFDA_Label_Ingredients_Analysis/openFDA_Label_Ingredients_Analysis.Rmd file, remove the `eval=FALSE` from the cells for downloading the full dataset, and from the cell for storing it into MongoDB.

### Objectives for this task
* Using the data from the OpenFDA API:
  + Determine the average number of ingredients contained in AstraZeneca's (AZ) medicines per year.
  + Determine the average number of ingredients across all manufacturers per year per route of administration.
  + Use the field `spl_product_data_elements` for identifying a medicine's ingredients.
  
### Problems
* The field `spl_product_data_elements` does not contain punctuations, difficulting the task of identifying the boundaries of multi-word ingredient.
* Given the morphology of the pharmaceutical linguistic domain, we can asssume that a significant portion of medications' ingredients are multi-worded, i.e. n-grams.

### Solution Proposal
1. Estimate ingredient-specific collocations (a.k.a Multi-word expressions [MWE]), leveraging the dataset's fields which contain properly punctuated lists of ingredients.
2. Use the learned ingredient-specific collocations to compound the unbounded multi-word ingredients in the `spl_product_data_elements` field.
3. Count the unique list of compounded multi-word ingredients plus single-word ingredients per AZ's medicines.
4. Count the unique list of compounded multi-word ingredients plus single-word ingredients for all manufacturers per year per route of administration.

