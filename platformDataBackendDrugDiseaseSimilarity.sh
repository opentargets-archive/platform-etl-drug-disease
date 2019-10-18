#!/bin/bash

#/home/mkarmona
#/data/drug_disease_similarity/19.09_drug-data.json
#/data/drug_disease_similarity/19.09_efo-data.json
#/data/drug_disease_similarity/19.09_expression-data.json
#/data/drug_disease_similarity/19.09_gene-data.json
#/data/drug_disease_similarity/evidences_sampled.json
#/data/drug_disease_similarity/out_190920_evidences_drug_aggregation_1909/
#/data/drug_disease_similarity/predictions.190905.parquet/
#/data/drug_disease_similarity/protein_pair_interactions.json
#/data/drug_disease_similarity/significant_AEs_by_drug.json
#/data/drug_disease_similarity/significant_AEs_by_target.json
#/data/drug_disease_similarity/studies.parquet/

export JAVA_OPTS="-Xms1G -Xmx200G"
time amm platformDataBackendDrugDiseaseSimilarity.sc \
  --drugFilename "/data/drug_disease_similarity/19.09_drug-data.json" \
  --targetFilename "/data/drug_disease_similarity/19.09_gene-data.json" \
  --diseaseFilename "/data/drug_disease_similarity/19.09_efo-data.json" \
  --evidenceFilename "/data/drug_disease_similarity/19.09_evidence-data.json" \
  --interactionsFilename "/data/drug_disease_similarity/protein_pair_interactions.json" \
  --aggregatedDrugsFilename "/data/drug_disease_similarity/out_190920_evidences_drug_aggregation_1909/part-*" \
  --studiesFilename "/data/drug_disease_similarity/studies.parquet/" \
  --predictionsFilename "/data/drug_disease_similarity/predictions.190905.parquet/" \
  --faersByDrugFilename "/data/drug_disease_similarity/significant_AEs_by_drug.json" \
  --faersByTargetFilename "/data/drug_disease_similarity/significant_AEs_by_target.json" \
  --expressionFilename "/data/drug_disease_similarity/19.09_expression-data.json" \
  --whitelistFilename "/data/drug_disease_similarity/whitelist.json" \
  --outputPathPrefix "drug_disease_similarity_whitelisted"