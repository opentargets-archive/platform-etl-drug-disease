import $ivy.`com.typesafe:config:1.3.4`
import $ivy.`com.github.fommil.netlib:all:1.1.2`
import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-mllib:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`sh.almond::ammonite-spark:0.7.0`
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object NetworkDB {
  def buildAnnotated(ndbPath: String, genesPath: String)(implicit ss: SparkSession): DataFrame = {
    val p2p = ss.read.json(ndbPath)
      .selectExpr("interactorA_uniprot_name as A",
        "interactorB_uniprot_name as B",
        "mi_score as score")

    val genes = ss.read.json(genesPath)
      .selectExpr("id",
        "uniprot_accessions as accessions",
        "approved_symbol as symbol",
        "hgnc_id")
      .withColumn("accession", explode(col("accessions")))
      .drop("accessions")
      .orderBy(col("accession"))
      .cache

    val p2pA = p2p.join(genes, genes("accession") === p2p("A"), "inner")
      .withColumnRenamed("symbol", "A_symbol")
      .withColumnRenamed("hgnc_id", "A_hgnc_id")
      .withColumnRenamed("id", "A_id")
      .drop("accession")

    p2pA.join(genes, genes("accession") === p2pA("B"), "inner")
      .withColumnRenamed("symbol", "B_symbol")
      .withColumnRenamed("hgnc_id", "B_hgnc_id")
      .withColumnRenamed("id", "B_id")
      .drop("accession")

  }

  def buildAsLUT(ndbPath: String, genesDF: DataFrame)(implicit ss: SparkSession): DataFrame = {
    val score = 0.45
    val p2pRaw = ss.read.json(ndbPath)
      //      .where(col("mi_score") > score or
      //        (array_contains(col("source_databases"), "intact") and
      //          (size(col("source_databases")) > 1)))
      .selectExpr("interactorA_uniprot_name as A",
        "interactorB_uniprot_name as B")

    val p2p = p2pRaw.union(p2pRaw.toDF("B", "A").select("A", "B"))
      .distinct()

    val genes = genesDF
      .selectExpr("target_id as id",
        "uniprot_accessions as accessions")
      .withColumn("accession", explode(col("accessions")))
      .drop("accessions")
      .orderBy(col("accession"))
      .cache

    val p2pA = p2p.join(genes, genes("accession") === p2p("A"), "inner")
      .withColumnRenamed("id", "A_id")
      .drop("accession")

    val doubleJoint = p2pA.join(genes, genes("accession") === p2pA("B"), "inner")
      .withColumnRenamed("id", "B_id")
      .drop("accession")

    doubleJoint.groupBy(col("A_id").as("target_id"))
      .agg(collect_set(col("B_id")).as("neighbours"),
        approx_count_distinct(col("B_id")).as("degree"))
  }
}

object Loaders {
  def loadExpression(path: String)(implicit ss: SparkSession): DataFrame = {
    ss.read.json(path)
      .selectExpr("gene as target_id", "tissues")
      .withColumn("tissue_list", expr(
        """
          |transform(filter(tissues,
          | t -> t.rna.zscore > 0 or t.protein.level > 0
          |), x -> x.efo_code)
          |""".stripMargin))
      .drop("tissues")
      .withColumnRenamed("tissue_list", "tissues")
  }

  /** load a drug datasset from OpenTargets drug index dump */
  def loadDrugs(path: String)(implicit ss: SparkSession): DataFrame = {
    val columns = Seq(
      "id as drug_id",
      "max_clinical_trial_phase",
      "type as drug_type",
      "pref_name",
      "mechanisms_of_action",
      "indications.efo_id as indication_ids",
      "number_of_mechanisms_of_action"
    )

    val drugList = ss.read.json(path)
    drugList
      .selectExpr(columns:_*)
  }

  def loadTargets(path: String)(implicit ss: SparkSession): DataFrame = {
    val columns = Seq(
      "id as target_id",
      "approved_symbol as target_name",
      "biotype",
      "gos",
      "tractability",
      "uniprot_accessions",
      "uniprot_subcellular_location",
      "uniprot_similarity"
    )

    val goExpr =
      expr("""
        |transform(go,
        | t -> struct(t.id as code, t.value.term as term)
        |)
        |""".stripMargin)

    val targetList = ss.read.json(path)
    targetList
      .withColumn("gos", goExpr)
      .selectExpr(columns:_*)
  }

  def loadNetwork(path: String, genes: DataFrame, expressions: DataFrame)(implicit ss: SparkSession): DataFrame = {
    val columns = Seq(
      "*"
    )

    val expressionsN = expressions
      .withColumnRenamed("target_id", "neighbour")
      .withColumnRenamed("tissues", "neighbour_tissues")

    val network = NetworkDB.buildAsLUT(path, genes)
        .join(expressions, Seq("target_id"))
        .withColumnRenamed("tissues", "target_id_tissues")
        .withColumn("neighbour", explode(col("neighbours")))
        .join(expressionsN, Seq("neighbour"))
        .withColumn("intersected_tissues_cardinality",
          size(array_intersect(col("target_id_tissues"), col("neighbour_tissues"))))
        .where("intersected_tissues_cardinality > 0")
        .groupBy("target_id")
        .agg(
          collect_list(col("neighbour")).as("neighbours")
        )

    network
  }

  def loadDiseases(path: String)(implicit ss: SparkSession): DataFrame = {
    val columns = Seq(
      "id as disease_id",
      "label as disease_name",
      "ancestors",
      "descendants",
      "phenotypes",
      "therapeutic_codes as therapeutic_areas"
    )

    val diseaseList = ss.read.json(path)

    val genAncestors = udf((codes: Seq[Seq[String]]) =>
      codes.view.flatten.toSet.toSeq)

    val efos = diseaseList
      .withColumn("id", substring_index(col("code"), "/", -1))
      .withColumn("ancestors", genAncestors(col("path_codes")))

    val descendants = efos
      .where(size(col("ancestors")) > 0)
      .withColumn("ancestor", explode(col("ancestors")))
      // all diseases have an ancestor, at least itself
      .groupBy("ancestor")
      .agg(collect_set(col("id")).as("descendants"))
      .withColumnRenamed("ancestor", "id")

    efos.join(descendants, Seq("id"))
      .selectExpr(columns:_*)
  }

  def loadEvidencesFromGenetics(studiesPath: String, predictionsPath: String)(implicit ss: SparkSession): DataFrame = {
    val columns = Seq(
      "study_id",
      "trait_reported",
      "trait_efos",
      "trait_category"
    )

    val predictionsColumns = Seq(
      "study_id",
      "concat_ws('_', chrom, string(pos), ref, alt) as variant_id",
      "y_proba_all_features as score",
      "gene_id as target_id"
    )

    val studies = ss.read.parquet(studiesPath)
      .selectExpr(columns:_*)

    val predictions = ss.read.parquet(predictionsPath)
      .selectExpr(predictionsColumns:_*)

    val evidences = predictions.join(studies, Seq("study_id"), "inner")
      .withColumn("disease_id", explode(col("trait_efos")))
      .withColumn("datasource", lit("genetics"))

    evidences
      .drop("trait_efos")
      .where(col("score") > 0.5)
      .withColumn("evs_id", sha1(concat(col("study_id"), col("variant_id"), col("disease_id"), col("target_id"))))
      .selectExpr("disease_id", "evs_id", "score", "target_id", "datasource")
  }

  def loadEvidences(path: String)(implicit ss: SparkSession): DataFrame = {
    val columns = Seq(
      "sourceID as datasource",
      "disease.id as disease_id",
      "target.id as target_id",
      "id as evs_id",
      "scores.association_score as score"
    )

    val evidences = ss.read.json(path)
    evidences
      .where("sourceID = 'europepmc'")
      .selectExpr(columns:_*)
  }

  def loadFaersByDrug(path: String)(implicit ss: SparkSession): DataFrame = {
    val columns = Seq(
      "chembl_id as drug_id",
      "event as drug_ae_event",
      "count as drug_ae_count",
      "llr as drug_ae_llr",
      "critval as drug_ae_llr_critval"
    )

    ss.read.json(path)
      .selectExpr(columns:_*)
      .groupBy(col("drug_id"))
      .agg(collect_list(struct(
        col("drug_ae_event"),
        col("drug_ae_count"),
        col("drug_ae_llr"),
        col("drug_ae_llr_critval")
      )).as("aes"))
  }

  def loadFaersByTarget(path: String)(implicit ss: SparkSession): DataFrame = {
    val columns = Seq(
      "target_id",
      "event as target_ae_event",
      "report_count as target_ae_count",
      "llr as target_ae_llr",
      "critval as target_ae_llr_critval"
    )

    ss.read.json(path)
      .selectExpr(columns:_*)
      .groupBy(col("target_id"))
      .agg(collect_list(struct(
        col("target_ae_event"),
        col("target_ae_count"),
        col("target_ae_llr"),
        col("target_ae_llr_critval")
      )).as("aes"))
  }

  def loadDrugAggragationsFromEvidences(path: String)(implicit ss: SparkSession): DataFrame = {
    val columns = Seq(
      "disease_id",
      "drug_id",
      "associated_diseases as associated_disease_ids",
      "associated_targets as associated_target_ids"
    )

    val aggregation = ss.read.json(path)

    aggregation
      .selectExpr(columns:_*)
  }
}

@main
def main(drugFilename: String,
         targetFilename: String,
         diseaseFilename: String,
         evidenceFilename: String,
         interactionsFilename: String,
         aggregatedDrugsFilename: String,
         studiesFilename: String,
         predictionsFilename: String,
         faersByDrugFilename: String,
         faersByTargetFilename: String,
         expressionFilename: String,
         outputPathPrefix: String): Unit = {
  val sparkConf = new SparkConf()
    .set("spark.driver.maxResultSize", "0")
    .setAppName("similarities-loaders")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  import ss.implicits._

  val drugs = Loaders.loadDrugs(drugFilename)
  val expressions = Loaders.loadExpression(expressionFilename)
  val targets = Loaders.loadTargets(targetFilename)
  val diseases = Loaders.loadDiseases(diseaseFilename)
  val interactions = Loaders.loadNetwork(interactionsFilename, targets, expressions)
  val aggregatedDrugsFromAssociations = Loaders.loadDrugAggragationsFromEvidences(aggregatedDrugsFilename)
  val evidences = Loaders.loadEvidences(evidenceFilename)
  val geneticsEvidences = Loaders.loadEvidencesFromGenetics(studiesFilename, predictionsFilename)
  val aesByDrug = Loaders.loadFaersByDrug(faersByDrugFilename)
  val aesByTarget = Loaders.loadFaersByTarget(faersByTargetFilename)

  /*
    building drugs + faers + aggregations (disease, drug)
    ideally, what we want is a disease and all related drugs
    with the rest of the information per drug
   */
  val dfDr = drugs.join(aesByDrug, Seq("drug_id"), "left_outer")
    .withColumnRenamed("aes", "drug_aes")
    .join(aggregatedDrugsFromAssociations, Seq("drug_id"), "right_outer")
    .groupBy("disease_id")
    .agg(collect_list(struct(
      col("drug_aes"),
      col("drug_id"),
      col("indication_ids"),
      col("max_clinical_trial_phase"),
      col("mechanisms_of_action"),
      col("number_of_mechanisms_of_action"),
      col("pref_name")
    )).as("drugs_for_disease"),
      first(col("associated_disease_ids")).as("associated_disease_ids"),
      first(col("associated_target_ids")).as("associated_target_ids")
    )

  /*
    building targets + drugs(by mechanisms of action) + faers
    ideally, what we want is a target and all related drugs
    with the rest of the information per drug
   */
  val dfDrByMOA = drugs
    .where(col("number_of_mechanisms_of_action") > 0)
    .withColumn("target_ids", flatten(expr("transform(mechanisms_of_action, m -> transform(m.target_components, c -> c.ensembl))")))
    .withColumn("target_id", explode(col("target_ids")))
    .groupBy(col("target_id"))
    .agg(
      collect_list(struct(
        col("drug_id"),
        col("max_clinical_trial_phase"),
        col("drug_type"),
        col("pref_name"),
        col("indication_ids")
      )).as("drugs_for_target")
    ).join(aesByTarget, Seq("target_id"), "left_outer")
    .withColumnRenamed("aes", "target_aes")

  /*
    already prepared diseases and targets
    including tissue expression filtered for network data
   */
  val dfD = diseases.join(dfDr, Seq("disease_id"), "left_outer")
  val dfT = targets.join(dfDrByMOA, Seq("target_id"), "left_outer")
      .join(interactions, Seq("target_id"), "left_outer")

  val evs = evidences.unionByName(geneticsEvidences)

  val evsScores = evs.selectExpr("evs_id", "datasource", "score")
    .groupBy(col("evs_id"))
    .pivot(col("datasource"))
    .agg(first(col("score")))
    .na.fill(0.0)

  val preparedEvs = evs.join(evsScores, Seq("evs_id"), "inner")
    .join(dfT.selectExpr("target_id", "neighbours"),
      Seq("target_id"))
    .withColumn("neighbour", explode(array_union(col("neighbours"), array(col("target_id")))))
//    .join(dfD, Seq("disease_id"))

  val associations = preparedEvs
    .groupBy(col("neighbour").as("target_id"), col("disease_id"))
    .agg(count(col("evs_id")).as("evidence_count"),
      slice(sort_array(collect_list(col("genetics")), false), 1, 100).as("genetics_score_list"),
      slice(sort_array(collect_list(col("europepmc")), false), 1, 100).as("literature_score_list"))
    .withColumn("harmonic_genetics",
      expr(
        """
          |aggregate(
          | zip_with(
          |   genetics_score_list,
          |   sequence(1, size(genetics_score_list)),
          |   (e, i) -> (e / pow(i,2))
          | ),
          | 0D,
          | (a, el) -> a + el
          |)
          |""".stripMargin))
    .withColumn("harmonic_literature",
      expr(
        """
          |aggregate(
          | zip_with(
          |   literature_score_list,
          |   sequence(1, size(literature_score_list)),
          |   (e, i) -> (e / pow(i,2))
          | ),
          | 0D,
          | (a, el) -> a + el
          |)
          |""".stripMargin))
    .withColumn("harmonic",
      expr(
        """
          |aggregate(
          | zip_with(
          |   sort_array(array(harmonic_genetics, harmonic_literature * 0.2), false),
          |   sequence(1, size(array(harmonic_genetics, harmonic_literature))),
          |   (e, i) -> (e / pow(i,2))
          | ),
          | 0D,
          | (a, el) -> a + el
          |)
          |""".stripMargin))
    .where("harmonic > 0.1")
    .join(dfT, Seq("target_id"))
    .join(dfD, Seq("disease_id"))
    .withColumn("new_drugs", array_except(col("drugs_for_target.drug_id"), col("drugs_for_disease.drug_id")))
    .withColumn("new_drugs_size", size(col("new_drugs")))
    .where("new_drugs_size > 0")

  associations.write.parquet(outputPathPrefix + "/associations/")

  val drugDiseaseDF = associations.selectExpr("disease_id",
    "target_id",
    "harmonic",
    "harmonic_genetics",
    "harmonic_literature",
    "target_name",
    "disease_name",
    "therapeutic_areas",
    "array_distinct(flatten(transform(drugs_for_disease, d -> transform(d.drug_aes, ae -> ae.drug_ae_event)))) as disease_aes_from_drugs",
    "array_distinct(flatten(drugs_for_disease.indication_ids)) as disease_indication_from_drugs",
    "array_max(drugs_for_disease.max_clinical_trial_phase) as disease_max_clinical_trial_phase_from_drugs",
    "associated_disease_ids as associated_disease_ids_from_disease_drug_agg",
    "associated_target_ids as associated_target_ids_from_disease_drug_agg",
    "new_drugs as hypotheses"
  )

  val cachedAEs = aesByDrug.selectExpr("drug_id", "aes.drug_ae_event as drug_ae_events")
    .cache

  val drugDisease = drugDiseaseDF.withColumn("drug_hypothesis", explode(col("hypotheses")))
    .join(cachedAEs, col("drug_hypothesis") === col("drug_id"), "left_outer")
    .withColumnRenamed("drug_ae_events", "drug_hypothesis_aes")
    .withColumn("drug_hypothesis_aes_score",
      expr("1.0 - (size(array_except(drug_hypothesis_aes, disease_aes_from_drugs)) / size(drug_hypothesis_aes))"))
    .withColumn("disease_aes_score",
      expr("1.0 - (size(array_except(disease_aes_from_drugs, drug_hypothesis_aes)) / size(disease_aes_from_drugs))"))
    // it needs to improve as a proper score
    .withColumn("drug_hypothesis_disease_aes_score",
      expr("drug_hypothesis_aes_score * disease_aes_score"))
//    .where("drug_hypothesis_disease_aes_score > 0.0")

  drugDisease.write.json(outputPathPrefix + "/drug_disease/")

//  dfD.write.json(outputPathPrefix + "/diseases/")
//  dfT.write.json(outputPathPrefix + "/targets/")
//  geneticsEvidences.write.json(outputPathPrefix + "/genetics_evidences/")
}
