package SPARKDIN

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, types}
import org.apache.spark.sql.functions._



object Funciones {


  def getSources(sources:DataFrame): DataFrame = {

    val sourcesOut = sources.select(
      explode(col("dataflows.sources")).as("sources")).select("sources.*")

    sourcesOut
  }

  def getTransformations(transformations:DataFrame): (DataFrame, DataFrame, DataFrame, DataFrame) = {

    val transformationsOut = transformations.select(
      explode(col("dataflows.transformations")).as("transformations")).select("transformations.*")

    val transformationsParams = transformations.select(
      explode(col("dataflows.transformations.params")).as("transformations_params"))
      .select("transformations_params.*")

    val transformationsParamsKO = transformations.select(
      explode(col("dataflows.transformations.params")).as("transformations_params"))
      .select(explode(col("transformations_params.validations")).as("transformations_params_valid"))
      .select("transformations_params_valid.*")

    val transformationsParamsAdd = transformations.select(
      explode(col("dataflows.transformations.params")).as("transformations_params"))
      .select(explode(col("transformations_params.addFields")).as("transformations_params_addFields"))
      .select("transformations_params_addFields.*")

    (transformationsOut, transformationsParams, transformationsParamsKO, transformationsParamsAdd)
  }

  def getSinks(sinks:DataFrame): (DataFrame, DataFrame) = {

    val sinksOut = sinks.select(
      explode(col("dataflows.sinks")).as("sinks")).select("sinks.*")

    val sinksPaths = sinks.select(
      explode(col("dataflows.sinks")).as("sinks_all"))
      .select("sinks_all.paths")

    (sinksOut, sinksPaths)
  }

}

