package SPARKDIN

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import SPARKDIN.Funciones._
import SPARKDIN.GestionReq._
import org.apache.spark.sql.types.StringType


object Proceso extends Estructura {

  override def dataflow(): Unit = {

    val spark: SparkSession = SparkSession
      .builder
      .appName("SDG-PRUEBA")
      .enableHiveSupport
      .getOrCreate()

    // ++ procesamiento

    println("====================")
    println(s"Ininciado Proceso: $name")
    println("====================")

    val readDF = spark.read.format(sourcesFormat).load("file://" + sourcesPath)

    val dfOK = readDF
      .where(transfParamField1 + " <> ''")
      .where(transfParamField2 + " IS NOT NULL")
      .withColumn(transfParamAddName, from_utc_timestamp(current_timestamp().cast(StringType), "Europe/Madrid").cast(StringType))

    println("dfOK")
    dfOK.show(20, truncate = false)

    val dfKOVacio = readDF
      .where(transfParamField1 + " = ''")
      .withColumn(transfParamVColError1, lit(transfParamVTypeError1))
      .withColumn(transfParamAddName, from_utc_timestamp(current_timestamp().cast(StringType), "Europe/Madrid").cast(StringType))

    println("dfKOVacio")
    dfKOVacio.show(20, truncate = false)

    val dfKONull = readDF
      .where(transfParamField2 + " IS NULL")
      .withColumn(transfParamVColError1, lit(transfParamVTypeError2))
      .withColumn(transfParamAddName, from_utc_timestamp(current_timestamp().cast(StringType), "Europe/Madrid").cast(StringType))

    println("dfKONull")
    dfKONull.show(20, truncate = false)

    val dfErrores = dfKOVacio.union(dfKONull)
    println("dfErrores")
    dfErrores.show(20, truncate = false)

    dfOK.repartition(1).write.format(sinksFormat1).mode(sinksSaveMode1).save("file://" + sinksPaths1)
    dfErrores.repartition(1).write.format(sinksFormat2).mode(sinksSaveMode2).save("file://" + sinksPaths2)

  }

  def main(args: Array[String]): Unit = {

    dataflow()

  }

}