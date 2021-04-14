package SPARKDIN

import SPARKDIN.Funciones._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{DataFrame, SparkSession}



object GestionReq {

  //Recuperamos el SparkSession
  val spark: SparkSession = SparkSession.builder.getOrCreate()

  import spark.implicits._

  //val pathReq(ruta local) = "file:///home/isamed/Desktop/TBetances_SDG/SDG-PRUEBA/src/main/scala/SDG/Requerimientos.json"
  //ruta docker
  val pathReq = "file:///opt/bitnami/TBetances_SDG/SDG-PRUEBA/src/main/scala/SDG/Requerimientos.json"

  val readJsonRequerimientos = spark.read.option("multiline", true).json(pathReq)



  val name = readJsonRequerimientos.select(explode($"dataflows").as("dataflows")).select($"dataflows.name")
    .rdd.map(r => r.getString(0))

  val sources = readJsonRequerimientos.select(explode($"dataflows").as("dataflows"))

  val sourcesName = getSources(sources).rdd.map(r => r.getString(1)).collect()(0)
  val sourcesPath = getSources(sources).rdd.map(r => r.getString(2)).collect()(0)
  val sourcesFormat = getSources(sources).rdd.map(r => r.getString(0)).collect()(0)


  // TRANSFORMACIONES 1

  val transfBK = readJsonRequerimientos.select(explode($"dataflows").as("dataflows"))

  val transfName1 =getTransformations(transfBK)._1.rdd.map(r => r.getString(0)).collect()(0)

  val transfType1 =getTransformations(transfBK)._1.rdd.map(r => r.getString(2)).collect()(0)

  val transfParamInput1 =getTransformations(transfBK)._2.rdd.map(r => r.getString(1)).collect()(0)

  val transfParamVColError1 =getTransformations(transfBK)._3.rdd.map(r => r.getString(0)).collect()(0)

  val transfParamVColError2 =getTransformations(transfBK)._3.rdd.map(r => r.getString(0)).collect()(1)

  val transfParamVTypeError1 =getTransformations(transfBK)._3.rdd.map(r => r.getString(1)).collect()(0)

  val transfParamVTypeError2 =getTransformations(transfBK)._3.rdd.map(r => r.getString(1)).collect()(1)

  val transfParamField1 =getTransformations(transfBK)._3.rdd.map(r => r.getString(2)).collect()(0)

  val transfParamField2 =getTransformations(transfBK)._3.rdd.map(r => r.getString(2)).collect()(1)

  val transfParamValid1 =getTransformations(transfBK)._3.rdd.map(r => r.getString(3)).collect()(0)

  val transfParamValid2 =getTransformations(transfBK)._3.rdd.map(r => r.getString(3)).collect()(1)


  // TRANSFORMACIONES 1 - Añadir columnas

  val transfName2 =getTransformations(transfBK)._1.rdd.map(r => r.getString(0)).collect()(1)

  val transfType2 =getTransformations(transfBK)._1.rdd.map(r => r.getString(2)).collect()(1)

  val transfParamInput2 =getTransformations(transfBK)._2.rdd.map(r => r.getString(1)).collect()(1)
  transfParamInput2.foreach(println)

  val transfParamAddName =getTransformations(transfBK)._4.rdd.map(r => r.getString(1)).collect()(0)
  transfParamAddName.foreach(println)

  val transfParamAddFunction =getTransformations(transfBK)._4.rdd.map(r => r.getString(0)).collect()(0)
  transfParamAddFunction.foreach(println)


  //SINKS
  //Variables de Sinks 1

  val sinksBK = readJsonRequerimientos.select(explode($"dataflows").as("dataflows"))

  val sinksInput1 =getSinks(sinksBK)._1.rdd.map(r => r.getString(1)).collect()(0)
  sinksInput1.foreach(println)

  val sinksName1 =getSinks(sinksBK)._1.rdd.map(r => r.getString(2)).collect()(0)
  sinksName1.foreach(println)

  val sinksPathsRes1 =getSinks(sinksBK)._2.rdd.map(r => r.getList(0)toArray).collect()(0)
  val sinksPaths1 = sinksPathsRes1 match { case Array(x: String, _*) => x }
  sinksPaths1.foreach(println)

  val sinksFormat1 =getSinks(sinksBK)._1.rdd.map(r => r.getString(0)).collect()(0)
  sinksFormat1.foreach(println)

  val sinksSaveMode1 =getSinks(sinksBK)._1.rdd.map(r => r.getString(4)).collect()(0).toLowerCase
  sinksSaveMode1.foreach(println)


  //Variables de Sinks 2

  val sinksInput2 =getSinks(sinksBK)._1.rdd.map(r => r.getString(1)).collect()(1)
  sinksInput2.foreach(println)

  val sinksName2 =getSinks(sinksBK)._1.rdd.map(r => r.getString(2)).collect()(1)
  sinksName2.foreach(println)

  val sinksPathsRes2 =getSinks(sinksBK)._2.rdd.map(r => r.getList(0)toArray).collect()(1)
  val sinksPaths2 = sinksPathsRes2 match { case Array(x: String, _*) => x }
  sinksPaths2.foreach(println)

  val sinksFormat2 =getSinks(sinksBK)._1.rdd.map(r => r.getString(0)).collect()(1)
  sinksFormat2.foreach(println)

  val sinksSaveMode2 =getSinks(sinksBK)._1.rdd.map(r => r.getString(4)).collect()(1).toLowerCase
  sinksSaveMode2.foreach(println)

}
