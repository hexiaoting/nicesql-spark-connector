package com.nicesql.spark.connector

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, TableScan}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

case class NicesqlRelation(location:String) (@transient val sqlContext: SQLContext) extends BaseRelation
  with TableScan {
  override def schema: StructType = StructType(Array(
    StructField("id", IntegerType, false),
    StructField("age", IntegerType,false)))

  override def buildScan(): RDD[Row] = {
    // Method1: Generate data directly
    //    sqlContext.sparkContext.parallelize(1 to 3).map(i=>Row(i,i*2))

    // Method2: read from file
//    val rdd = sqlContext
//      .sparkContext
//      .wholeTextFiles(location)
//      .map(x =>x._2)
//    val rows = rdd.map(file => {
//      val lines = file.split("\n")
//      //lines.map(line=>Row.fromSeq(Seq(line.split("#").apply(0),line.split("#").apply(1))))
//      //only read first line right now, How can read more?
//      Row.fromSeq(Seq(lines(0).split("#").apply(0), lines(0).split("#").apply(1)))
//    })
//    rows

    // Method3: new RDD
    new NiceSQLRDD(sqlContext.sparkContext,location)
  }
}


class DefaultSource extends RelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val location = parameters.getOrElse("path", sys.error("No path parameter"))
    NicesqlRelation(location)(sqlContext)
  }
}

object test{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TestNiceSql").config("spark.master", "local").getOrCreate()
    val df = spark.read.format("com.nicesql.spark.connector").option("path","/Users/hewenting/a/").load()
    df.printSchema()
    df.show()
  }
}