package com.ness.lochness.sample

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
  * Created by p3700703 on 10/21/16.
  */




//@serializable
case class MovieLog(path: String, country:String, name:String, mobile:Boolean, views:Int)

object ZeppelinPlayground2 {

  var sc : SparkContext = null

  def main(args:Array[String]) : Unit = {

    val conf = new SparkConf()
        .setAppName("submit-playground")
        .setMaster("local[2]")
//        .setMaster("yarn-client")
//        .set("spark.executor.cores", "3")
//        .set("spark.executor.memory", "6g")
//        .set("spark.dynamicAllocation.enabled", "true")
        .set("spark.shuffle.service.enabled", "true")
//        .set("spark.kryoserializer.buffer.max", "256m")
//        .set("yarn.nodemanager.resource.cpu-vcores", "3")
//        .set("yarn.nodemanager.resource.memory-mb", "6g")

    val spark = new SparkContext(conf)

    this.sc = spark

    printSparkConf()

    sample2()

    sc.stop()

  }

  def printSparkConf(): Unit = {
    val confs = sc.getConf.getAll

    confs.foreach(println)
  }

  def simpleConversionToDataFrame(): Unit ={

    val numList = List(1,2,3,4,5)
    val numRDD = sc.parallelize(numList)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val numDF = numRDD.toDF()

  }


  /**
    * sample load in dataframe from csv
    * */
  def sample2() : Unit = {

    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("hdfs://localhost:8020/user/datalake/movies/ml-latest/movies.csv")

    df.printSchema()
    df.show()

  }

  def sample1() : Unit = {


    val initialMap = mutable.HashMap[String, mutable.ArrayBuffer[MovieLog]]()
    val addToSet = (s: mutable.HashMap[String, mutable.ArrayBuffer[MovieLog]], m: MovieLog) => {
      s.getOrElseUpdate(m.country, mutable.ArrayBuffer.empty[MovieLog]) += m
      s
    }

    val mergePartitionSets = (p1: mutable.HashMap[String, mutable.ArrayBuffer[MovieLog]],
                              p2: mutable.HashMap[String, mutable.ArrayBuffer[MovieLog]]) => {
      val newMap = mutable.HashMap[String, mutable.ArrayBuffer[MovieLog]]()
      Seq(p1,p2).foreach(map => {
        map.foreach(e => {
          newMap += e._1 -> e._2
        })
      })

      newMap
    }


    val unionAll = obtainRDDUnion()


    val movieLimit = 10

    val res = unionAll.aggregate(initialMap)(addToSet,mergePartitionSets)
    initialMap.keySet.foreach(println)


    println(res.keySet)
  }


  def version1() : Unit = {

    val movieLimit = 10
    val countryResults = obtainRDDUnion
      .map(movieLog => (countryName(movieLog.country), movieLog.views))
      .reduceByKey((a, b) => a + b)
      .map(pair => (pair._2, pair._1))
      .sortByKey(false)
      .map(pair => (pair._2, pair._1))
      .take(movieLimit)

    println("========version-1-first=======")

    countryResults.foreach(println)

    println("========version-1-final=======")
  }








  def obtainInputFiles(): Array[String] = {
    /*    val path = new Path("/user/datalake/wiki-dump/")

        val fs = FileSystem.get(new Configuration())

        fs.listStatus(path)
          .filter(_.isDirectory)
          .flatMap(folder => fs.listStatus(folder.getPath)
          .filter(_.getPath.toString.endsWith(".combined")))
          .map(_.getPath.toString)*/
//    Array("/user/datalake/wiki-dump/processed-2016-01/*.combined")
    Array("/Users/p3700703/work/tmp/wiki-dump/processed-2016-01/*.combined")
  }

  def countryName(input:String):String = {
    val dot = input.indexOf(".")
    if(dot == -1) {
      return input
    }
    return input.substring(0, dot)
  }


  def obtainRdds(): Array[RDD[MovieLog]] = {
    obtainInputFiles().take(1).map(month => {
      val basePath = month

      sc.textFile(basePath).map(entry => {
        val components = entry.split(" ")
        val country = components(0)
        val name = components(1)
        val views = components(2).toInt
        val size = components(3).toInt
        val isMobile = country.endsWith(".m")
        MovieLog(basePath, country, name, isMobile, views)
      })
    })
  }


  def obtainRDDUnion(): RDD[MovieLog] = {
    sc.union(obtainRdds)
  }




}
