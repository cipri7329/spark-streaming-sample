package com.ness.lochness.sample

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

/**
  * Created by p3700703 on 10/21/16.
  */




//@serializable
case class MovieLog(path: String, country:String, name:String, mobile:Boolean, views:Int)

object ZeppelinPlayground2 {

  var sc : SparkContext = null

  def main(args:Array[String]) : Unit = {

    val conf = new SparkConf().setAppName("zeppelin2")
    val spark = new SparkContext(conf)

    this.sc = spark



    version1()

    sc.stop()

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
    Array("/user/datalake/wiki-dump/processed-2016-01/*.combined")
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
