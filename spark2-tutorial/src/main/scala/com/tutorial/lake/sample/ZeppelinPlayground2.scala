package com.tutorial.lake.sample

import java.net.URI

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext }
import org.apache.spark.SparkContext._

import scala.collection.mutable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

/**
  * Created by p3700703 on 10/21/16.
  */


object ExtractYear {

  val regexpr1 = """\(\d{4}\)""".r
  val regexpr2 = """\d{4}""".r

  def apply(title: String): Int = {
    regexpr2.findFirstIn(
      regexpr1.findFirstIn(title).getOrElse("0000"))
      .getOrElse("0").toInt
  }

}

object RemoveYear {

  val regexpr1 = """\(\d{4}\)""".r
  val regexpr2 = """\d{4}""".r

  def apply(title: String): String = {

    val value = regexpr1.findFirstIn(title)

    var cleanedTitle = title
    if(value.isDefined){
        val year = value.get
        cleanedTitle = cleanedTitle.replaceAll("\\("+ year + "\\)", "").replaceAll("\"", "").trim
    }

    val parenthesisIndex = cleanedTitle.indexOf("(")
    if(parenthesisIndex > 0){
      cleanedTitle = cleanedTitle.substring(0, parenthesisIndex).trim
    }

    if(cleanedTitle.endsWith(" The")){
      cleanedTitle = cleanedTitle.substring(0, cleanedTitle.size - 4).trim
      cleanedTitle = "The " + cleanedTitle
    }

    cleanedTitle.trim
  }
}

object FormatTitle {

  def apply(title: String): String = {

    var title2 = title.trim
    title2 = title2.replaceAll(" ", "-")
    title2
  }
}



//@serializable
case class MovieLog(path: String, country:String, name:String, mobile:Boolean, views:Int)

case class MovieItem(
                      source : String,  //0
                      id : Int,     //1
                      title : String,   //2
                      ftitle : String,  //3
                      year : Int,   //4
                      views : Int,  //5
                      genres : List[String], //6
                      domain : String, //7
                      mobile : Boolean, //8
                      reviews : Int, //9
                      rating : Double //10
                    )

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

//    printSparkConf()

    movieLensSample()

//    movieWikiSample()

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





  def movieLensSample() : Unit = {

    val moviesDump = sc.textFile("hdfs://localhost:8020/user/datalake/movies/ml-latest/movies.csv")

    val items = moviesDump.map(s => s.split(",")).filter(s => s(0)!="movieId")
      .map(
        s => movieLensItem(s)
      )

    items.take(13).foreach(m => println(m))

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val movieLensDF = items.toDF()

    movieLensDF.printSchema()
    movieLensDF.show()


    movieLensDF.filter($"id" > 715 && $"id" < 740).foreach(println)

//    movieLensDF.write.mode(SaveMode.Overwrite).parquet("hdfs://localhost:8020/user/datalake/movies/ml-latest/movieitems-lens.parquet")


//    val movieLensDF2 = sqlContext.read.parquet("hdfs://localhost:8020/user/datalake/movies/ml-latest/movieitems-lens.parquet")

//    movieLensDF2.printSchema()
//    movieLensDF2.show()
//
//    val ratings = movieLensRatings()
//
//    val joined = movieLensDF2.join(ratings, movieLensDF2("movieid") === ratings("movieid"))
//
//
//    joined.show(30)


  }


  def movieLensItem(s: Array[String]) : MovieItem = {
    MovieItem(
      "movie-lens", //source
      s(0).toInt, //id
      RemoveYear(s.slice(1, s.length-1).map(s => s.toLowerCase()).mkString("")), //title
      FormatTitle(RemoveYear(s.slice(1, s.length-1).map(s => s.toLowerCase()).mkString(""))), //ftitle
      ExtractYear(s.slice(1, s.length-1).mkString("")), //year
      0, //views
      s(s.length-1).split('|').toList, //genres
      "", //domain
      false,
      0,
      0
    )
  }

  def movieLensRatings() : DataFrame = {
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    hc.sql("use movieslake")
    val st = hc.sql("show tables")
    st.show()

    val moviesRatings = hc.sql("""
        select count(movies_ratings.userid) as reviews, movies_ratings.movieid, movies.title from movies_ratings
        join movies on movies.movieid = movies_ratings.movieid
        where movies_ratings.movieid is not NULL
        group by movies_ratings.movieid, movies.title
                               """)

    moviesRatings.show()

    moviesRatings
  }

  def movieWikiSample() : Unit = {

    val rddArray = obtainWikiMovieItemsRDDs()

    rddArray.foreach(rdd => rdd.take(10).foreach(println))


//    val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//
//    val movieLensDF = items.toDF()
//
//    movieLensDF.printSchema()
//    movieLensDF.show()
//
//
//    movieLensDF.write.mode(SaveMode.Overwrite).parquet("hdfs://localhost:8020/user/datalake/movies/ml-latest/movieitems-lens.parquet")
//
//
//    val movieLensDF2 = sqlContext.read.parquet("hdfs://localhost:8020/user/datalake/movies/ml-latest/movieitems-lens.parquet")
//
//    movieLensDF2.printSchema()
//    movieLensDF2.show()

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
       val path = new Path("/user/datalake/wiki-dump/")

        val fs = FileSystem.get(new URI("hdfs://localhost:8020"), new Configuration())

        fs.listStatus(path)
          .filter(_.isDirectory)
          .flatMap(folder => fs.listStatus(folder.getPath)
          .filter(_.getPath.toString.endsWith(".combined")))
          .map(_.getPath.toString)

//    Array("/user/datalake/wiki-dump/processed-2016-01/*.combined")
//    Array("hdfs://localhost:8020/user/datalake/wiki-dump/processed-2016-01/*.combined",
//      "hdfs://localhost:8020/user/datalake/wiki-dump/processed-2016-02/*.combined",
//      "hdfs://localhost:8020/user/datalake/wiki-dump/processed-2016-03/*.combined"
//    )
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


  def obtainWikiMovieItemsRDDs(): Array[RDD[MovieItem]] = {
    obtainInputFiles().map(month => {
      val basePath = month
      val regexpr3 = """\d{4}-\d{2}""".r
      val processedDate = regexpr3.findFirstIn(basePath).getOrElse("none")

      sc.textFile(basePath).map(entry => {
        val components = entry.split(" ")
        val name = components(1)

        val regexpr4 = """_\([a-z]*\)""".r
        val genreDirty = regexpr4.findFirstIn(name).getOrElse("_()")
        val genre = genreDirty.substring(2, genreDirty.length-1)

        val titleDirty = name.replaceAll("_\\("+ genre + "\\)", "").trim
        val ftitle = titleDirty.replaceAll("_", "-")
        val title = titleDirty.replaceAll("_", " ")

        val country = components(0)
        val views = components(2).toInt
        val size = components(3).toInt

        val isMobile = country.endsWith(".m")
        val reviews = 0
        val rating = 0

        MovieItem("wiki-dump-"+processedDate, 0, title, ftitle, 0, views, List[String](genre), country, isMobile, reviews, rating)
      })
    })
  }



  def obtainRDDUnion(): RDD[MovieLog] = {
    sc.union(obtainRdds)
  }




}
