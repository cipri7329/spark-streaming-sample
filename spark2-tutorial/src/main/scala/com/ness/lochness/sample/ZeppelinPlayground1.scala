package com.ness.lochness.sample

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by p3700703 on 10/21/16.
  */
object ZeppelinPlayground1 {

  def main(args:Array[String]) : Unit = {

    playground1()

  }


  def playground1() : Unit = {
    //    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master\\")

    val conf = new SparkConf().setAppName("zeppelinplayground1").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val moviesDump = sc.textFile("hdfs://rotsrlxdv01:8020/user/datalake/movies/ml-latest/movies.csv")

    case class Movie(movieId: Integer, title: String, genres: List[String])

    val movies = moviesDump.map(s => s.split(",")).filter(s => s(0) != "movieId")
      .map(
        s => Movie(s(0).toInt,
          s.slice(1, s.size - 1).mkString(""),
          s(s.size - 1).split('|').toList
        )
      )

    movies.take(3).foreach(m => println(m))
    val totalMovies = movies.count
    println(f"total movies found $totalMovies")


    val genresDup = movies.flatMap(m => m.genres)
    val genresUnique1 = genresDup.distinct()
    val totalGenres = genresUnique1.count
    println(f"total genres found $totalGenres")

    val mf = movies.map(m => ((m.movieId, m.title), m.genres))
    val mfm = mf.flatMapValues(x => x)


    val genre2Movie = mfm.map{
      case (key, value) => (value, key)
    }


    val genre2Count = mfm.map{
      case (key, value) => (value, 1)
    }

    println("=\n\n=")

    val counts = genre2Count.reduceByKey((x,y)=>x+y)
    val countedGenres = counts.count
    println(f"total genres found: $countedGenres")

    counts.collect().foreach(println)

    println("===")

    sc.stop()
  }

}
