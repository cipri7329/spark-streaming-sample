package labs

/**
  * Created by p3700703 on 12/21/16.
  */
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

object Lab5 {


  def joinAndCalculateDistance(trips: RDD[Trip], stations: Broadcast[Map[Int, Station]]) = {
    trips.map(t => {
      val start = stations.value.getOrElse(t.startTerminal, Station.default)
      val end = stations.value.getOrElse(t.endTerminal, Station.default)

      (t.id, distanceOf(start.lat, start.lon, end.lat, end.lon))
    })
  }


  def main(args: Array[String]) = {
    val sc = new SparkContext(new SparkConf().
      setAppName("lab5"))

    val output = args.length match {
      case 1 => args(0)
      case _ => "data/lab5/output"
    }

    val trips_input = sc.textFile("data/trips/*")
    val trips_header = trips_input.first
    val trips = trips_input.filter(_ != trips_header).map(_.split(",")).map(Trip.parse(_))

    val stations_input = sc.textFile("data/stations/*")
    val stations_header = stations_input.first
    val stations = stations_input.filter(_ != stations_header).map(_.split(",")).map(Station.parse(_)).keyBy((_.id)).collectAsMap()
    val bcstations = sc.broadcast(stations)

    val result = joinAndCalculateDistance(trips, bcstations)

    result.coalesce(1).saveAsTextFile(output)

    println("Press [Enter] to quit")
    Console.readLine()

    sc.stop
  }


  def main2(args: Array[String]) = {
    val sc = new SparkContext(new SparkConf().
      setAppName("lab5"))

    val trips = sc.textFile("data/trips/*")
    val stations = sc.textFile("data/stations/*")

    trips.count()
    stations.count()

    println("Press [Enter] to quit")
    Console.readLine()

    sc.stop
  }


  def distanceOf(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val earthRadius = 3963 - 13 * Math.sin(lat1)

    val dLat = Math.toRadians(lat2-lat1)
    val dLon = Math.toRadians(lon2-lon1)

    val a = Math.pow(Math.sin(dLat / 2), 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.pow(Math.sin(dLon / 2), 2)

    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a))

    val dist = earthRadius * c

    dist
  }

}


case class Trip(
                 id: Int,
                 duration: Int,
                 startDate: String,
                 startStation: String,
                 startTerminal: Int,
                 endDate: String,
                 endStation: String,
                 endTerminal: Int,
                 bike: Int,
                 subscriberType: String,
                 zipCode: Option[String]
               )

object Trip {
  def parse(i: Array[String]) = {
    val zip = i.length match {
      case 11 => Some(i(10))
      case _ => None
    }
    Trip(i(0).toInt, i(1).toInt, i(2), i(3), i(4).toInt, i(5), i(6), i(7).toInt, i(8).toInt, i(9), zip)
  }
}



case class Station(
                    id: Int,
                    name: String,
                    lat: Double,
                    lon: Double,
                    docks: Int,
                    landmark: String,
                    installDate: String
                  )

object Station {
  def parse(i: Array[String]) = {
    Station(i(0).toInt, i(1), i(2).toDouble, i(3).toDouble, i(4).toInt, i(5), i(6))
  }

  val default = Station(0, "None", 0, 0, 0, "", "")
}


