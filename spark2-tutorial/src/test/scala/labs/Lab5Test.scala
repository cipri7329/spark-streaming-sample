package labs

/**
  * Created by p3700703 on 12/21/16.
  *
  * https://courses.bigdatauniversity.com/courses/course-v1:BigDataUniversity+BD0212EN+2016/
  * Spark Fundamentals II
  * Big Data University BD0212EN
  */
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite


class Lab5Test extends FunSuite with SharedSparkContext {

  test("distance calculation") {
    val start = (42.178, -82.556)
    val end = (42.229, -82.5528)

    val expected = 3.542615227517951

    assertResult(expected) {
      Lab5.distanceOf(start._1, start._2, end._1, end._2)
    }
  }

  test("join trips and stations and calculate distance") {



    val trips = sc.makeRDD(Seq(Trip(1, 1, "1/1/2015 1:00", "Start", 1, "1/1/2015", "End", 2, 1, "Subscriber", Some("1234"))))
    val stations = sc.broadcast(scala.collection.Map(1 -> Station(1, "Start", 42.178, -82.556, 1, "", ""), 2 -> Station(2, "End", 42.229, -82.5528, 1, "", "")))

    val expected = Array((1, 3.542615227517951))

    assertResult(expected) {
      Lab5.joinAndCalculateDistance(trips, stations).collect()
    }
  }
}
