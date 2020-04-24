package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  pickupInfo.createOrReplaceTempView("pickupInfo")
  pickupInfo = spark.sql("select x, y, z from pickupInfo where x >= " + minX + " and x <= " + maxX + " and y >= " + minY  + " and y <= " + maxY + " and z >= " + minZ + " and z <= " + maxZ)
  //givenPoints.show()
  pickupInfo = pickupInfo.groupBy("x", "y", "z").count()
  pickupInfo.show()
  pickupInfo.createOrReplaceTempView("cellCount")
  
  val sum = spark.sql("select sum(count) from cellCount").first().getLong(0).toDouble
  println(sum)
  val mean = sum / numCells.toDouble
  println(mean)

  val sumSquare = spark.sql("select sum(count*count) from cellCount").first().getLong(0).toDouble
  println(sumSquare)
  val deviation = math.sqrt(sumSquare / numCells - (mean * mean))
  println(deviation)
  spark.udf.register("st_Neighbors", (x: Double, y: Double, z: Double, x1: Double, y1: Double, z1: Double) => (HotcellUtils.st_Neighbors(x, y, z, x1, y1, z1)))
  var neighbors = spark.sql("select p1.x,p1.y,p1.z,sum(p2.count) as neighbors from cellCount as p1, cellCount as p2 where st_Neighbors(p1.x,p1.y,p1.z,p2.x,p2.y,p2.z) group by p1.x,p1.y,p1.z")
  neighbors.show()
  neighbors.createOrReplaceTempView("neighbors")


  def g_score(sum:Int): Double =
  {
    val numerator = sum-(mean*27)
    val denominator = deviation * math.sqrt(((numCells*27)-(27*27))/(numCells-1))
    val score = numerator/denominator
    return score
  }

  spark.udf.register("g_score", (sum: Int) => (g_score(sum)))
  var score = spark.sql("select x, y, z, g_score(neighbors) as gscore from neighbors order by gscore desc")
  score.show()
  return pickupInfo
}
}
