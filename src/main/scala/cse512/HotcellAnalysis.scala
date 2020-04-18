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

  pickupInfo = spark.sql("select x,y,z from pickupInfoView where x>= " + minX + " and x<= " + maxX + " and y>= " + minY + " and y<= " + maxY + " and z>= " + minZ + " and z<= " + maxZ + " order by z,y,x")
  pickupInfo.createOrReplaceTempView("cells")
  pickupInfo = spark.sql("select x, y, z, count(*) as pickups from cells group by x, y, z order by z,y,x")
  pickupInfo.createOrReplaceTempView("cellHotness")
  
  val totalHotness = spark.sql("select sum(pickups) as sumHotCells from cellHotness")
  totalHotness.createOrReplaceTempView("totalHotness")
 
  val sumOfSquares = spark.sql("select sum((pickups*pickups).toDouble) as sumOfSquares from cellHotness")
  sumOfSquares.createOrReplaceTempView("sumOfSquares")
  
  val avg = (totalHotness.head().getLong(0).toDouble / numCells.toDouble).toDouble     //////////

  standardDeviation = HotcellUtils.calculateStandardDeviation(sumOfSquares, numCells, avg)
  //val standardDeviation = scala.math.sqrt(((sumOfSquares.first().getDouble(0).toDouble / numCells.toDouble) - (avg.toDouble * avg.toDouble))).toDouble
  
  spark.udf.register("numValidNeighbours", (x: Int, y: Int, z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int) => ((HotcellUtils.calculateNumNeighbours(x, y, z, minX, minY, minZ, maxX, maxY, maxZ))))
  
  spark.udf.register("isNeighbour", (x1: Int, x2: Int, x3: Int, y1: Int, y2: Int, y3: Int, minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int) => (HotcellUtils.isNeighbour(x1,x2,x3,y1,y2,y3,minX,minY,minZ,maxX,maxY,maxZ)))
  
  val neighbours = spark.sql("select first.x as x, first.y as y, first.z as z, sum(second.pickups) as totalPickups, numValidNeighbours(first.x,first.y,first.z) as numNeighbours from totalHotness first cross join totalHotness second where isNeighbour(first.x,first.y,first.z,second.x,second.y,second.z) group by first.x,first.y,first.z").persist()
  

  // val adjacentCells = spark.sql("select adjacentCells(sch1.x, sch1.y, sch1.z, " + minX + "," + maxX + "," + minY + "," + maxY + "," + minZ + "," + maxZ + ") as adjacentCellCount,"
  //     + "sch1.x as x, sch1.y as y, sch1.z as z, "
  //     + "sum(sch2.numCells) as sumHotCells "
  //     + "from cellHotness as sch1, cellHotness as sch2 "
  //     + "where (sch2.x = sch1.x+1 or sch2.x = sch1.x or sch2.x = sch1.x-1) "
  //     + "and (sch2.y = sch1.y+1 or sch2.y = sch1.y or sch2.y = sch1.y-1) "
  //     + "and (sch2.z = sch1.z+1 or sch2.z = sch1.z or sch2.z = sch1.z-1) "
  //     + "group by sch1.z, sch1.y, sch1.x "
  //     + "order by sch1.z, sch1.y, sch1.x")
  neighbours.createOrReplaceTempView("neighbours")
  // adjacentCells.show()
    
  spark.udf.register("gScore", (numNeighbours: Int, sumHotCells: Int, numCells: Int, x: Int, y: Int, z: Int, avg: Double, standardDeviation: Double) => ((HotcellUtils.calculateGScore(numNeighbours, sumHotCells, numCells, x, y, z, avg, standardDeviation))))
    
  pickupInfo = spark.sql("select gScore(numNeighbours, totalPickups, " + numCells + ", x, y, z," + avg + ", " + standardDeviation + ") as getisOrdStatistic, x, y, z from neighbours order by getisOrdStatistic desc");
  pickupInfo.createOrReplaceTempView("result")
  // pickupInfo.show()
    
  pickupInfo = spark.sql("select x, y, z from result")
  pickupInfo.createOrReplaceTempView("finalPickupInfo")
  // pickupInfo.show()

  return pickupInfo
}
}
