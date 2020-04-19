package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // YOU NEED TO CHANGE THIS PART
//   def calculateStandardDeviation(sumOfSquares: Double, numCells: Double, avg: Double) : Double =
//   {
//     return scala.math.sqrt(((sumOfSquares.first().getDouble(0).toDouble / numCells.toDouble) - (avg.toDouble * avg.toDouble))).toDouble;
//   }

  def calculateNumNeighbours(x: Int, y: Int, z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int): Int =
  {
    
    // corner  
    if ((x == minX || x == maxX) && (y == minY || y == maxY) && (z == minZ || z == maxZ)){
      return 7;
    }

    // centre
    if ((x != minX && x != maxX) && (y != minY && y != maxY) && (z != minZ && z != maxZ)){
      return 26;
    }

    // edge
    if (((x == minX || x == maxX) && (y == minY || y == maxY))){
        return 11;
    }
    if (((x == minX || x == maxX) && (z == minZ || z == maxZ))){
        return 11;
    }
    if (((y == minY || y == maxY) && (z == minZ || z == maxZ))){
        return 11;
    }
     
    // face
    return 17;
  }

  // checks if two cells are neighbours
  def isNeighbour(x1: Int, y1: Int, z1: Int, x2: Int, y2: Int, z2: Int, minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int) : Boolean = 
  {
    if (x2 > maxX || x2 < minX || y2 < minY || y2 > maxY || z2 > maxZ || z2 < minZ){
      return false;
      } 
    //else if ((x1 - 1 == x2 || x1 == x2 || x1 + 1 == x2) && (y1 - 1 == y2 || y1 == y2 || y2 == y1 + 1) && (z1 == z2 || z2 == z1 - 1 || z2 == z1 + 1)) 
    else if((x1-x2).abs <= 1 && (y1-y2).abs <= 1 && (z1-z2).abs <= 1){
      return true;
      }
    else{
      return false;
    }
  }


  def calculateGScore(numNeighnours: Int, sumHotCells: Int, numCells: Int, x: Int, y: Int, z: Int, mean: Double, standardDeviation: Double): Double =
  {

    val sumWijXj = sumHotCells.toDouble
    val sumWij = numNeighnours.toDouble
    val root = math.sqrt((((numCells.toDouble * sumWij) - (sumWij * sumWij)) / (numCells.toDouble - 1.0).toDouble).toDouble).toDouble

    return (sumWijXj - (mean*sumWij)) / (standardDeviation*root)
  }
}
