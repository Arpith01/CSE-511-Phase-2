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
  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = 
  {
    if(queryRectangle == null || pointString == null || queryRectangle.isEmpty() || pointString.isEmpty())
      return false
    var rect = queryRectangle.split(",")
    var x1 = rect(0).trim.toDouble
    var y1 = rect(1).trim.toDouble
    var x2 = rect(2).trim.toDouble
    var y2 = rect(3).trim.toDouble

    var point = pointString.split(",")
    var p_x = point(0).trim.toDouble
    var p_y = point(1).trim.toDouble

    var min_x = math.min(x1, x2)
    var max_x = math.max(x1, x2)
    var min_y = math.min(y1, y2)
    var max_y = math.max(y1, y2)

    if(p_x >= min_x && p_x <= max_x && p_y >= min_y && p_y <= max_y){
      return true
    }
    return false
  }

  def CalculateWindow(min:Int, max:Int, point:Int): Int =
  {
    var min_p = math.min(min, max)
    var max_p = math.max(min, max)
    var offset = point - min_p
    var window = offset % (max_p - min_p)
    return min_p+window
  }

  def GetNeighbourCount(i:Double, mini:Double, maxi:Double, j:Double, minj:Double, maxj:Double, k:Double, mink:Double, maxk:Double): Int = 
  {
	var total  = 26
	if((i==mini || i== maxi) && (j==minj || j== maxj) && (k==mink || k== maxk))
		total-=19
	else if(((i==mini || i==maxi) && (j==minj || j==maxj)) || ((i==mini || i==maxi) && (k==mink || k==maxk)) || ((k==mink || k==maxk) &&(j==minj || j==maxj)))
		total-=15
	else if((i==mini || i==maxi) || (j==minj || j==maxj) || (k==mink || k==maxk))
		total-=9
	return total+1
  }
}
