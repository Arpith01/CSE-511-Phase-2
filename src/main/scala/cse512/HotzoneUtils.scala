package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
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
}
