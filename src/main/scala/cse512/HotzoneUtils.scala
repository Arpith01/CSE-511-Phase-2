package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    //print(queryRectangle)
    //print(pointString)
    // YOU NEED TO CHANGE THIS PART
    if (queryRectangle == null || queryRectangle.isEmpty() || pointString == null || pointString.isEmpty() || queryRectangle.split(",").length < 4 || pointString.split(",").length < 2){

	return false

    }
	
    var rect =  queryRectangle.split(",")
    // Below logic may not work all the time, again assumtions from phase1     

    /*
    var rectX1 = rect(0).toDouble
    var rectY1 = rect(1).toDouble
    var rectX2 = rect(2).toDouble
    var rectY2 = rect(3).toDouble
    */ 
    
    var rectX1 = Math.min(rect(0).toDouble, rect(2).toDouble)
    var rectY1 = Math.min(rect(1).toDouble, rect(3).toDouble)
    var rectX2 = Math.max(rect(0).toDouble, rect(2).toDouble)
    var rectY2 = Math.max(rect(1).toDouble, rect(3).toDouble)
    
    var point = pointString.split(",")
    var pointX = point(0).toDouble
    var pointY = point(1).toDouble

    /*
    if (x >= x1 && x <= x2 && y >= y1 && y <= y2)
            return true
        else if (x >= x2 && x <= x1 && y >= y2 && y <= y1)
            return true
        else
            return false
    */
   
    //Better to code for negative cases than finding for positive cases as in the code above
    if (pointX < rectX1 || pointX > rectX2 || pointY < rectY1 || pointY > rectY2) {
      return false
    }

     
    return true // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART

}
