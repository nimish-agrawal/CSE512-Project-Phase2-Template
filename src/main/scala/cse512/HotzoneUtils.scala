package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {


    val rectPoints = queryRectangle.split(",")
    val rec_x1 = rectPoints(0).trim().toDouble
    val rec_y1 = rectPoints(1).trim().toDouble
    val rec_x2 = rectPoints(2).trim().toDouble
    val rec_y2 = rectPoints(3).trim().toDouble

    val testPoints = pointString.split(",")
    val pt_x = testPoints(0).trim().toDouble
    val pt_y = testPoints(1).trim().toDouble

    if(rec_x1 < rec_x2){
      if(rec_y1 < rec_y2){
        if(pt_x < rec_x1 || pt_x > rec_x2){
          return false
        }
        if(pt_y < rec_y1 || pt_y > rec_y2){
          return false
        }
      }
      else{
        if(pt_x < rec_x1 || pt_x > rec_x2){
          return false
        }
        if(pt_y > rec_y1 || pt_y < rec_y2){
          return false
        }
      }
    }

    else{
      if(rec_y1 < rec_y2){
        if(pt_x > rec_x1 || pt_x < rec_x2){
          return false
        }
        if(pt_y < rec_y1 || pt_y > rec_y2){
          return false
        }
      }
      else{
        if(pt_x > rec_x1 || pt_x < rec_x2){
          return false
        }
        if(pt_y > rec_y1 || pt_y < rec_y2){
          return false
        }
      }
    }

    return true
  }

}
