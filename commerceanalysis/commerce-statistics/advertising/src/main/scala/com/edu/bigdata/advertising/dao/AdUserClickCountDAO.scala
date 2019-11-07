package com.edu.bigdata.advertising.dao

import com.edu.bigdata.advertising.model.AdUserClickCount

object AdUserClickCountDAO {
  def updateBatch(adUserClick:Array[AdUserClickCount]): Unit ={

  }

  def findClickCountByMultiKey(date:String, userid:Long, adid:Long): Long ={
    0L
  }

}
