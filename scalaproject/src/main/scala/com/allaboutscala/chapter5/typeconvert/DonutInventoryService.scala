package com.allaboutscala.chapter5.typeconvert

trait DonutInventoryService[A] {

 def checkStockQuantity(donut: A): Int

}