package com.edu.bigdata.product.model

case class AreaTop3Product(taskId:String,
                           area:String,
                           area_level:String,
                           click_product_id:Long,
                           city_infos:String,
                           click_count:Long,
                           product_name:String,
                           product_status:String)