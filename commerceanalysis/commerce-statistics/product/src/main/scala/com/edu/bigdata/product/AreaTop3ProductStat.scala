package com.edu.bigdata.product

import java.util.UUID

import com.edu.bigdata.common.conf.ConfigurationManager
import com.edu.bigdata.common.util.{Constants, ParamUtils}
import com.edu.bigdata.product.model.{AreaTop3Product, CityAreaInfo, CityClickProduct}
import com.edu.bigdata.product.udf.GroupConcatDistinct
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object AreaTop3ProductStat {
  def main(args: Array[String]): Unit = {
    // 获取过滤条件，【为了方便，直接从配置文件中获取，企业中会从一个调度平台获取】
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    // 获取过滤条件对应的 JsonObject 对象
    val taskParam = JSONObject.fromObject(jsonStr)

    // 创建全局唯一的主键，每次执行 main 函数都会生成一个独一无二的 taskUUID，来区分不同任务，作为写入 MySQL 数据库中那张表的主键
    val taskUUID = UUID.randomUUID().toString

    // 创建 sparkConf
    val sparkConf = new SparkConf().setAppName("product").setMaster("local[*]")

    // 创建 sparkSession（包含 sparkContext）
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // ******************** 需求六：各区域 Top3 商品统计 ********************

    // 获取 用户访问行为的数据 (cityId, clickProductId)
    // cityId2ProductIdRDD: RDD[(cityId, clickProductId)]
    val cityId2ProductIdRDD = getCityAndProductInfo(sparkSession, taskParam)

    // 获取 城市信息 (城市 ID，城市名称，区域名称)
    // cityId2AreaInfoRDD: RDD[(cityId, CityAreaInfo)]
    val cityId2AreaInfoRDD = getCityAreaInfo(sparkSession)

    // 做 Join 操作，得到 (city_id, city_name, area, click_product_id)
    // 临时表 temp_area_product_info 中的一条数据就代表一次点击商品的行为
    getAreaProductIdBasicInfoTable(sparkSession, cityId2ProductIdRDD, cityId2AreaInfoRDD)

    // 自定义弱类型的聚合函数（UDAF）：实现字符串带去重的拼接
    sparkSession.udf.register("concat_long_string", (v1: Long, v2: String, split: String) => {
      v1 + split + v2
    })
    sparkSession.udf.register("group_concat_distinct", new GroupConcatDistinct)

    // 统计 每一个区域里每一个商品被点击的次数，得到 (area, click_product_id, click_count, city_infos)
    getAreaProductClickCountTable(sparkSession)

    // 自定义 UDAF 函数：实现从 json 串中取出指定字段的值
    sparkSession.udf.register("get_json_field", (json: String, field: String) => {
      val jsonObject = JSONObject.fromObject(json)
      jsonObject.getString(field)
    })

    // 将 temp_area_product_count 表 join 商品信息表 product_info
    getAreaProductClickCountInfo(sparkSession)

    // 获取 各区域 Top3 商品（使用到了开窗函数）
    getAreaTop3Product(sparkSession, taskUUID)

    // 测试
    // sparkSession.sql("select * from temp_area_product_info").show
    // sparkSession.sql("select * from temp_area_product_count").show
    // sparkSession.sql("select * from temp_area_count_product_info").show
    // sparkSession.sql("select * from temp_test").show
  }

  /**
    * 获取 各区域 Top3 商品（使用了开窗函数）
    *
    * @param sparkSession
    * @param taskUUID
    * @return
    */
  def getAreaTop3Product(sparkSession: SparkSession, taskUUID: String) = {
    //    val sql = "select area, city_infos, click_product_id, click_count, product_name, product_status, " +
    //      "row_number() over(partition by area order by click_count desc) row_number from temp_area_count_product_info"
    //    sparkSession.sql(sql).createOrReplaceTempView("temp_test") // 测试

    val sql = "select area, " +
      "case " +
      "when area='华北' or area='华东' then 'A_Level' " +
      "when area='华中' or area='华男' then 'B_Level' " +
      "when area='西南' or area='西北' then 'C_Level' " +
      "else 'D_Level' " +
      "end area_level, " +
      "city_infos, click_product_id, click_count, product_name, product_status from (" +
      "select area, city_infos, click_product_id, click_count, product_name, product_status, " +
      "row_number() over(partition by area order by click_count desc) row_number from temp_area_count_product_info) t where row_number <= 3"
    val areaTop3ProductRDD = sparkSession.sql(sql).rdd.map {
      case row =>
        AreaTop3Product(taskUUID, row.getAs[String]("area"), row.getAs[String]("area_level"),
          row.getAs[Long]("click_product_id"), row.getAs[String]("city_infos"),
          row.getAs[Long]("click_count"), row.getAs[String]("product_name"),
          row.getAs[String]("product_status"))
    }

    import sparkSession.implicits._
    areaTop3ProductRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "area_top3_product")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }

  /**
    * 将 temp_area_product_count 表 join 商品信息表
    *
    * @param sparkSession
    */
  def getAreaProductClickCountInfo(sparkSession: SparkSession): Unit = {
    // temp_area_product_count: (area, click_product_id, click_count, city_infos) tapc
    // product_info: (product_id, product_name, extend_info)  pi
    val sql = "select tapc.area, tapc.city_infos, tapc.click_product_id, tapc.click_count, pi.product_name, " +
      "if (get_json_field(pi.extend_info, 'product_status') = '0', 'Self', 'Third Party') product_status" +
      " from temp_area_product_count tapc join product_info pi on tapc.click_product_id = pi.product_id"

    sparkSession.sql(sql).createOrReplaceTempView("temp_area_count_product_info")
  }

  /**
    * 统计 每一个区域里每一个商品被点击的次数
    */
  def getAreaProductClickCountTable(sparkSession: SparkSession): Unit = {
    val sql = "select area, click_product_id, count(*) click_count," +
      " group_concat_distinct(concat_long_string(city_id, city_name, ':')) city_infos" +
      " from temp_area_product_info group by area, click_product_id"

    sparkSession.sql(sql).createOrReplaceTempView("temp_area_product_count")
  }

  /**
    * 将 用户访问行为的数据 (cityId, clickProductId) 和 城市信息 (城市 ID，城市名称，区域名称) 做 join 操作，得到所需的临时表数据
    *
    * @param sparkSession
    * @param cityId2ProductIdRDD
    * @param cityId2AreaInfoRDD
    */
  def getAreaProductIdBasicInfoTable(sparkSession: SparkSession,
                                     cityId2ProductIdRDD: RDD[(Long, Long)],
                                     cityId2AreaInfoRDD: RDD[(Long, CityAreaInfo)]): Unit = {
    val areaProductIdBasicInfoRDD = cityId2ProductIdRDD.join(cityId2AreaInfoRDD).map {
      case (cityId, (clickProductId, cityAreaInfo)) =>
        (cityId, cityAreaInfo.city_name, cityAreaInfo.area, clickProductId)
    }

    import sparkSession.implicits._
    // 转换为临时表的时候需要指定字段的名称
    areaProductIdBasicInfoRDD.toDF("city_id", "city_name", "area", "click_product_id").createOrReplaceTempView("temp_area_product_info")
  }

  /**
    * 获取 城市信息（城市 ID，城市名称，区域名称）
    *
    * @param sparkSession
    */
  def getCityAreaInfo(sparkSession: SparkSession) = {
    val cityAreaInfoArray = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"),
      (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"),
      (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"),
      (9L, "哈尔滨", "东北"))

    // RDD[(cityId, CityAreaInfo)]
    sparkSession.sparkContext.makeRDD(cityAreaInfoArray).map {
      case (cityId, cityName, area) =>
        (cityId, CityAreaInfo(cityId, cityName, area))
    }
  }

  /**
    * 获取 用户访问行为的数据 (city_id, click_product_id)
    *
    * @param sparkSession
    * @param taskParam
    * @return
    */
  def getCityAndProductInfo(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    // 只获取发生过点击的 action 的数据，获取到的一条 action 数据就代表一个点击行为
    val sql = "select city_id, click_product_id from user_visit_action where date>='" + startDate +
      "' and date<='" + endDate + "' and click_product_id != -1"

    import sparkSession.implicits._
    sparkSession.sql(sql).as[CityClickProduct].rdd.map {
      case cityIdAndProductId =>
        (cityIdAndProductId.city_id, cityIdAndProductId.click_product_id)
    }
  }

}
