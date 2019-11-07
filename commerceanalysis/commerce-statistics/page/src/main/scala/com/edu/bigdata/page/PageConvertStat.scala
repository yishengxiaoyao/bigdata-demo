package com.edu.bigdata.page

import java.util.UUID

import com.edu.bigdata.common.conf.ConfigurationManager
import com.edu.bigdata.common.model.UserVisitAction
import com.edu.bigdata.common.util.{Constants, DateUtils, ParamUtils}
import com.edu.bigdata.page.model.PageSplitConvertRate
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

object PageConvertStat {
  def main(args: Array[String]): Unit = {
    // 获取过滤条件，【为了方便，直接从配置文件中获取，企业中会从一个调度平台获取】
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    // 获取过滤条件对应的 JsonObject 对象
    val taskParam = JSONObject.fromObject(jsonStr)

    // 创建全局唯一的主键，每次执行 main 函数都会生成一个独一无二的 taskUUID，来区分不同任务，作为写入 MySQL 数据库中那张表的主键
    val taskUUID = UUID.randomUUID().toString

    // 创建 sparkConf
    val sparkConf = new SparkConf().setAppName("pageConvert").setMaster("local[*]")

    // 创建 sparkSession（包含 sparkContext）
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // ******************** 需求五：页面单跳转化率统计 ********************

    // 获取原始的动作表数据（带有过滤条件）
    // actionRDD: RDD[UserVisitAction]
    val actionRDD = getOriActionRDD(sparkSession, taskParam)

    // 将用户行为信息转换为 K-V 结构
    // sessionId2ActionRDD: RDD[(sessionId, UserVisitAction)]
    val sessionId2ActionRDD = actionRDD.map(item => (item.session_id, item))

    // 将数据进行内存缓存
    sessionId2ActionRDD.persist(StorageLevel.MEMORY_ONLY)

    // 目标页面切片：将页面流路径转换为页面切片
    // targetPageFlowStr:"1,2,3,4,5,6,7"
    val targetPageFlowStr = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    // targetPageFlowArray: Array[Long][1,2,3,4,5,6,7]
    val targetPageFlowArray = targetPageFlowStr.split(",")
    // targetPageFlowArray.slice(0, targetPageFlowArray.length - 1): [1,2,3,4,5,6]
    // targetPageFlowArray.tail: [2,3,4,5,6,7]
    // targetPageFlowArray.slice(0, targetPageFlowArray.length - 1).zip(targetPageFlowArray.tail): [(1,2),(2,3),(3,4),(4,5),(5,6),(6,7)]
    val targetPageSplit = targetPageFlowArray.slice(0, targetPageFlowArray.length - 1).zip(targetPageFlowArray.tail).map {
      case (page1, page2) =>
        (page1 + "_" + page2)
    }

    // 获取实际页面切片
    // 对 <sessionId,访问行为> RDD，做一次 groupByKey 操作，生成页面切片
    val session2GroupActionRDD = sessionId2ActionRDD.groupByKey()

    // realPageSplitNumRDD: RDD[(String, 1L)]
    val realPageSplitNumRDD = session2GroupActionRDD.flatMap {
      case (sessionId, iterableUserVisitAction) =>
        // item1: UserVisitAction
        // item2: UserVisitAction
        // sortList: List[UserVisitAction] // 排好序的 UserVisitAction
        val sortList = iterableUserVisitAction.toList.sortWith((item1, item2) => {
          DateUtils.parseTime(item1.action_time).getTime < DateUtils.parseTime(item2.action_time).getTime
        })

        // 获取 page 信息
        // pageList: List[Long]
        val pageList = sortList.map {
          case userVisitAction =>
            userVisitAction.page_id
        }

        // pageList.slice(0, pageList.length - 1): List[1,2,3,...,N-1]
        // pageList.tail: List[2,3,4,...,N]
        // pageList.slice(0, pageList.length - 1).zip(pageList.tail): List[(1,2),(2,3),(3,4),...,(N-1,N)]
        val realPageSplit = pageList.slice(0, pageList.length - 1).zip(pageList.tail).map {
          case (page1, page2) =>
            (page1 + "_" + page2)
        }

        // 过滤：留下存在于 targetPageSplit 中的页面切片
        val realPageSplitFilter = realPageSplit.filter {
          case realPageSplit =>
            targetPageSplit.contains(realPageSplit)
        }

        realPageSplitFilter.map {
          case realPageSplitFilter =>
            (realPageSplitFilter, 1L)
        }
    }

    // 聚合
    // realPageSplitCountMap; Map[(page1_page2, count)]
    val realPageSplitCountMap = realPageSplitNumRDD.countByKey()

    realPageSplitCountMap.foreach(println(_))

    val startPage = targetPageFlowArray(0).toLong

    val startPageCount = sessionId2ActionRDD.filter {
      case (sessionId, userVisitAction) =>
        userVisitAction.page_id == startPage.toLong
    }.count()

    println("哈啊哈"+ startPageCount)

    // 得到最后的统计结果
    getPageConvertRate(sparkSession, taskUUID, targetPageSplit, startPageCount, realPageSplitCountMap)
  }

  // ******************** 需求五：页面单跳转化率统计 ********************

  /**
    * 计算页面切片转化率
    *
    * @param sparkSession
    * @param taskUUID
    * @param targetPageSplit
    * @param startPageCount
    * @param realPageSplitCountMap
    */
  def getPageConvertRate(sparkSession: SparkSession,
                         taskUUID: String,
                         targetPageSplit: Array[String],
                         startPageCount: Long,
                         realPageSplitCountMap: collection.Map[String, Long]): Unit = {
    val pageSplitRatioMap = new mutable.HashMap[String, Double]()

    var lastPageCount = startPageCount.toDouble

    // 1_2,2_3,3_4,...
    for (pageSplit <- targetPageSplit) {
      // 第一次循环：currentPageSplitCount: page1_page2   lastPageCount: page1
      // 第二次循环：currentPageSplitCount: page2_page3   lastPageCount: page1_page2
      val currentPageSplitCount = realPageSplitCountMap.get(pageSplit).get.toDouble
      val rate = currentPageSplitCount / lastPageCount
      pageSplitRatioMap.put(pageSplit, rate)
      lastPageCount = currentPageSplitCount
    }

    val convertRateStr = pageSplitRatioMap.map {
      case (pageSplit, rate) =>
        pageSplit + "=" + rate
    }.mkString("|")


    // 封装数据
    val pageSplitConvertRate = PageSplitConvertRate(taskUUID, convertRateStr)

    val pageSplitConvertRateRDD = sparkSession.sparkContext.makeRDD(Array(pageSplitConvertRate))

    // 写入到 MySQL
    import sparkSession.implicits._
    pageSplitConvertRateRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "page_split_convert_rate")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }

  /**
    * 根据日期范围获取对象的用户行为数据
    *
    * @param sparkSession
    * @param taskParam
    * @return
    */
  def getOriActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {
    // 先获取所用到的过滤条件：开始日期 和 结束日期
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    // 把所有的时间范围在 startDate 和 endDate 之间的数据查询出来
    val sql = "select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'"

    // 在对 DataFrame 和 Dataset 进行许多操作都需要这个包进行支持
    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd // DataFrame(Row类型) -> DataSet(样例类类型) -> rdd(样例类)
  }

}
