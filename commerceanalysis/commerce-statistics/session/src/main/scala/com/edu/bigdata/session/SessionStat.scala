package com.edu.bigdata.session

import java.util.{Date, Random, UUID}

import com.edu.bigdata.common.conf.ConfigurationManager
import com.edu.bigdata.common.model.{UserInfo, UserVisitAction}
import com.edu.bigdata.common.util._
import com.edu.bigdata.session.accumulator.SessionStatisticAccumulator
import com.edu.bigdata.session.model._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object SessionStat {
  def main(args: Array[String]): Unit = {
    // 获取过滤条件，【为了方便，直接从配置文件中获取，企业中会从一个调度平台获取】
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    // 获取过滤条件对应的 JsonObject 对象
    val taskParam = JSONObject.fromObject(jsonStr)
    // 创建全局唯一的主键，每次执行 main 函数都会生成一个独一无二的 taskUUID，来区分不同任务，作为写入 MySQL 数据库中那张表的主键
    val taskUUID = UUID.randomUUID().toString
    // 创建 sparkConf
    val sparkConf = new SparkConf().setAppName("session").setMaster("local[*]")

    // 创建 sparkSession（包含 sparkContext）
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 获取原始的动作表数据（带有过滤条件）
    // actionRDD: RDD[UserVisitAction]
    val actionRDD = getOriActionRDD(sparkSession, taskParam)

    // 将用户行为信息转换为 K-V 结构，sessionId2ActionRDD: RDD[(sessionId, UserVisitAction)]
    val sessionId2ActionRDD = actionRDD.map(item => (item.session_id, item))

    // session2GroupActionRDD: RDD[(sessionId, Iterable[UserVisitAction])]
    val session2GroupActionRDD = sessionId2ActionRDD.groupByKey() // 把同一个 sessionId 的数据聚合到一起，得到斧子形数据

    // 将数据进行内存缓存
    session2GroupActionRDD.cache()

    // sessionId2FullAggrInfoRDD: RDD[(sessionId, fullAggrInfo)]
    val sessionId2FullAggrInfoRDD = getSessionFullAggrInfo(sparkSession, session2GroupActionRDD)

    // 创建自定义累加器对象
    val sessionStatisticAccumulator = new SessionStatisticAccumulator()

    // 在 sparkSession 中注册自定义累加器，这样后面就可以用了
    sparkSession.sparkContext.register(sessionStatisticAccumulator)

    // 根据过滤条件对 sessionId2FullAggrInfoRDD 进行过滤操作，即过滤掉不符合条件的数据，并根据自定义累加器 统计不同范围的 访问时长 和 访问步长 的 session 个数 以及 总的 session 个数
    val seeionId2FilterRDD = getSessionFilterRDD(taskParam, sessionId2FullAggrInfoRDD, sessionStatisticAccumulator)

    // 必须引入任意一个 action 的算子，才能启动
    seeionId2FilterRDD.foreach(println(_))

    // 计算各个 session 的占比
    getSessionRatio(sparkSession,taskUUID, sessionStatisticAccumulator.value)

    // ******************** 需求二：Session 随机抽取 ********************

    // sessionId2FullAggrInfoRDD: RDD[(sessionId, fullAggrInfo)]，注意：到这里一个 sessionId 对应一条数据，也就是一个 fullAggrInfo
    sessionRandomExtract(sparkSession, taskUUID, seeionId2FilterRDD)

    // ******************** 需求三：Top10 热门品类统计 ********************

    // sessionId2ActionRDD: RDD[(sessionId, UserVisitAction)]
    // seeionId2FilterRDD: RDD[(sessionId, fullAggrInfo)]

    // join 默认是内连接，即不符合条件的不显示（即被过滤掉）

    // 获取所有符合过滤条件的原始的 UserVisitAction 数据
    val seeionId2ActionFilterRDD = sessionId2ActionRDD.join(seeionId2FilterRDD).map {
      case (sessionId, (userVisitAction, fullAggrInfo)) =>
        (sessionId, userVisitAction)
    }

    val top10CategoryArray = top10PopularCategories(sparkSession, taskUUID, seeionId2ActionFilterRDD)


    // ******************** 需求四：Top10 热门品类的 Top10 活跃 Session 统计 ********************

    // seeionId2ActionFilterRDD: RDD[(sessionId, UserVisitAction)]
    // top10CategoryArray: Array[(sortKey, fullCountInfo)]

    top10ActiveSession(sparkSession, taskUUID, seeionId2ActionFilterRDD, top10CategoryArray)

  }

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

  /**
    * 将用户的信息添加到当前对象
    * @param sparkSession
    * @param session2GroupActionRDD
    * @return
    */
  def getSessionFullAggrInfo(sparkSession: SparkSession,
                             session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    // userId2PartAggrInfoRDD: RDD[(userId, partAggrInfo)]
    val userId2PartAggrInfoRDD = session2GroupActionRDD.map {
      // 使用模式匹配：当结果是 KV 对的时候尽量使用 case 模式匹配，这样更清楚，更简洁直观
      case (sessionId, iterableAction) =>
        var userId = -1L

        var startTime: Date = null
        var endTime: Date = null

        var stepLength = 0 // 有多少个 action

        val searchKeywords = new StringBuffer("") // 搜索行为
        val clickCategories = new StringBuffer("") // 点击行为

        for (action <- iterableAction) {
          //将user_id赋值
          if (userId == -1) {
            userId = action.user_id
          }

          val actionTime = DateUtils.parseTime(action.action_time) // action_time = "2019-05-30 18:17:11" 是字符串类型
          if (startTime == null || startTime.after(actionTime)) { // startTime 在 actionTime 的后面   正常区间：[startTime, actionTime, endTime]
            startTime = actionTime
          }

          if (endTime == null || endTime.before(actionTime)) { // endTime 在 actionTime 的前面
            endTime = actionTime
          }

          val searchKeyword = action.search_keyword
          if (StringUtils.isNotEmpty(searchKeyword) && !searchKeywords.toString.contains(searchKeyword)) {
            searchKeywords.append(searchKeyword + ",")
          }

          val clickCategoryId = action.click_category_id
          if (clickCategoryId != -1 && !clickCategories.toString.contains(clickCategoryId)) {
            clickCategories.append(clickCategoryId + ",")
          }

          stepLength += 1
        }

        // searchKeywords.toString.substring(0, searchKeywords.toString.length - 1) // 等价于下面
        val searchKw = StringUtils.trimComma(searchKeywords.toString) // 去除最后一个逗号
        val clickCg = StringUtils.trimComma(clickCategories.toString) // 去除最后一个逗号

        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        // 拼装聚合数据的字符串：
        // (31,sessionid=7291cc307f96432f8da9d926fd7d88e5|searchKeywords=洗面奶,小龙虾,机器学习,苹果,华为手机|clickCategoryIds=11,93,36,66,
        // 60|visitLength=3461|stepLength=43|startTime=2019-05-30 14:01:01)
        val partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime) // 格式化时间为字符串类型

        (userId, partAggrInfo)
    }

    // user_visit_action 表联立 user_info 表，让我们的统计数据中具有用户属性
    val sql = "select * from user_info"

    import sparkSession.implicits._
    // userId2InfoRDD: RDD[(userId, UserInfo)]
    val userId2InfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))

    val sessionId2FullAggrInfoRDD = userId2PartAggrInfoRDD.join(userId2InfoRDD).map {
      case (userId, (partAggrInfo, userInfo)) =>
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city

        // 拼装最终的聚合数据字符串：
        val fullAggrInfo = partAggrInfo + "|" +
          Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city

        val seesionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID)

        (seesionId, fullAggrInfo)
    }

    sessionId2FullAggrInfoRDD
  }

  /**
    *
    * @param taskParam
    * @param sessionId2FullAggrInfoRDD
    * @param sessionStatisticAccumulator
    * @return
    */
  def getSessionFilterRDD(taskParam: JSONObject,
                          sessionId2FullAggrInfoRDD: RDD[(String, String)],
                          sessionStatisticAccumulator: SessionStatisticAccumulator) = {
    // 先获取所用到的过滤条件：
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    // 拼装过滤条件的字符串：
    var filterInfo = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

    // 去除过滤条件字符串末尾的 "|"
    if (filterInfo.endsWith("\\|"))
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)

    // 进行过滤操作（过滤自带遍历功能）
    sessionId2FullAggrInfoRDD.filter {
      case (sessionId, fullAggrInfo) =>
        var success = true

        // 如果 age 不在过滤条件范围之内，则当前 sessionId 对应的 fullAggrInfo 数据被过滤掉
        if (!ValidUtils.between(fullAggrInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) { // 范围用 between
          success = false
        } else if (!ValidUtils.in(fullAggrInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
          success = false
        } else if (!ValidUtils.in(fullAggrInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
          success = false
        } else if (!ValidUtils.equal(fullAggrInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) { // 二选一用 equal
          success = false
        } else if (!ValidUtils.in(fullAggrInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)) { // 多选一用 in
          success = false
        } else if (!ValidUtils.in(fullAggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constants.PARAM_CATEGORY_IDS)) {
          success = false
        }

        // 自定义累加器，统计不同范围的 访问时长 和 访问步长 的个数 以及 总的 session 个数
        if (success) {
          sessionStatisticAccumulator.add(Constants.SESSION_COUNT) // 总的 session 个数

          // 获取当前 sessionId 对应的 访问时长 和 访问步长
          val visitLength = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong

          // 统计不同范围的 访问时长 和 访问步长 的个数
          calculateVisitLength(visitLength, sessionStatisticAccumulator)
          calculateStepLength(stepLength, sessionStatisticAccumulator)
        }

        success
    }
  }

  def calculateVisitLength(visitLength: Long, sessionStatisticAccumulator: SessionStatisticAccumulator) = {
    if (visitLength >= 1 && visitLength <= 3) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    } else if (visitLength >= 4 && visitLength <= 6) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    } else if (visitLength >= 7 && visitLength <= 9) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLength > 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30m)
    }
  }

  def calculateStepLength(stepLength: Long, sessionStatisticAccumulator: SessionStatisticAccumulator) = {
    if (stepLength >= 1 && stepLength <= 3) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_1_3)
    } else if (stepLength >= 4 && stepLength <= 6) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  /**
    * 计算session的比例
    * @param sparkSession
    * @param taskUUID
    * @param value
    */
  def getSessionRatio(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {
    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    // 先获取各个值
    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    // 计算比例
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    // 封装数据
    val stat = SessionAggrStat(taskUUID, session_count.toInt,
      visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    // 样例类实例 -> 数组 -> RDD
    val sessionRatioRDD = sparkSession.sparkContext.makeRDD(Array(stat))

    // 写入 MySQL 数据库中
    import sparkSession.implicits._
    sessionRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_aggr_stat")
      .mode(SaveMode.Append) // 表存在就追加，表不存在就新建
      .save()
  }


  // ******************** 需求二：Session 随机抽取 ********************
  /**
    * Session 随机抽取
    *
    * @param sparkSession
    * @param taskUUID
    * @param seeionId2FilterRDD
    */
  def sessionRandomExtract(sparkSession: SparkSession, taskUUID: String, seeionId2FilterRDD: RDD[(String, String)]): Unit = {
    // 由于是按照 时间 为 key 进行聚合，所以先将 seeionId2FilterRDD 的 key 转化为 时间

    // dateHour2FullAggrInfoRDD: RDD[(dateHour, fullAggrInfo)]
    val dateHour2FullAggrInfoRDD = seeionId2FilterRDD.map {
      case (sessionId, fullAggrInfo) =>
        // 先从 fullAggrInfo 中提取出来 startTime
        val startTime = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_START_TIME)
        // 得到的 startTime = "2019-05-30 18:17:11" 是字符串类型，需要转换成我们需要的格式：yyyy-MM-dd_HH
        val dateHour = DateUtils.getDateHour(startTime)

        (dateHour, fullAggrInfo)
    }

    // hourCountMap: Map[(dateHour, count)]，示例：(yyyy-MM-dd_HH, 20)
    val hourCountMap = dateHour2FullAggrInfoRDD.countByKey()

    // dateHourCountMap: Map[data, Map[(hour, count)]]，示例：(yyyy-MM-dd, (HH, 20))
    val dateHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()

    for ((dateHour, count) <- hourCountMap) {
      val date = dateHour.split("_")(0) // yyyy-MM-dd_HH
      val hour = dateHour.split("_")(1) // HH

      dateHourCountMap.get(date) match { // Map[(hour, count)
        case None =>
          dateHourCountMap(date) = new mutable.HashMap[String, Long]() // 先创建 1 个空的 HashMap
          dateHourCountMap(date) += (hour -> count) // 再给 HashMap 赋值
        case Some(map) =>
          dateHourCountMap(date) += (hour -> count) // 直接给 HashMap 赋值
      }
    }

    // 解决问题一：
    //   一共有多少天：dateHourCountMap.size
    //   一天抽取多少条：1000 / dateHourCountMap.size
    val extractPerDay = 1000 / dateHourCountMap.size

    // 解决问题二：
    //   一共有多少个：session：dateHourCountMap(date).values.sum
    //   一个小时有多少个：session：dateHourCountMap(date)(hour)

    val dateHourExtractIndexListMap = new mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]()

    // dateHourCountMap: Map[data, Map[(hour, count)]]，示例：(yyyy-MM-dd, (HH, 20))
    // hourCountMap: Map[(hour, count)]，示例：(HH, 20) ，注意：这里面的 hourCountMap 含义发生变化了，要跟上面的最开始的 hourCountMap 区别开来
    for ((date, hourCountMap) <- dateHourCountMap) {
      // 一天共有多少个 session
      val dataCount = hourCountMap.values.sum

      dateHourExtractIndexListMap.get(date) match {
        case None =>
          dateHourExtractIndexListMap(date) = new mutable.HashMap[String, mutable.ListBuffer[Int]]()
          generateRandomIndexList(extractPerDay, dataCount, hourCountMap, dateHourExtractIndexListMap(date))
        case Some(map) =>
          generateRandomIndexList(extractPerDay, dataCount, hourCountMap, dateHourExtractIndexListMap(date))
      }
    }

    // 到此为止，我们获得了每个小时要抽取的 session 的 index
    // 之后在算子中使用 dateHourExtractIndexListMap 这个 Map，由于这个 Map 可能会很大，所以涉及到 广播大变量 的问题

    // 广播变量，提升任务 task 的性能
    val dateHourExtractIndexListMapBroadcastVar = sparkSession.sparkContext.broadcast(dateHourExtractIndexListMap)

    // dateHour2FullAggrInfoRDD: RDD[(dateHour, fullAggrInfo)]
    // dateHour2GroupRDD: RDD[(dateHour, Iterable[fullAggrInfo])]
    val dateHour2GroupRDD = dateHour2FullAggrInfoRDD.groupByKey()

    // extractSessionRDD: RDD[SessionRandomExtract]
    val extractSessionRDD = dateHour2GroupRDD.flatMap {
      case (dateHour, iterableFullAggrInfo) =>
        val date = dateHour.split("_")(0)
        val hour = dateHour.split("_")(1)

        val extractIndexList = dateHourExtractIndexListMapBroadcastVar.value.get(date).get(hour)

        // 创建一个容器存储抽取的 session
        val extractSessionArrayBuffer = new ArrayBuffer[SessionRandomExtract]()
        var index = 0

        for (fullAggrInfo <- iterableFullAggrInfo) {
          if (extractIndexList.contains(index)) {
            // 提取数据，封装成所需要的样例类，并追加进 ArrayBuffer 中
            val sessionId = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_SESSION_ID)
            val startTime = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_START_TIME)
            val searchKeywords = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
            val clickCategoryIds = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)

            val sessionRandomExtract = SessionRandomExtract(taskUUID, sessionId, startTime, searchKeywords, clickCategoryIds)

            extractSessionArrayBuffer += sessionRandomExtract
          }
          index += 1
        }

        extractSessionArrayBuffer
    }

    // 将抽取后的数据保存到 MySQL
    import sparkSession.implicits._
    extractSessionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "session_random_extract")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }

  /**
    * 根据每个小时应该抽取的数量，来产生随机值
    *
    * @param extractPerDay 一天抽取的 seesion 个数
    * @param dataCount     当天所有的 seesion 总数
    * @param hourCountMap  每个小时的session总数
    * @param hourListMap   主要用来存放生成的随机值
    */
  def generateRandomIndexList(extractPerDay: Long,
                              dataCount: Long,
                              hourCountMap: mutable.HashMap[String, Long],
                              hourListMap: mutable.HashMap[String, mutable.ListBuffer[Int]]): Unit = {
    // 先遍历 hourCountMap，hourCountMap: Map[(hour, count)]，示例：(HH, 20) ，注意：这里面的 hourCountMap 含义发生变化了，要跟上面的最开始的 hourCountMap 区别开来
    for ((hour, count) <- hourCountMap) {
      // 计算一个小时抽取多少个 session
      var hourExtractCount = ((count / dataCount.toDouble) * extractPerDay).toInt
      // 避免一个小时要抽取的数量超过这个小时的总数
      if (hourExtractCount > count) {
        hourExtractCount = count.toInt
      }

      val random = new Random()

      hourListMap.get(hour) match {
        case None =>
          hourListMap(hour) = new mutable.ListBuffer[Int] // 没有 List，需要新建一个 List
          for (i <- 0 until hourExtractCount) {
            var index = random.nextInt(count.toInt) // 生成 index
            while (hourListMap(hour).contains(index)) { // 如果 index 已存在
              index = random.nextInt(count.toInt) // 则重新生成 index
            }

            // 将生成的 index 放入到 hourListMap 中
            hourListMap(hour).append(index)
          }

        case Some(list) =>
          for (i <- 0 until hourExtractCount) {
            var index = random.nextInt(count.toInt) // 生成 index
            while (hourListMap(hour).contains(index)) { // 如果 index 已存在
              index = random.nextInt(count.toInt) // 则重新生成 index
            }

            // 将生成的 index 放入到 hourListMap 中
            hourListMap(hour).append(index)
          }
      }
    }

  }

  /**
    * Top10 热门品类统计
    *
    * @param sparkSession
    * @param taskUUID
    * @param seeionId2ActionFilterRDD 所有符合过滤条件的原始的 UserVisitAction 数据
    */
  def top10PopularCategories(sparkSession: SparkSession, taskUUID: String, seeionId2ActionFilterRDD: RDD[(String, UserVisitAction)]) = {
    // 第一步：获取所有发生过点击、下单、付款的 categoryId，注意：其中被点击的 categoryId 只有一个，被下单和被付款的 categoryId 有多个，categoryId 之间使用逗号隔开的
    var cid2CidRDD = seeionId2ActionFilterRDD.flatMap {
      case (sessionId, userVisitAction) =>
        val categoryIdBuffer = new ArrayBuffer[(Long, Long)]()

        // 提取出数据填充 ArrayBuffer
        if (userVisitAction.click_category_id != -1) { // 点击行为
          categoryIdBuffer += ((userVisitAction.click_category_id, userVisitAction.click_category_id)) // 只有第一个 key 有用，第二个 value 任何值都可以，但是不可以没有
        } else if (userVisitAction.order_category_ids != null) { // 下单行为
          for (order_category_id <- userVisitAction.order_category_ids.split(",")) {
            categoryIdBuffer += ((order_category_id.toLong, order_category_id.toLong))
          }
        } else if (userVisitAction.pay_category_ids != null) { // 付款行为
          for (pay_category_id <- userVisitAction.pay_category_ids.split(",")) {
            categoryIdBuffer += ((pay_category_id.toLong, pay_category_id.toLong))
          }
        }

        categoryIdBuffer
    }

    // 第二步：进行去重操作
    cid2CidRDD = cid2CidRDD.distinct()

    // 第三步：统计各品类 被点击的次数、被下单的次数、被付款的次数
    val cid2ClickCountRDD = getClickCount(seeionId2ActionFilterRDD)
    val cid2OrderCountRDD = getOrderCount(seeionId2ActionFilterRDD)
    val cid2PayCountRDD = getPayCount(seeionId2ActionFilterRDD)

    // 第四步：获取各个 categoryId 的点击次数、下单次数、付款次数，并进行拼装
    // cid2FullCountRDD: RDD[(cid, aggrCountInfo)]
    // (81,categoryId=81|clickCount=68|orderCount=64|payCount=72)
    val cid2FullCountRDD = getFullCount(cid2CidRDD, cid2ClickCountRDD, cid2OrderCountRDD, cid2PayCountRDD)

    // 第五步：根据点击次数、下单次数、付款次数依次排序，会用到 【二次排序】，实现自定义的二次排序的 key

    // 第六步：封装 SortKey
    val sortKey2FullCountRDD = cid2FullCountRDD.map {
      case (cid, fullCountInfo) =>
        val clickCount = StringUtils.getFieldFromConcatString(fullCountInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(fullCountInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(fullCountInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong

        val sortKey = SortKey(clickCount, orderCount, payCount)
        (sortKey, fullCountInfo)
    }

    // 第七步：降序排序，取出 top10 热门品类
    val top10CategoryArray = sortKey2FullCountRDD.sortByKey(false).take(10)

    // 第八步：将 Array 结构转化为 RDD，封装 Top10Category
    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10CategoryArray).map {
      case (sortKey, fullCountInfo) =>
        val categoryid = StringUtils.getFieldFromConcatString(fullCountInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        val clickCount = sortKey.clickCount
        val orderCount = sortKey.orderCount
        val payCount = sortKey.payCount

        Top10Category(taskUUID, categoryid, clickCount, orderCount, payCount)
    }

    // 第九步：写入 MySQL 数据库
    import sparkSession.implicits._
    top10CategoryRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "top10_category")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

    top10CategoryArray
  }

  /**
    *
    * @param cid2CidRDD
    * @param cid2ClickCountRDD
    * @param cid2OrderCountRDD
    * @param cid2PayCountRDD
    * @return
    */
  def getFullCount(cid2CidRDD: RDD[(Long, Long)],
                   cid2ClickCountRDD: RDD[(Long, Long)],
                   cid2OrderCountRDD: RDD[(Long, Long)],
                   cid2PayCountRDD: RDD[(Long, Long)]) = {
    // 左外连接：不符合添加显示为空（null）

    // 4.1 所有品类id 和 被点击的品类 做左外连接
    val cid2ClickInfoRDD = cid2CidRDD.leftOuterJoin(cid2ClickCountRDD).map {
      case (cid, (categoryId, option)) =>
        val clickCount = if (option.isDefined) option.get else 0
        val aggrCountInfo = Constants.FIELD_CATEGORY_ID + "=" + cid + "|" + Constants.FIELD_CLICK_COUNT + "=" + clickCount

        (cid, aggrCountInfo)
    }
    // 4.2 4.1 的结果 和 被下单的品类 做左外连接
    val cid2OrderInfoRDD = cid2ClickInfoRDD.leftOuterJoin(cid2OrderCountRDD).map {
      case (cid, (clickInfo, option)) =>
        val orderCount = if (option.isDefined) option.get else 0
        val aggrCountInfo = clickInfo + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount

        (cid, aggrCountInfo)
    }
    // 4.3 4.2 的结果 和 被付款的品类 做左外连接
    val cid2PayInfoRDD = cid2OrderInfoRDD.leftOuterJoin(cid2PayCountRDD).map {
      case (cid, (orderInfo, option)) =>
        val payCount = if (option.isDefined) option.get else 0
        val aggrCountInfo = orderInfo + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount

        (cid, aggrCountInfo)
    }

    cid2PayInfoRDD
  }

  /**
    * 统计各品类被点击的次数
    *
    * @param seeionId2ActionFilterRDD
    */
  def getClickCount(seeionId2ActionFilterRDD: RDD[(String, UserVisitAction)]) = {
    // 方式一：把发生过点击的 action 过滤出来
    val clickActionFilterRDD = seeionId2ActionFilterRDD.filter {
      case (sessionId, userVisitAction) =>
        userVisitAction.click_category_id != 1L
    }
    // 方式二：把发生点击的 action 过滤出来，二者等价
    // val clickActionFilterRDD2 = seeionId2ActionFilterRDD.filter(item => item._2.click_category_id != -1L)

    // 获取每种类别的点击次数
    val clickNumRDD = clickActionFilterRDD.map {
      case (sessionId, userVisitAction) =>
        (userVisitAction.click_category_id, 1L)
    }
    // 计算各个品类的点击次数
    clickNumRDD.reduceByKey(_ + _)
  }

  /**
    * 统计各品类被下单的次数
    *
    * @param seeionId2ActionFilterRDD
    */
  def getOrderCount(seeionId2ActionFilterRDD: RDD[(String, UserVisitAction)]) = {
    // 把发生过下单的 action 过滤出来
    val orderActionFilterRDD = seeionId2ActionFilterRDD.filter {
      case (sessionId, userVisitAction) =>
        userVisitAction.order_category_ids != null
    }
    // 获取每种类别的下单次数
    val orderNumRDD = orderActionFilterRDD.flatMap {
      case (sessionId, userVisitAction) =>
        userVisitAction.order_category_ids.split(",").map(item => (item.toLong, 1L))
    }
    // 计算各个品类的下单次数
    orderNumRDD.reduceByKey(_ + _)
  }

  /**
    * 统计各品类被付款的次数
    *
    * @param seeionId2ActionFilterRDD
    */
  def getPayCount(seeionId2ActionFilterRDD: RDD[(String, UserVisitAction)]) = {
    // 把发生过付款的 action 过滤出来
    val payActionFilterRDD = seeionId2ActionFilterRDD.filter {
      case (sessionId, userVisitAction) =>
        userVisitAction.pay_category_ids != null
    }
    // 获取每种类别的支付次数
    val payNumRDD = payActionFilterRDD.flatMap {
      case (sessionId, userVisitAction) =>
        userVisitAction.pay_category_ids.split(",").map(item => (item.toLong, 1L))
    }
    // 计算各个品类的支付次数
    payNumRDD.reduceByKey(_ + _)
  }

  // ******************** 需求四：Top10 热门品类的 Top10 活跃 Session 统计 ********************

  /**
    * Top10 热门品类的 Top10 活跃 Session 统计
    *
    * @param sparkSession
    * @param taskUUID
    * @param seeionId2ActionFilterRDD
    * @param top10CategoryArray
    */
  def top10ActiveSession(sparkSession: SparkSession,
                         taskUUID: String,
                         seeionId2ActionFilterRDD: RDD[(String, UserVisitAction)],
                         top10CategoryArray: Array[(SortKey, String)]): Unit = {
    // 第一步：获取所有点击过 Top10 热门品类的 UserVisitAction
    // 第一种方法：Join 方法，该方式需要引起 Shuffle，比较麻烦

    /*
    // 将 top10CategoryArray 转化为 RDD，然后将其 key sortKey 转化为 cid
    val cid2FullCountInfoRDD = sparkSession.sparkContext.makeRDD(top10CategoryArray).map {
      case (sortKey, fullCountInfo) =>
        // 取出 categoryId
        val cid = StringUtils.getFieldFromConcatString(fullCountInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        // 返回所需的 RDD
        (cid, fullCountInfo)
    }

    // 将 seeionId2ActionFilterRDD 的 key sessionId 转化为 cid，对其使用 map 操作即可
    val cid2ActionRDD = seeionId2ActionFilterRDD.map {
      case (sessionId, userVisitAction) =>
        val cid = userVisitAction.click_category_id
        (cid, userVisitAction)
    }

    // joinn 操作（即内连接）：两边都有的才留下，否则过滤掉
    cid2FullCountInfoRDD.join(cid2ActionRDD).map {
      case (cid, (fullCountInfo, userVisitAction)) =>
        val sid = userVisitAction.session_id
        (sid, userVisitAction)
    }*/

    // 第二种方法：使用 filter
    // cidArray: Array[Long] 包含了 Top10 热门品类的 id
    val cidArray = top10CategoryArray.map {
      case (sortKey, fullCountInfo) =>
        val cid = StringUtils.getFieldFromConcatString(fullCountInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        cid
    }

    // 所有符合过滤条件的，并且点击过 Top10 热门品类的 UserVisitAction
    val seeionId2ActionRDD = seeionId2ActionFilterRDD.filter {
      case (sessionId, userVisitAction) =>
        cidArray.contains(userVisitAction.click_category_id)
    }

    // 第二步：先对 所有符合过滤条件的，并且点击过 Top10 热门品类的 UserVisitAction 按照 sessionId 进行聚合
    val seeionId2GroupRDD = seeionId2ActionRDD.groupByKey()


    // 第三步：统计 每一个 sessionId 对于点击过的每一个品类的点击次数
    // cid2SessionCountRDD: RDD[(cid, sessionN=sessionCount)]
    val cid2SessionCountRDD = seeionId2GroupRDD.flatMap {
      case (sessionId, iterableUserVisitAction) =>
        // 创建 Map，用于保存当前每一个 sessionId 对于点击过的每一个品类的点击次数
        val categoryCountMap = new mutable.HashMap[Long, Long]()

        for (userVisitAction <- iterableUserVisitAction) {
          val cid = userVisitAction.click_category_id
          if (!categoryCountMap.contains(cid))
            categoryCountMap += (cid -> 0)

          categoryCountMap.update(cid, categoryCountMap(cid) + 1)
        }

        // 该 Map 记录了一个 session 对于它所有点击过的品类的点击次数
        // categoryCountMap

        for ((cid, sessionCount) <- categoryCountMap)
          yield (cid, sessionId + "=" + sessionCount)
    }

    // 第四步：对 cid2SessionCountRDD 进行聚合
    // cid2GroupRDD: RDD[(cid, Iterable[sessionN=sessionCount]))]
    // cid2GroupRDD 的每一条数据都是一个 cid 和它对应的所有点击过它的 sessionId 对它的点击次数
    val cid2GroupRDD = cid2SessionCountRDD.groupByKey()

    // 第五步：取出 top10SessionRDD: RDD[Top10Session]
    val top10SessionRDD = cid2GroupRDD.flatMap {
      case (cid, iterablesSessionCount) =>
        val sortList = iterablesSessionCount.toList.sortWith((item1, item2) => { // true: item1 放在前面
          item1.split("=")(1).toLong > item2.split("=")(1).toLong // item1: sessionCount 字符串类型 sessionIdN=count
        }).take(10)

        // 封装数据，准备写入 MySQL 数据库
        val top10Session = sortList.map {
          case item => {
            val categoryid = cid
            val sessionid = item.split("=")(0)
            val clickCount = item.split("=")(1).toLong

            Top10Session(taskUUID, categoryid, sessionid, clickCount)
          }
        }

        top10Session
    }

    // 写入 MySQL 数据库
    import sparkSession.implicits._
    top10SessionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_session")
      .mode(SaveMode.Append)
      .save()
  }

}
