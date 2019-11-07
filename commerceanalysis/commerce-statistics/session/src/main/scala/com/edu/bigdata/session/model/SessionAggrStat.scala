package com.edu.bigdata.session.model

/**
  * Session 聚合统计表
  *
  * @param taskid                     当前计算批次的 ID
  * @param session_count              所有 Session 的总和
  * @param visit_length_1s_3s_ratio   1-3s Session 访问时长占比
  * @param visit_length_4s_6s_ratio   4-6s Session 访问时长占比
  * @param visit_length_7s_9s_ratio   7-9s Session 访问时长占比
  * @param visit_length_10s_30s_ratio 10-30s Session 访问时长占比
  * @param visit_length_30s_60s_ratio 30-60s Session 访问时长占比
  * @param visit_length_1m_3m_ratio   1-3m Session 访问时长占比
  * @param visit_length_3m_10m_ratio  3-10m Session 访问时长占比
  * @param visit_length_10m_30m_ratio 10-30m Session 访问时长占比
  * @param visit_length_30m_ratio     30m Session 访问时长占比
  * @param step_length_1_3_ratio      1-3 步长占比
  * @param step_length_4_6_ratio      4-6 步长占比
  * @param step_length_7_9_ratio      7-9 步长占比
  * @param step_length_10_30_ratio    10-30 步长占比
  * @param step_length_30_60_ratio    30-60 步长占比
  * @param step_length_60_ratio       大于 60 步长占比
  */
case class SessionAggrStat(taskid: String,
                           session_count: Long,
                           visit_length_1s_3s_ratio: Double,
                           visit_length_4s_6s_ratio: Double,
                           visit_length_7s_9s_ratio: Double,
                           visit_length_10s_30s_ratio: Double,
                           visit_length_30s_60s_ratio: Double,
                           visit_length_1m_3m_ratio: Double,
                           visit_length_3m_10m_ratio: Double,
                           visit_length_10m_30m_ratio: Double,
                           visit_length_30m_ratio: Double,
                           step_length_1_3_ratio: Double,
                           step_length_4_6_ratio: Double,
                           step_length_7_9_ratio: Double,
                           step_length_10_30_ratio: Double,
                           step_length_30_60_ratio: Double,
                           step_length_60_ratio: Double)