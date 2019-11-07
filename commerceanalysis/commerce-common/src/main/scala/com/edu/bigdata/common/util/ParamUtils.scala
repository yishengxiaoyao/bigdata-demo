package com.edu.bigdata.common.util

import net.sf.json.JSONObject


object ParamUtils {
  /**
    * 从 JSON 对象中提取参数
    *
    * @param jsonObject JSON对象
    * @return 参数
    */
  def getParam(jsonObject: JSONObject, field: String): String = {
    jsonObject.getString(field)
  }
}
