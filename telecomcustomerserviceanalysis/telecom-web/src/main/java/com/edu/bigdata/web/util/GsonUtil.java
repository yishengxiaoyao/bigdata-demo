package com.edu.bigdata.web.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;

/**
 * 封装返回数据的方法
 *
 * @author lwc
 */
public class GsonUtil {

  /**
   * 添加私有构造函数，否则sona会报错
   */
  private GsonUtil() {

  }

  /**
   * 接口调用成功时返回数据
   *
   * @param data 返回数据的对象
   */

  public static ResponseEntity<String> getResult(Object data, HttpStatus status) {
    return getResult(data, status,"no error");
  }

  public static ResponseEntity<String> getResult(Object data, HttpStatus status,String msg) {
    Map<String, Object> map = new HashMap<>();
    map.put("data", data);
    map.put("msg", msg);
    Gson gson = new Gson();
    return new ResponseEntity<>(gson.toJson(map), status);
  }


  public static String getResultWithNull(Object data) {
    Gson gson = new GsonBuilder().serializeNulls().create();
    return gson.toJson(data);
  }


  public static void returnPreHandlerResult(HttpServletResponse response, HttpStatus httpStatus) throws Exception{
    response.setStatus(httpStatus.value());
    ResponseEntity<String> result = GsonUtil.getResult(null, httpStatus,"no data");
    response.getWriter().write(result.getBody());
    response.getWriter().flush();
    response.getWriter().close();
  }
}