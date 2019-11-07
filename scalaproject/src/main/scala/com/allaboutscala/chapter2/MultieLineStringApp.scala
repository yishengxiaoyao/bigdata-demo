package com.allaboutscala.chapter2

object MultieLineStringApp {
  def main(args: Array[String]): Unit = {

    /**
      * 使用转义字符来获取json字符串
      */
    println("\nStep 2: Using backslash to escpae quotes")
    val donutJson2: String = "{\"donut_name\":\"Glazed Donut\",\"taste_level\":\"Very Tasty\",\"price\":2.50}"
    println(s"donutJson2 = $donutJson2")

    /**
      * 使用 """ 来转义字符
      */
    println("\nStep 3: Using triple quotes \"\"\" to escape characters")
    val donutJson3: String = """{"donut_name":"Glazed Donut","taste_level":"Very Tasty","price":2.50}"""
    println(s"donutJson3 = $donutJson3")

    /**
      * 多行使用stripMargin删除指定前缀，默认为|，可以自定义这个符号
      */
    println("\nStep 4:  Creating multi-line text using stripMargin")
    val donutJson4: String =
      """
        |{
        |"donut_name":"Glazed Donut",
        |"taste_level":"Very Tasty",
        |"price":2.50
        |}
      """
        .stripMargin
    println(s"donutJson4=$donutJson4")

    println("\nTip: stripMargin using a different character")
    val donutJson5: String =
      """
    #{
    #"donut_name":"Glazed Donut",
    #"taste_level":"Very Tasty",
    #"price":2.50
    #}
    """.stripMargin('#')
    println(s"donutJson5 = $donutJson4")
  }
}
