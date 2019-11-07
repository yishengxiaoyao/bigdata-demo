package com.allaboutscala.chapter2

object ForComprehensionApp {
  def main(args: Array[String]): Unit = {
    //forExpression()
    //rangeMethod()
    //convertRangeToCollection()
    loopMethod()
  }

  def forExpression(): Unit ={
    forLoopTo()
    forLoopUntil()
    forIfCondition()
    forIfConditionToList()
    dimensionalArray()
  }

  def forLoopTo(): Unit ={
    //左闭右闭
    println("Step 1: A simple for loop from 1 to 5 inclusive")
    for(numberOfDonuts <- 1 to 5){
      println(s"Number of donuts to buy = $numberOfDonuts")
    }
  }

  def forLoopUntil(): Unit ={
    //左闭右开
    println("\nStep 2: A simple for loop from 1 to 5, where 5 is NOT inclusive")
    for(numberOfDonuts <- 1 until 5){
      println(s"Number of donuts to buy = $numberOfDonuts")
    }
  }

  def forIfCondition(): Unit ={
    //直接后面跟上if语句
    println("\nStep 3: Filter values using if conditions in for loop")
    val donutIngredients = List("flour", "sugar", "egg yolks", "syrup", "flavouring")
    for(ingredient <- donutIngredients if ingredient == "sugar"){
      println(s"Found sweetening ingredient = $ingredient")
    }
  }

  def forIfConditionToList(): Unit ={
    println("\nStep 4: Filter values using if conditions in for loop and return the result back using the yield keyword")
    val donutIngredients = List("flour", "sugar", "egg yolks", "syrup", "flavouring")

    /**
      *  如果使用yield关键字,将结果写入到一个List中
      */
    val sweeteningIngredients = for {
      ingredient <- donutIngredients
      if (ingredient == "sugar" || ingredient == "syrup")
    } yield ingredient
    println(s"Sweetening ingredients = $sweeteningIngredients")
  }

  def dimensionalArray(){
    val twoDimensionalArray = Array.ofDim[String](2,2)
    twoDimensionalArray(0)(0) = "flour"
    twoDimensionalArray(0)(1) = "sugar"
    twoDimensionalArray(1)(0) = "egg"
    twoDimensionalArray(1)(1) = "syrup"

    /**
      * 如果在多维数组中，在循环的过程中，需要使用下面的方式来循环，需要使用{
      *  }
      */
    for { x <- 0 until 2
          y <- 0 until 2
    } println(s"Donut ingredient at index ${(x,y)} = ${twoDimensionalArray(x)(y)}")
  }

  def rangeMethod(): Unit ={
    rangeFromTo()
    rangeFromUtil()
    rangeByStep()
    rangeAToZ()
    rangeAToZByStep()
  }

  def rangeFromTo(): Unit ={
    /**
      * 左闭右闭
      */
    println("Step 1: Create a simple numeric range from 1 to 5 inclusive")
    val from1To5 = 1 to 5
    println(s"Range from 1 to 5 inclusive = $from1To5")
  }

  def rangeFromUtil(): Unit ={
    /**
      * 左闭右开
      */
    println("\nStep 2: Create a numeric range from 1 to 5 but excluding the last integer number 5")
    val from1Until5 = 1 until 5
    println(s"Range from 1 until 5 where 5 is excluded = $from1Until5")

  }

  def rangeByStep(): Unit ={
    println("\nStep 3: Create a numeric range from 0 to 10 but increment with multiples of 2")
    val from0To10By2 = 0 to 10 by 2
    println(s"Range from 0 to 10 with multiples of 2 = $from0To10By2")
  }

  def rangeAToZ(): Unit ={
    println("\nStep 4: Create an alphabetical range to represent letter a to z")
    val alphabetRangeFromAToZ = 'a' to 'z'
    println(s"Range of alphabets from a to z = $alphabetRangeFromAToZ")
  }

  def rangeAToZByStep(): Unit ={
    println("\nStep 4: Create an alphabetical range to represent letter a to z")
    val alphabetRangeFromAToZ = 'a' to 'z' by 2
    println(s"Range of alphabets from a to z = $alphabetRangeFromAToZ")
  }

  def convertRangeToCollection(): Unit ={
    println("\nStep 6: Storing our ranges into collections")
    // 转换为List
    val listFrom1To5 = (1 to 5).toList
    println(s"Range to list = ${listFrom1To5.mkString(" ")}")
    //转换为set
    val setFrom1To5 = (1 to 5).toSet
    println(s"Range to set = ${setFrom1To5.mkString(" ")}")
    //转换为序列
    val sequenceFrom1To5 = (1 to 5).toSeq
    println(s"Range to sequence = ${sequenceFrom1To5.mkString(" ")}")
    //转换为array
    val arrayFrom1To5 = (1 to 5).toArray
    println(s"Range to array = `${arrayFrom1To5.mkString(" ")}")

  }

  def loopMethod(): Unit ={
    println("Step 1: How to use while loop in Scala")
    var numberOfDonutsToBake = 10
    while (numberOfDonutsToBake > 0) {
      println(s"Remaining donuts to be baked = $numberOfDonutsToBake")
      numberOfDonutsToBake -= 1
    }


    println("\nStep 2: How to use do while loop in Scala")
    var numberOfDonutsBaked = 0
    do {
      numberOfDonutsBaked += 1
      println(s"Number of donuts baked = $numberOfDonutsBaked")
    } while (numberOfDonutsBaked < 5)


  }

}
