package com.allaboutscala.chapter5.di2

object TraitDependencyInjectionPartTwo extends App {

  /**
    * 定义相应的泛型类，并插入方法
    */
  println("Step 1: How to define a class to encapsulate inventory services")
  class DonutInventoryService[T] {
    def checkStock(donut: T): Boolean = {
      println("DonutInventoryService->checkStock")
      true
    }
  }

  println("\nStep 2: How to define a class to encapsulate pricing services")
  class DonutPricingService[T] {
    def calculatePrice(donut: T): Double = {
      println("DonutPricingService->calculatePrice")
      2.50
    }
  }

  println("\nStep 3: How to define a class to encapsulate creating a donut order")
  class DonutOrderService[T] {
    def createOrder(donut: T, quantity: Int, price: Double): Int = {
      println(s"Saving donut order to database: donut = $donut, quantity = $quantity, price = $price")
      100 // the id of the booked order
    }
  }

  /**
    * 多个类作为参数
    */
  println("\nStep 4: How to define a class to encapsulate shopping cart services")
  class DonutShoppingCartService[T] (
                                      donutInventoryService: DonutInventoryService[T],
                                      donutPricingService: DonutPricingService[T],
                                      donutOrderService: DonutOrderService[T]) {

    def bookOrder(donut: T, quantity: Int): Int = {
      println("DonutShoppingCartService->bookOrder")

      donutInventoryService.checkStock(donut) match {
        case true =>
          val price = donutPricingService.calculatePrice(donut)
          donutOrderService.createOrder(donut, quantity, price) // the id of the booked order

        case false =>
          println(s"Sorry donut $donut is out of stock!")
          -100 // return some error code to identify out of stock
      }
    }
  }

  /**
    * 创建trait
    */
  println("\nStep 5: How to define a trait to encapsulate all the services for Donut store")
  trait DonutStoreServices {
    val donutInventoryService = new DonutInventoryService[String]
    val donutPricingService = new DonutPricingService[String]
    val donutOrderService = new DonutOrderService[String]
    val donutShoppingCartService = new DonutShoppingCartService(donutInventoryService, donutPricingService, donutOrderService)
  }



  println("\nStep 6: How to define a facade to expose functionality of DonutStoreServices")
  trait DonutStoreAppController {
    this: DonutStoreServices =>

    def bookOrder(donut: String, quantity: Int): Int = {
      println("DonutStoreAppController->bookOrder")
      donutShoppingCartService.bookOrder(donut, quantity)
    }
  }


  println("\nStep 7: How to create a Donut store app which extends facade from Step 5 and injects the required donut services from Step 4")
  object DonutStoreApp extends DonutStoreAppController with DonutStoreServices



  println("\nStep 8: How to call the bookOrder method of the Donut store app from Step 7")
  DonutStoreApp.bookOrder("Vanilla Donut", 10)



  println("\nStep 9: Test DonutStoreApp by injecting a mocked version of DonutStoreServices")
  trait MockedDonutStoreServices extends DonutStoreServices {
    override val donutInventoryService: DonutInventoryService[String] = ???
    override val donutPricingService: DonutPricingService[String] = ???
    override val donutOrderService: DonutOrderService[String] = ???
    override val donutShoppingCartService: DonutShoppingCartService[String] = new DonutShoppingCartService[String](
      donutInventoryService, donutPricingService, donutOrderService)
  }



  println("\nStep 1: Create a Mocked Donut Store App and inject mocked donut services")
  object MockedDonutStoreApp extends DonutStoreAppController with MockedDonutStoreServices

}
