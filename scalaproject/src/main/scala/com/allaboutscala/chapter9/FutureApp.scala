package com.allaboutscala.chapter9

object FutureApp extends App{

  /**
    * 阻塞进程
    */
  println("Step 1: Define a method which returns a Future")
  //访问Future类型
  import scala.concurrent.Future
  //替换默认的线程池，异步运行
  import scala.concurrent.ExecutionContext.Implicits.global
  def donutStock(donut: String): Future[Int] = Future {
    println("checking donut stock")
    10
  }


  println("\nStep 2: Call method which returns a Future")
  import scala.concurrent.Await
  import scala.concurrent.duration._

  /**
    * Await.result()阻塞主进程，等待方法的值
    * 第二个参数为等待的时间
    */
  val vanillaDonutStock = Await.result(donutStock("vanilla donut"), 5 seconds)
  println(s"Stock of vanilla donut = $vanillaDonutStock")

  /**
    * 使用onComplete方法，不会阻塞等到方法返回结果，而是将收到成功或失败的回调。
    */
  println("\nStep 3: Non blocking future result")
  import scala.util.{Failure, Success}
  donutStock("vanilla donut").onComplete {
    case Success(stock) => println(s"Stock for vanilla donut = $stock")
    case Failure(e) => println(s"Failed to find vanilla donut stock, exception = $e")
  }
  Thread.sleep(3000)

}
