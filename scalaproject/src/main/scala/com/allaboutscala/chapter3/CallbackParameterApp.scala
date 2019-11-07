package com.allaboutscala.chapter3

object CallbackParameterApp extends App {
  println("Step 1: How to define a function with a callback parameter")
  def printReport(sendEmailCallback: () => Unit) {
    println("Printing report ... started")
    // look up some data in  database and create a report
    println("Printing report ... finished")
    sendEmailCallback()
  }



  println("\nStep 2: How to call a function which has a callback parameter")
  printReport(() =>
    println("Sending email ... finished")
  )



  println("\nStep 3: How to call a function without providing its callback parameter")
  // printReport() // You get compile time error
  printReport(() => {}) // Not that elegant.



  println("\nStep 4: How to define a function with an Option callback")
  def printReportWithOptionCallback(sendEmailCallback: Option[() => Unit] = None) {
    println("Printing report ... started")
    // look up some data in database and create a report
    println("Printing report ... finished")
    sendEmailCallback.map(callback => callback())
  }



  println("\nStep 5: How to call a function without providing its callback parameter")
  printReportWithOptionCallback() // more elegant



  println("\nStep 6: How to call a function with Option callback parameter")
  printReportWithOptionCallback(Some(() =>
    println("Sending email wrapped in Some() ... finished")
  ))

}
