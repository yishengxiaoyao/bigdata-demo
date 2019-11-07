package com.book.code.sql

sealed case class Weather(date: String, city: String, minTem: Int, maxTem: Int)