package com.edu.spark.log

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.parboiled2.{Parser, ParserInput}

case class LogRecord(dateTime: Timestamp, component: String, level: String, message: String)

class LogParser(val input: ParserInput) extends Parser {
  def WhiteSpaceChar = rule {
    anyOf(" \t")
  }

  def NonWhiteSpaceChar = rule {
    noneOf(" \t")
  }

  def WhiteSpace = rule {
    WhiteSpaceChar +
  }

  def Field = rule {
    capture(NonWhiteSpaceChar +)
  }

  def MessageField = rule {
    capture(ANY +)
  }

  def DateTimeField = rule {
    Field ~> { str =>
      Timestamp.valueOf(LocalDateTime.parse(str, DateTimeFormatter.ISO_LOCAL_DATE_TIME))
    }
  }

  def Record = rule {
    DateTimeField ~ WhiteSpace ~
      Field ~ WhiteSpace ~
      Field ~ WhiteSpace ~
      MessageField ~> LogRecord
  }

  def Line = rule {
    Record ~ EOI
  }
}