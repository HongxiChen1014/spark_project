package com.example.spark

import batch.LogApp

object DataApp {
  def main(args: Array[String]): Unit = {
    val url = "GET /course/list?c=cb HTTP/1.1"
    val referer = "https://www.imooc.com/course/list?c=data"
    val ip = "110.85.18.234"
    val rowkey = LogApp.getRowKey("20190130",url+referer+ip)

    println(rowkey)
  }
}
