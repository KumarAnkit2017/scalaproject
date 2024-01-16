package com.services

import org.apache.spark.sql.SparkSession

object ConvertCSVtoDF {

  def main(args: Array[String]): Unit = {

    val ss=SparkSession.builder().appName("MyCSVtoDF").master("local").getOrCreate();

    /// 1st way to create schema for my csv file
  }
}
