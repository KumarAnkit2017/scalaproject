package com.services

import org.apache.spark.sql.DataFrame

trait DataFrameServices {

  def totalWealthByBillionarsOfWorldUS(dataframe: DataFrame): Unit

  def totalWealthByBillionarsOfWorldINR(dataframe: DataFrame): Unit
  def averAgeOfBillionarsHavingNetworthGt18(dataframe: DataFrame):  Unit

  def industryHavingMorebillionars(dataframe: DataFrame):  Unit
}
