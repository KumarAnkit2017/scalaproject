package com.services

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, desc}

class DataFrameServicesImpl extends DataFrameServices {

  def totalWealthByBillionarsOfWorldUS(dataframe: DataFrame):  Unit = {
    //var totalWealthUSD= dataframe.selectExpr("SUM(net_worth) as total_wealth_usd").collect()(0)(0);
    val totalWealthUSD= dataframe.selectExpr("SUM(net_worth) as total_wealth_usd");
    totalWealthUSD.show();

    println(s"1. Total Wealth (USD): $totalWealthUSD")

  }

  def totalWealthByBillionarsOfWorldINR(dataframe: DataFrame):  Unit= {
    //val totalWealthINR = dataframe.selectExpr("SUM(net_worth * 82.14) as total_wealth_inr").collect()(0)(0);
    val totalWealthINR = dataframe.selectExpr("SUM(net_worth * 82.14) as total_wealth_inr")
    totalWealthINR.show();
    println(s"2. Total Wealth (INR): $totalWealthINR")
  }


  def averAgeOfBillionarsHavingNetworthGt18(dataframe: DataFrame):  Unit= {
    //val averageAge = dataframe.filter("net_worth > 18").agg(avg("age4").as("average_age")).collect()(0)(0);
    val averageAge = dataframe.filter("net_worth > 18").agg(avg("age4").as("average_age"));
    averageAge.show();
    println(s"3. Average Age of Billionaires with Net Worth > 18 billion: $averageAge")

  }


  def industryHavingMorebillionars(dataframe: DataFrame):  Unit= {
    val mostCommonIndustry = dataframe.groupBy("industry").count().orderBy(desc("count")).select("industry").first().getString(0)
    println(s"4. Industry with the Most Number of Billionaires: $mostCommonIndustry")
  }


}
