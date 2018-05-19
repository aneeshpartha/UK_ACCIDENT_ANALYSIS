/**
  * Created by Aneesh Partha on 3/10/2018.
  */

//Import statements

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.rdd.RDD



object config {

    def create_config() = {

    //Setting hadoop directory as there is a bug in Spark 2.0 versions
    System.setProperty("hadoop.home.dir", "G:\\Datasets\\UKaccident")

    // Turning off the logs which are generated during the program run
    val rootLogger = Logger.getRootLogger()
    Logger.getLogger("org").setLevel(Level.OFF)

    // Setting the configuration for the application


    val conf = new SparkConf().setMaster("local").setAppName("UKData")

    // Connection to spark cluster
    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)

    //Creating spark session
    val spark = SparkSession.builder().appName("UKda" +
      "ta").config("spark.sql.warehouse.dir", "file:///C:/Users/Aneesh%20Partha/IdeaProjects/OrderItems/spark-warehouse").getOrCreate()

    spark
  }


}



object ukdataanalysis extends App {

  def create_data() = {

    // Reading contents from csv file
    val input = spark.read.format("com.databricks.spark.csv").option("header","true").
      load("G:\\Datasets\\UKaccident\\UKdata\\accidents_2005_to_2007.csv")
    val input1 = spark.read.format("com.databricks.spark.csv").option("header","true").
      load("G:\\Datasets\\UKaccident\\UKdata\\accidents_2009_to_2011.csv")
    val input2 = spark.read.format("com.databricks.spark.csv").option("header","true").
      load("G:\\Datasets\\UKaccident\\UKdata\\accidents_2012_to_2014.csv")

    // Joining data from 3 different files and converting it to RDD.
    // The operation can be done with dataframes but now using RDDs
       val finaldata = input.union(input1).union(input2).rdd

    finaldata
  }

  def road_class(finaldata:RDD[Row]): Unit = {


    // Extracting the required fields
    val data1 = finaldata.map(rec => (rec(32),rec(16)))

    // Finding out the total accidents based on key value and sorting them in descending order
    val data2= data1.map(rec => ((rec._1,rec._2),1)).reduceByKey((a,b) => a+b).sortBy(- _._2)

    // Saving the output
    data2.map(rec => (rec._1._1+","+rec._1._2+","+rec._2)).coalesce(1).saveAsTextFile("G:\\Datasets\\UKaccident\\UKdata\\output_roadclass")


  }

  def weather_conditions(finaldata:RDD[Row]): Unit ={


    // Extracting the required fields
    val data1 = finaldata.map(rec => (rec(32),rec(25)))

    // Finding out the total accidents based on key value and sorting them in descending order
    val data2= data1.map(rec => ((rec._1,rec._2),1)).reduceByKey((a,b) => a+b).sortBy(- _._2)

    // Saving the output
    data2.map(rec => (rec._1._1+","+rec._1._2+","+rec._2)).coalesce(1).saveAsTextFile("G:\\Datasets\\UKaccident\\UKdata\\output_weather_conditions")


  }

  def road_surface(finaldata:RDD[Row]): Unit ={

    // Extracting the required fields
    val data1 = finaldata.map(rec => (rec(32),rec(26)))

    // Finding out the total accidents based on key value and sorting them in descending order
    val data2= data1.map(rec => ((rec._1,rec._2),1)).reduceByKey((a,b) => a+b).sortBy(- _._2)

    // Saving the output
    data2.map(rec => (rec._1._1+","+rec._1._2+","+rec._2)).coalesce(1).saveAsTextFile("G:\\Datasets\\UKaccident\\UKdata\\output_roadsurface")

  }

  // Creating all configurations using a singleton object
  val spark = config.create_config()

  // Getting combined data for all years.
  val finaldata = create_data()

  // Determine accidents caused by road class
  road_class(finaldata)

  // Determine accidents cause by weather conditions
  weather_conditions(finaldata)

  // Determine accidents caused by road surface
  road_surface(finaldata)

}
