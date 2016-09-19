package com.app

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}

import java.sql.{Connection, DriverManager, ResultSet};
import org.postgresql.Driver

import org.apache.spark.sql._

object sparkConnect
{
    // connect to a postgresql database
    def main(arg: Array[String]) 
    {

        // disable logger spam
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)
        
        val partSize: Int = 10
        
        // new spark 2.0.0 syntax
        val spark: SparkSession = SparkSession
            .builder()
            .master    ("local[*]")
            .appName   ("spark.connect")
            .enableHiveSupport()
            //.config    ("spark.logLevel", "WARN")
            .getOrCreate()
            
        spark.conf.set("spark.sql.shuffle.partitions", 6)
        spark.conf.set("spark.executor.memory", "2g")
        spark.conf.set("spark.driver.memory", "2g")    
                  
        try
        {
            // database
            val cnxStr: String = "jdbc:postgresql://localhost:5432/aletheia"
            val user:   String = "postgres"
            val pwd:    String = "1234"
            
            // Setup the connection
            val cnx = DriverManager.getConnection(cnxStr, user, pwd)
            
            try
            {
                
                val query: String = 
                    "(" + 
                        "SELECT timeseries.index, timeseries.date, timeseries.value " +
                        "FROM timeseries " +
                        "JOIN index ON timeseries.index = index.id " +
                        "WHERE index.source = 2" +
                    ") AS temp"
                
                val df: DataFrame = spark.read
                    .format("jdbc")
                    .option("url", cnxStr)
                    .option("user", user)
                    .option("password", pwd)
                    .option("dbtable", query)
                    .load()
                    
                println("retrieved " + df.count + " records")
                println("sample:")
                println(df.head())
                
            }
            catch
            {
                case e: Exception => println("an exception occurred: " + e.getMessage)
            }
            finally
            {
                cnx.close
            }
        }
        catch
        {
            case e: Exception => println("an exception occurred: " + e.getMessage)
        }
        finally
        {
            spark.stop                      // terminate spark session
        }
    }
}
