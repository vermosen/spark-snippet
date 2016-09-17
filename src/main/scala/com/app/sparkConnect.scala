package com.app

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger

import java.sql.{Connection, DriverManager, ResultSet};
import org.postgresql.Driver

import org.apache.spark.sql._

object sparkConnect 
{
    // connect to a postgresql database
    def main(arg: Array[String]) 
    {
        val conf = new SparkConf()
            conf.setAppName("helloWorld")
                .setMaster("local")
      
        val sc = new SparkContext(conf)
            sc.setLogLevel("WARN")
        
        // Change to Your Database Config
        classOf[org.postgresql.Driver]
        val cStr = "jdbc:postgresql://localhost:5432/aletheia?user=posgres&password=1234"

        // Setup the connection
        val cnx = DriverManager.getConnection(cStr)
        
        try
        {
            
        }
        finally 
        {
            cnx.close
        }
        
        // do stuff
        println("Hello, world!")
        
        // terminate spark context
        sc.stop()
    }
}
