package com.app

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger

object sparkInit 
{

    // a simple project to test spark initialization
    def main(arg: Array[String]) 
    {
        val conf = new SparkConf()
            conf.setAppName("helloWorld")
                .setMaster("local")
      
        val sc = new SparkContext(conf)
            sc.setLogLevel("WARN")
        
        // do stuff
        println("Hello, world!")
        
        // terminate spark context
        sc.stop()
    }
}
