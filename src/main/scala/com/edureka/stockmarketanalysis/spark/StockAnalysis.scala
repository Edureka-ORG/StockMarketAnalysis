package com.edureka.stockmarketanalysis.spark

import org.apache.spark.rdd.RDD
import com.edureka.stockmarketanalysis.Stock
import com.edureka.stockmarketanalysis.util.StockUtil
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import com.edureka.stockmarketanalysis.STOCKConstants
import com.edureka.stockmarketanalysis.STOCKConstants
import com.edureka.stockmarketanalysis.STOCKConstants
import scala.reflect.ClassTag
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row
import org.apache.spark.mllib.stat.Statistics

object StockAnalysis 
{
  
  
  def parseRDD(rdd:RDD[String]):RDD[Stock] = {
    val header = rdd.first();
    
    rdd.filter(_(0) != header(0)).map(StockUtil.parseStock).cache();
  }
  
  def main(args: Array[String]): Unit = 
  {
    System.setProperty("hadoop.home.dir", "E:\\SAIWS\\Edureka\\Hadoop");
    
    val sparkConf = new SparkConf().setAppName("Stock-Market-Analysis")
    .setMaster("local[*]")
    .set("spark.submit.deployMode", "client");
    
    val spark = SparkSession.builder().config(sparkConf).getOrCreate();
    spark.sparkContext.setLogLevel("ERROR");
    
    val AAONPATH = "file:///C:\\Users\\Suresh\\Dropbox\\Students\\Batch030818\\stockmarketanalysis\\src\\main\\resources\\Project_datasets\\AAON.csv";
    val MSFTPATH = "file:///C:\\Users\\Suresh\\Dropbox\\Students\\Batch030818\\stockmarketanalysis\\src\\main\\resources\\Project_datasets\\MSFT.csv";
    val ABAXPATH = "file:///C:\\Users\\Suresh\\Dropbox\\Students\\Batch030818\\stockmarketanalysis\\src\\main\\resources\\Project_datasets\\ABAX.csv";
    val FASTPATH = "file:///C:\\Users\\Suresh\\Dropbox\\Students\\Batch030818\\stockmarketanalysis\\src\\main\\resources\\Project_datasets\\ABAX.csv";
    
//    println("Reading Directories"+inputPath);
     

    
    import spark.implicits._;
    
    val stockAAONDF = parseRDD(spark.sparkContext.textFile(AAONPATH)).toDF;
    val stockMSFTDF = parseRDD(spark.sparkContext.textFile(MSFTPATH)).toDF;
    val stockABAXDF = parseRDD(spark.sparkContext.textFile(ABAXPATH)).toDF;
    val stockFASTDF = parseRDD(spark.sparkContext.textFile(ABAXPATH)).toDF;
    
//    inputRDD.
    stockAAONDF.show();
    stockMSFTDF.show()
    stockABAXDF.show();
    stockFASTDF.show();
    
    import org.apache.spark.sql.functions._;
    
    stockAAONDF.select(
        year($"dt").alias("yr"),
        month($"dt").alias("mo"),
        $"adjClosePrice"
        )
        .groupBy("yr","mo")
        .agg(avg("adjClosePrice"))
        .orderBy(desc("yr"),desc("mo")).show()
        
    
    //CREATE TEMPORARY VIEWS
    stockAAONDF.createOrReplaceTempView("stocksAAON");
    stockMSFTDF.createOrReplaceTempView("stocksMSFT");
    stockABAXDF.createOrReplaceTempView("stocksABAX");
    stockFASTDF.createOrReplaceTempView("stocksFAST");
//    implicit val ConstantTag = ClassTag[STOCKConstants](classOf[STOCKConstants])

    var res= spark.sql(STOCKConstants.MSFT_STEEP_CHANGE)
    res.show()

    val joinClose = spark.sql(STOCKConstants.STOCKS_COMPARE_CLOSING_PRICE).cache();
    joinClose.show()
    
    val joinCloseOutput ="file:///C:\\Users\\Suresh\\Dropbox\\Students\\Batch030818\\stockmarketanalysis\\src\\main\\resources\\Project_datasets\\JOIN_CLOSE_OUT";
    joinClose.write.mode(SaveMode.Overwrite).parquet(joinCloseOutput);
    
    val df = spark.read.parquet(joinCloseOutput);
    df.show();
    
    //Create Temporary View
    df.createOrReplaceTempView("JoinClose");
    val newTable = spark.sql(STOCKConstants.AVG_CLOSING_PER_YEAR);
    newTable.show();
    
    //Create temporary view
    newTable.createOrReplaceTempView("newTable");
    val companyAll = spark.sql(STOCKConstants.COMPANY_ALL_VALUES);
    companyAll.show();
    
    companyAll.createOrReplaceTempView("CompanyAll");
    
    val bestCompanyYear = spark.sql(STOCKConstants.BEST_AVG_CLOSING);
    bestCompanyYear.show();
    bestCompanyYear.createOrReplaceTempView("BestCompanyYear");
    
    val finalTable = spark.sql(STOCKConstants.FINAL_TABLE);
    finalTable.show();
    
    //Compute Correlation
    
    val series1 = df.select($"AAONClose").map{row:Row => row.getAs[Double]("AAONClose")}.rdd;
//    series1.show();
     val series2 = df.select($"ABAXClose").map{row:Row => row.getAs[Double]("ABAXClose")}.rdd;
     
     import org.apache.spark.mllib.stat.Statistics._;
     
     val correlation = Statistics.corr(series1,series2,"pearson");
    
    println(correlation)
    
    
  }
}