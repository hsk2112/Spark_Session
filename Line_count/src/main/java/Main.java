import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import java.util.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

import java.util.Arrays;

public class Main {
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Line_count").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//Reading a file example i.e reading data from external datasets
		JavaRDD<String> textLoadRDD = sc.textFile("C:/Users/hepa0816/Desktop/HSK/spark/spark-2.4.0-bin-hadoop2.7/spark-2.4.0-bin-hadoop2.7/README.md");
		//System.out.println(textLoadRDD.toString());
		System.out.println(textLoadRDD.count());
		
		//This is to save the RDD to another file
		textLoadRDD.saveAsTextFile("C:/Users/hepa0816/Desktop/HSK/spark/1.txt");
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> distData = sc.parallelize(data);
		System.out.println(distData.collect());
		//Data Sets
		//Reading and parsing the data from json file using data frames
		SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark SQL basic example")
				  .config("spark.some.config.option", "some-value")
				  .getOrCreate();
		RDD<String> textLoadRDD2= spark.sparkContext().textFile("C:/Users/hepa0816/Desktop/HSK/spark/spark-2.4.0-bin-hadoop2.7/spark-2.4.0-bin-hadoop2.7/README.md", 2);
		System.out.println("count: " + textLoadRDD2.count());
		Dataset<Row> df = spark.read().json("C:/Users/hepa0816/Desktop/HSK/Rakuten/CCUPP_work/InputFiles/jp-mobile-mno_monthly-comm_20160401_out.json");
		df.createOrReplaceTempView("HSK_JSON");
		Dataset<Row> sqlQ = spark.sql("SELECT sum(grossAmount) from HSK_JSON group by currencyCode");
		sqlQ.show(); 
		df.explain();
		df.select("cardToken").explain();
		df.show();
		df.printSchema();
		
		df.select("cardToken").show();
		df.select(col("currencyCode"), col("grossAmount")).show(); 
		
		//sc.close();
	}
}
