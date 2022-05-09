import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._     // StructType
import org.apache.spark._

object csvRead{
	def main(args: Array[String]) {
	
		// create Spark session
		val spark = SparkSession.builder().appName("csv-HDFS")
					.master("local[2]").getOrCreate();
		// create Spark context
		val sc=spark.sparkContext
		// set level of terminal logging information
		spark.sparkContext.setLogLevel("Warn")	
		
		// create SQL context
		val sqlContext = new SQLContext(sc) 
		
		// define schema for CSV reading
		val userSchema = new StructType()
		.add("player","string")
		.add("season","integer")
		.add("team","string")
		.add("s/c","string")
		.add("pos","string")
		.add("gp","integer")
		.add("g","integer")
		.add("a","integer")
		.add("p","integer")
		.add("+/-","integer")
		.add("pim","integer")
		.add("p/gp","integer")
		.add("evg","integer")
		.add("evp","integer")
		.add("ppg","integer")
		.add("ppp","integer")
		.add("shg","integer")
		.add("shp","integer")
		.add("otg","integer")
		.add("gwg","integer")
		.add("s","integer")
		.add("s%","string")
		.add("toi/gp","string")
		.add("fow%","string")
		
		// read CSV file into a DataFrame 
		 val csvDF = sqlContext
		 .read
		 .format("csv")   
		 .option("sep", ";")
		 .option("header","true")
		 .schema(userSchema)  // user-defined schema
		 //.option("inferSchema","true")   // automatically defined schema
		 //.load("file:///home/kofi69/NHL.csv")
		 .load("hdfs://localhost:54310/spark/input/NHL_stats_2021-2022.csv")
		 
		// show schema
	    csvDF.printSchema() 
		 //val result = csvDF.select("Player","Season","Team","G").where("G > 50") //a
		  //val result =csvDF.select("Player","Season","Team","G","A").where("A > 30 AND G < 20") //b
		  // val result =csvDF.select("Player","S/C","Pos","+/-").where("`+/-` > 17 AND `S/C` = 'L' AND Pos = 'C'")
		   
		 // show result on terminal
		 //result.show()
		 
		 // write result to file
		 result.write
		  .option("header", "true")
    	          .option("sep", ";")
    	         //.csv("hdfs://localhost:54310/spark/output1/")
    	         //.csv("hdfs://localhost:54310/spark/output2/")
    	         //.csv("hdfs://localhost:54310/spark/output3/")
		 spark.stop()
	}
}
