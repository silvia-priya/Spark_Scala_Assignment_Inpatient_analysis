
import org.apache.spark.SparkContext;

import org.apache.spark.SparkConf;



object Main {

  def main(args:Array[String]) {

    val sparkConf = new SparkConf().setAppName("Inpatient_Analysis").setMaster("local")

    val sc = new SparkContext(sparkConf)

/* 
	
    use case 1

    Find out the city and the state name which has the highest and lowest number of discharge cases.

*/

    #RDD to load the input file
    val inpatientRDD = sc.textFile("/home/hadoop/Learnbay/Spark/inpatient_samp.csv")

    #RDD to fetch the first line which is the header
    val headers = inpatientRDD.first();

    #RDD 
    val inpatient_1 = inpatientRDD.filter(x=>x!=headers).map(x=>(x.split(","))).map(x=>((x(4),x(5)),x(6).toInt)).reduceByKey(_+_).sortBy(x=>x._2,false).take(5)

    #Load the top 5 cities and states having maximum number of discharge cases.
    sc.parallelize(inpatient_1).saveAsTextFile("/home/hadoop/Learnbay/spark/inpatient_usecase1")


/* 
    use case 2

    What are all the different referral regions under every state with a code AL and CA which has a discharge rate of more than 20.

*/

    #chain RDD which performs the below operation
		#skip the header
		#Fetches the region with the discharge rates.
		#Fetch only the region AL and CA.
		#Filter records with the discharge rate > 20
    val inpatient_2 = inpatientRDD.filter(x=>x!=headers).map(x=>(x.split(","))).map(x=>((x(7).split("-")(0).trim(),x(7).split("-")(1).trim()), x(8))).map(x=>( (x._1._1,x._1._2), x._2.toInt)).distinct().filter( x=> (x._1._1.equals("AL") || x._1._1.equals("CA")) && x._2 > 20)

    inpatient_2.saveAsTextFile("/home/hadoop/Learnbay/Spark/usecase2")

  }
}

