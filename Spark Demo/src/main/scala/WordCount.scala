import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.io.File

object WordeCount {
  val inputFile = "D:\\Spark\\dataset\\word1.txt"
  val outputFile = "D:\\Spark\\dataset\\output.txt"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    val inputRDD: RDD[String] = sc.textFile(inputFile)
    println(s"Total Lines: ${inputRDD.count()} ")
    println(s" Total number of partition :${inputRDD.getNumPartitions}")
    val contentArr: Array[String] = inputRDD.collect()
    println("content: ")
    contentArr.foreach(println)

    val words: RDD[String] = inputRDD.flatMap(line => line split (" "))
    val countiPerWords: RDD[(String, Int)] = words.map(word => (word, 1))
    val counts: RDD[(String, Int)] = countiPerWords.reduceByKey{ (counter:Int, nextVal:Int) => counter + nextVal }
    println("only collect",counts.collect())
    println("Glom collect",counts.glom.count())
//    FileUtils.deleteQuietly(new File(outputFile))
//    counts.saveAsTextFile(outputFile)

    println("Program executed successfully")

  }
}

