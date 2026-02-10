import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.io.File

object WordCount {
  val inputFile = "D:\\Spark_Trainings\\dataset\\word1.txt"
  val outputFile = "D:\\Spark_Trainings\\dataset\\output.txt"

  def main(args: Array[String]): Unit = {
    " Spark Configuration "

    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")

    "Reading File with 3 partition"
    val inputRDD: RDD[String] = sc.textFile(inputFile,3)
    println(s"Total Lines: ${inputRDD.count()} ")
    println(s" Total number of partition :${inputRDD.getNumPartitions}")

    "Checking the number of Contents"
    val contentArr: Array[String] = inputRDD.collect()
    println("\n\n InputRDD collect Content: ")
    contentArr.foreach(println)

    "Checking the data in each partition"
    println("\n\nGlom Collect input RDD:")
    inputRDD.glom().collect().foreach { partition =>
      println(s"Partition: ${partition.mkString("[", ", ", "]")}")
    }

    "Getting each work , and counting the number of each word"
    val words: RDD[String] = inputRDD.flatMap(line => line split (" "))
    val countiPerWords: RDD[(String, Int)] = words.map(word => (word, 1))
    val counts: RDD[(String, Int)] = countiPerWords.reduceByKey{ (counter:Int, nextVal:Int) => counter + nextVal }

    println("\n Only collect:")
    counts.collect().foreach(println)

    println("\nGlom collect:")
    counts.glom.collect().foreach { partition =>
      println(s"Partition: ${partition.mkString("[", ", ", "]")}")
    }

    "Writing output in each file, Each partition will create 1 file "
//    FileUtils.deleteQuietly(new File(outputFile))
//    counts.saveAsTextFile(outputFile)

    println("Program executed successfully")

  }
}

