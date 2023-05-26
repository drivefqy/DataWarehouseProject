package Spark_Require

import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat

object song {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[*]").setAppName("song")
      val sc = new SparkContext(conf)
      val elementDatas = sc.textFile("Resources/song.tsv")
      import java.time._
      import java.util.Date
      def instantToDate(long: Long) ={
        val instant = new Date(long)
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        dateFormat.format(instant)
      }
      elementDatas.map(
        lines => {
          val datas = lines.split("\t")
          val instant = datas(10)
          if (instant.contains("E+")){
            val ms = instant.substring(0, instant.length - 4).toDouble * Math.pow(10, instant.substring(instant.length - 2).toDouble)
            datas(10) = instantToDate(ms.toLong)
            println(datas(10)+"   " + ms)
          }
          datas.mkString("|").
            replaceAll("\\[|\\]", "").
            replaceAll("\"", "").
            replaceAll("（", "(").
            replaceAll("）", ")").
            replaceAll("\\{|\\}", "").
            replaceAll("《|》", "")
        }
      ).coalesce(1).saveAsTextFile("song")

    }
}

