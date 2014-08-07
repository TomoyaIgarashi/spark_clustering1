package jp.co.couger.clustering

import scala.collection.JavaConversions

import java.net.URL
import java.io.FileInputStream
import java.util.Properties

import jp.co.couger.AESCrypt

import org.apache.spark._
import org.apache.spark.SparkContext._

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.language.reflectiveCalls
import scala.util.matching.Regex

object Clustering {
    def main(args: Array[String]) {
      if (args.length < 2) {
        println("Usage: [sparkmaster] [inputfile]")
        sys.exit(1)
      }
      val master = args(0)
      val inputFile = args(1)
      type Stoppable = { def stop(): Unit }
      def using[A <: Stoppable, B](resource: A)(f: A => B) = try {
        f(resource)
      } finally {
        resource.stop()
      }
      using (new SparkContext(master, "Clustering", System.getenv("SPARK_HOME"))) { sc =>
        val input = sc.textFile(inputFile)

        // 日付の表示のある行を抽出
        val r1 = """^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\]""".r
        val pf1: PartialFunction[String, Array[String]] = {
          case s if (r1 findFirstIn s).nonEmpty => s.split(" ").filter(_.trim.size > 0)
        }
        // INFOタグがあり、URLリクエストかパラメータの表示がある行を抽出
        val r2 = """INFO""".r
        val r3 = """^(Started|Parameters)""".r
        val pf2: PartialFunction[Array[String], String] = {
          case s if (r2 findFirstIn s(3)).nonEmpty && (r3 findFirstIn s(6).trim).nonEmpty => s.mkString(" ")
        }
        val extract1 = input.collect(pf1).filter(_.size >= 7).collect(pf2)

        // URLリクエストの表示行とパラメータの表示行でグループ分け
        val r4 = """.*(Started).*""".r
        val r5 = """.*(Parameters).*""".r
        val pf3: PartialFunction[String, String] = {
          case r4(a) => "url"
          case r5(a) => "params"
        }
        val extract2 = extract1.groupBy(pf3)

        // 復号化に必要な設定値を取得
        val properties = new Properties()
        properties.load(new FileInputStream("conf/local.properties"))
        val convertedProperties = JavaConversions.propertiesAsScalaMap(properties)
        val secretKey: String = convertedProperties.getOrElse("secretKey", "")
        val secretKeyAlgorithm: String = convertedProperties.getOrElse("secretKeyAlgorithm", "")
        // URLリクエストがあった日時とパスを抽出
        val pf4: PartialFunction[String, String] = {
          case s => {
            val splitted = s.split(" ")

            val r =  """^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]""".r
            val d = splitted.take(2).mkString(" ")
            val m: Regex.Match = r.findFirstMatchIn(d).get;

            val accessed = splitted.slice(8, 9).head.replaceAll("\"", "")
            val url = new URL("http", "example.com", accessed)

            "%s\t%s".format(m.group(1), url.getPath)
          }
        }
        // 暗号化されたパラメータを復号化してユーザを特定するauth_tokenを抽出
        val pf5: PartialFunction[String, String] = {
          case s => {
            val param = s.split(" ").drop(7).head.split("=>")(1).replaceAll("""[\"}]""", "")
            val decrypted = AESCrypt.decrypt(param, secretKey, secretKeyAlgorithm)
            val pf: PartialFunction[String, String] = {
              case s if s.startsWith("auth_token") => s.split("=")(1)
            }
            decrypted.split("&").collect(pf).mkString("")
          }
        }
        val pf6: PartialFunction[(String, Iterable[String]), Iterable[String]] = {
          case x if x._1 == "url" => x._2.map(pf4)
          case x if x._1 == "params" => x._2.map(pf5)
        }
        val partitions = extract2.map(pf6)

        // URLリクエストグループとパラメータグループからそれぞれタプルを生成
        val ary1 = partitions.collect()
        val rdd1 = sc.parallelize(ary1(0).toList)
        val rdd2 = sc.parallelize(ary1(1).toList)
        // auth_tokenの無いものをここで除去
        val rdd3 = rdd1.zip(rdd2).filter(_._1 != "")

        // (auth_token, datetime\turl)
        // DateTimeでsortかけるため(datetime, url)のタプルに変換
        val pf7: PartialFunction[(String, String), (DateTime, String)] = {
          case s => {
            val l = s._2.split("\t")
            val dateTime = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(l(0))
            (dateTime, l(1))
          }
        }
        val rdd4 = rdd3.groupBy(_._1).map(_._2.map(pf7))

        // Array[(DateTime, url)] groupBy auth_token
        // グループ分けしたものを書き出し
        val ary2 = rdd4.collect

        // ID指定のあるアクセスを除外
        val er1 = """[0-9]+.json""".r
        val pf8: PartialFunction[(DateTime, String), String] = {
          case (d, s) if (er1 findFirstIn s).isEmpty => s
        }
        // ascend by DateTime
        implicit def dateTimeOrdering: Ordering[DateTime] = Ordering.fromLessThan(_ isBefore _)
        val accessSequence = ary2.map { r =>
          // アクセス順にURLを並べた文字列を生成
          sc.parallelize(r.toList).sortByKey().collect(pf8).fold("")(_ + _)
        }
        // アクセス順URLをキーに(access sequence, 1)のRDDを生成して集計
        val rdd5 = sc.parallelize(accessSequence.toList).map((_, 1)).reduceByKey(_ + _).map {case (key, count) => (count, key)}.sortByKey()
        rdd5.collect.foreach(println)
      }
    }
}
