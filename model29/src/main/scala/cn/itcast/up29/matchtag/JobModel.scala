package cn.itcast.up29.matchtag

import java.util.Properties

import cn.itcast.up29.SimpleModel.parseMeta
import cn.itcast.up29.bean.HBaseMeta
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StructField

object JobModel {
  def main(args: Array[String]): Unit = {

    //构建SparkSession对象
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SimpleModel")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .getOrCreate()
    //获取MySQL数据源
    val url = "jdbc:mysql://bd001:3306/tags_new?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&user=root&password=123456"
    val tableName = "tbl_basic_tag"
    val properties = new Properties()
    val mysqlDF: DataFrame = spark.read
      .jdbc(url, tableName, properties)
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //加载MySQL中的4级规则,获取HBase的连接信息
    //    inType=HBase##zkHosts=192.168.10.20##zkPort=2181##hbaseTable=tbl_users##family=detail##selectFields=id,gender
    val fourRuleStr: Map[String, String] = mysqlDF.select('rule)
      //获取工作的数据
      .where('id === 259)
      //开始解析
      .rdd.map(row => {
      val ruleStr: String = row.getAs("rule").toString
      ruleStr.split("##")
        .map(kv => {
          //          inType=HBase
          val arr: Array[String] = kv.split("=")
          (arr(0), arr(1))
        }).toMap
    }).collect()(0)
    //将map封装为HBaseMeta
    val hbaseMeta: HBaseMeta = parseMeta(fourRuleStr)

    //加载MySQL中的5级规则
    val fiveDF: Dataset[Row] = mysqlDF.select('id, 'rule)
      .where('pid === 259)



    //加载HBase中的数据源
    val hbaseSource: DataFrame = spark.read
      .format("cn.itcast.up29.tools.HBaseSource")
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .option(HBaseMeta.ROWKEY, hbaseMeta.rowKey)
      .load()
    //使用5级规则和HBase的数据源进行对应的计算
    val newData: DataFrame = hbaseSource.select(
      'id as "userid",
      when('job === 1, 260)
        .when('job === 2, 261)
        .when('job === 3, 262)
        .when('job === 4, 263)
        .when('job === 5, 264)
        .when('job === 6, 265)
        .when('job === 7, 267)
        .as("tagIds")
    )
    newData.show()

    //读取原数据,将新数据进行合并.
    val oldData: DataFrame = spark.read
      .format("cn.itcast.up29.tools.HBaseSource")
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, "test03")
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS, "userid,tagIds")
      .option(HBaseMeta.ROWKEY, "userid")
      .load()

    //定义合并标签的函数

    val mergeUDF = udf(mergeTag _)
    val getTag: Column = mergeUDF(newData.col("tagIds"), oldData.col("tagIds"))

    //开始合并
    val totalDF: DataFrame = newData.join(oldData, newData.col("userid") === oldData.col("userid"), "left")
      .select(newData.col("userid"), getTag as "tagIds")

    totalDF.show()

    //将计算结果存入HBase
    totalDF.write
      .format("cn.itcast.up29.tools.HBaseSource")
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, "test03")
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS, "userid,tagIds")
      .option(HBaseMeta.ROWKEY, "userid")
      .save()
  }

  def parseMeta(fourRuleStr: Map[String, String]): HBaseMeta = {
    HBaseMeta(
      fourRuleStr.getOrElse(HBaseMeta.INTYPE, null),
      fourRuleStr.getOrElse(HBaseMeta.ZKHOSTS, null),
      fourRuleStr.getOrElse(HBaseMeta.ZKPORT, null),
      fourRuleStr.getOrElse(HBaseMeta.HBASETABLE, null),
      fourRuleStr.getOrElse(HBaseMeta.FAMILY, null),
      fourRuleStr.getOrElse(HBaseMeta.SELECTFIELDS, null),
      fourRuleStr.getOrElse(HBaseMeta.ROWKEY, null)
    )
  }
  def mergeTag (newIDs :String, oldIds: String) = {
    if (StringUtils.isBlank(newIDs)) {
      oldIds
    }else if (StringUtils.isBlank(oldIds)) {
      newIDs
    } else {
      (newIDs.toString.split(",") ++ oldIds.toString.split(",")).toSet.mkString(",")
    }
  }

}
