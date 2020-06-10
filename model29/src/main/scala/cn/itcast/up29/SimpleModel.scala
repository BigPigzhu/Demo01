package cn.itcast.up29

import java.util.Properties

import cn.itcast.up29.bean.HBaseMeta
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 入门案例
  */
object SimpleModel {

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

    import org.apache.spark.sql.functions._

    import spark.implicits._
    val ds: Dataset[Row] = mysqlDF.select('rule)
      .where('id === 256)
    //加载MySQL中的4级规则,获取HBase的连接信息
    //    inType=HBase##zkHosts=192.168.10.20##zkPort=2181##hbaseTable=tbl_users##family=detail##selectFields=id,gender
    val fourRuleStr: Map[String, String] = ds
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
      .where('pid === 256)



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
    val rse: DataFrame = hbaseSource.select(
      'id as "userid",
      when('gender === 1, 257)
        .when('gender === 2, 258)
        .as("tagIds")
    )
    rse.show()

    val list: List[StructField] = rse.schema.toList
    println(list)

    //将计算结果存入HBase
    rse.write
      .format("cn.itcast.up29.tools.HBaseSource")
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, "test03")
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS, "userid,tagIds")
      .option(HBaseMeta.ROWKEY, "userid")
      .save()
  }
}
