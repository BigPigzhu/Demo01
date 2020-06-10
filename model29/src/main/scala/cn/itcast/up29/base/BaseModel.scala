package cn.itcast.up29.base

import java.util.Properties

import cn.itcast.up29.bean.HBaseMeta
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 公共的代码都定义在这里
  */
trait BaseModel {
  val config: Config = ConfigFactory.load()
  val url: String = config.getString("jdbc.url")
  val tableName: String = config.getString("jdbc.table")
  val sourceClass: String = config.getString("hbase.source.class")
  val zkHosts: String = config.getString("hbase.source.zkHosts")
  val zkPort: String = config.getString("hbase.source.zkPort")
  val hbaseTable: String = config.getString("hbase.source.hbaseTable")
  val family: String = config.getString("hbase.source.family")
  val selectFields: String = config.getString("hbase.source.selectFields")
  val rowKey: String = config.getString("hbase.source.rowKey")

  //操作标签结果的meta
  val hbaseMeta = HBaseMeta(
    "",
    zkHosts,
    zkPort,
    hbaseTable,
    family,
    selectFields,
    rowKey
  )

  /**
    * 定义标签ID
    * @return
    */
  def getTagID():Int


  //定义启动方法
  def execute()={
    //加载mysql数据源
    val mysqlDF: DataFrame = getMySQLSource()
    //获取4级规则
    val params: Map[String, String] = getFourDF(mysqlDF)
    //获取5级规则
    val fiveDF: DataFrame = getFiveDF(mysqlDF)
    //获取HBase数据源
    val hbaseDF: DataFrame = getHBaseSource(params)
    //开始计算
    val newDF: DataFrame = compute(fiveDF, hbaseDF)
    //合并结果
    val resultDF: DataFrame = beginMergeTag(newDF)
    //保存数据
    saveData(resultDF)
  }


  //创建sparksession
  val spark = SparkSession.builder()
    .appName("model")
    .master("local[*]")
    .config("spark.hadoop.validateOutputSpecs", "false")
    .getOrCreate()
  import spark.implicits._
  import org.apache.spark.sql.functions._

  //创建MySQL数据源
  def getMySQLSource(): DataFrame={
    val properties = new Properties()
    val mysqlDF: DataFrame = spark.read
      .jdbc(url, tableName, properties)
    mysqlDF
  }
  //获取4级数据源规则
  def getFourDF(mysqlDF: DataFrame):Map[String, String]={
    mysqlDF.select('rule)
      //获取工作的数据
      .where('id === getTagID())
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
  }
  //获取5级数据源规则
  def getFiveDF(mysqlDF: DataFrame): DataFrame={
    mysqlDF.select('id, 'rule)
      .where('pid === getTagID())
      .toDF()
  }
  //加载HBase数据源
  def getHBaseSource(params: Map[String, String]): DataFrame={
    spark.read
      .format(sourceClass)
      .options(params)
      .load()
  }

  /**
    * 开始计算
    */
  def compute(fiveDF: DataFrame, sourceDF: DataFrame): DataFrame
  //计算结果合并
  def beginMergeTag(newDF: DataFrame): DataFrame={
    //读取之前的数据
    val oldDF: DataFrame = spark.read
      .format(sourceClass)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .option(HBaseMeta.ROWKEY, hbaseMeta.rowKey)
      .load()
    //将本次的数据合并
    //    2 34   2 43   2 34,43
    oldDF.createOrReplaceTempView("oldTbl")
    newDF.createOrReplaceTempView("newTbl")
    spark.udf.register("mergeTag",(oldTag: String, newTag: String)=>{
      if (StringUtils.isBlank(oldTag)){
        newTag
      } else if (StringUtils.isBlank(newTag)) {
        oldTag
      } else {
        //两个都不为空,开始合并
        (oldTag.split(",") ++ newTag.split(",")).toSet.mkString(",")
      }
    })

    val resultDF: DataFrame = spark.sql("select n.userId as userId, mergeTag(o.tagIds, n.tagIds) as tagIds from newTbl n left join oldTbl o on o.userId = n.userId")
    resultDF
  }
  //数据落地到HBase
  def saveData(resultDF: DataFrame) = {
    resultDF
      //将合并后的数据存入HBase
      .write
      .format(sourceClass)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .option(HBaseMeta.ROWKEY, hbaseMeta.rowKey)
      .save()
  }
}
