package com.czxy.Gender.tag

import java.util.Properties

import com.czxy.Gender.bean.{HBaseMeta, TagRule}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object nationalityaaa {
  def main(args: Array[String]): Unit = {

    //1创建sparksql对象
    val spark: SparkSession = SparkSession.builder().appName("GenderTag").master("local[*]").getOrCreate()

    //2连接mysql 数据库
    val url = "jdbc:mysql://bd001:3306/tags_new?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&user=root&password=123456"
    var table = "tbl_basic_tag"
    var properties: Properties = new Properties
    val mysqlConn: DataFrame = spark.read.jdbc(url, table, properties)

    //隐式转换
    import spark.implicits._
    //引入java 和scala相互转换
    import scala.collection.JavaConverters._
    //引入spark内置函数
    import org.apache.spark.sql.functions._
    //3读取mysql数据库中的的四级标签

    val fourTags: Dataset[Row] = mysqlConn.select("id", "rule").where("id=61")

    val KVMap: Map[String, String] = fourTags.map(row => {

      row.getAs("rule").toString
        .split("##")
        .map(kv => {

          val arr: Array[String] = kv.split("=")
          (arr(0), arr(1))
        })
    }).collectAsList().get(0).toMap
    //print(KVMap+"---")
    //开发 toHbaseMeta方法 讲KVMap 封装成样列类 TagFour
    var hbaseMeta: HBaseMeta = toHBaseMeta(KVMap)

    //4读取mysql数据库中的的五级标签

    val fiveTagsDS: Dataset[Row] = mysqlConn.select("id", "rule").where("pid=61")
    val fiveTagsList: List[TagRule] = fiveTagsDS.map(row => {
      //获取id 和 rule
      val id: Int = row.getAs("id").toString.toInt
      val rule: String = row.getAs("rule").toString
      //封装样列类
      TagRule(id, rule)

    }).collectAsList().asScala.toList

    //5根据mysql数据中的四级标签的规则 读取hbase 数据
    //5、读取hbase数据
    val hbaseDatas: DataFrame = spark.read.format("com.czxy.Gender.tools.HBaseDataSource")
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts) //192.168.10.20
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort) //2181
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .load()
    //输出  查看效果
    //+---+---+
    //| id|job|
    //+---+---+
    //|  1|  3|
    //| 10|  5|
    //|100|  3|
    //|101|  1|
    hbaseDatas.show()

    //职业变换的自定义函数
    var getTags = udf((rule: String) => {
      //遍历每一个 rule，判断是否与数据中的相同，若相同返回对应的id
      //定义默认的 tagId=0
      var tagId = 0
      for (tagRule <- fiveTagsList) {
        if (tagRule.rule == rule) {
          tagId = tagRule.id
        }
      }
      tagId
    })

    //6、使用五级标签与hbase数据进行匹配获得标签
    //id job
    val jobNewTags: DataFrame = hbaseDatas.select('id.as("userId"), getTags('nationality).as("tagsId"))
    //输出 查看效果
    //+------+------+
    //|userId|tagsId|
    //+------+------+
    //|     1|    67|
    //|    10|    69|
    //|   100|    67|
    //|   101|    65|
    jobNewTags.show()

    //该函数用于步骤7-b
    //新数据(job) 的结构  userId  tagsId
    //老数据(gender) 的结构  userId  tagsId
    //使用join 将两个数据的 tagsId 合并到一起
    var getAllTags = udf((oldTagsId: String, newTagsId: String) => {
      if (oldTagsId == "") {
        newTagsId
      } else if (newTagsId == "") {
        oldTagsId
      } else if (oldTagsId == "" && newTagsId == "") {
        ""
      } else {
        //拼接历史数据和新数据(可能有重复的数据)
        val allTags = oldTagsId + "," + newTagsId
        //对重复数据去重,使用逗号分隔，返回字符串类型数据
        allTags.split(",").distinct.mkString(",")
      }
    })






    //7、解决数据覆盖的问题【职业标签会覆盖前面所有的标签】
    //读取 hbase中test表中的 历史标签数据，追加新计算出来的标签到历史数据，最后覆盖写入hbase
    //a、读取 hbase中test表中的 历史标签数据【不是职业标签，是已经计算出来的其他标签】
    val oldTags: DataFrame = spark.read.format("com.czxy.Gender.tools.HBaseDataSource")
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts) //192.168.10.20
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort) //2181
      .option(HBaseMeta.HBASETABLE, "test")
      .option(HBaseMeta.FAMILY, "detail")
      .option(HBaseMeta.SELECTFIELDS, "userId,tagsId")
      .load()
    oldTags.show(20)

    //b、追加新计算出来的标签到历史数据AAAAAAAAAAAAA
    //历史表 join 新表，条件是：两个表的 userId相等
    //【两表join，拿到4个字段：userId，tagsId，userId，tagsId，其中userId是相同的，所以后面需要对其去重，并将两个tagsId字段进行拼接】
    val joinTags: DataFrame = oldTags.join(jobNewTags, oldTags("userId") === jobNewTags("userId"))
    joinTags.show()

    val allTags: DataFrame = joinTags.select(
      //处理第一个字段,两个表中的多个userId字段，只读取一个
      when((oldTags.col("userId").isNotNull), (oldTags.col("userId")))
        .when((jobNewTags.col("userId").isNotNull), (jobNewTags.col("userId")))
        .as("userId"),
      //处理第二个字段，将两个字段合并到一起
      //自定义udf函数【用于做数据的拼接】
      getAllTags(oldTags.col("tagsId"), jobNewTags.col("tagsId")).as("tagsId")
    )
    allTags

    //c、最后覆盖写入hbase
    //8、将最终数据写入hbase
    allTags.write.format("com.czxy.Gender.tools.HBaseDataSource")
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, "test")
      .option(HBaseMeta.FAMILY, "detail")
      .option(HBaseMeta.SELECTFIELDS, "userId,tagsId")
      .save()
  }


  //将mysql中的四级标签的rule  封装成HBaseMeta
  def toHBaseMeta(KVMap: Map[String, String]): HBaseMeta = {
    HBaseMeta(
      KVMap.getOrElse("inType", ""),
      KVMap.getOrElse("zkHosts", ""),
      KVMap.getOrElse("zkPort", ""),
      KVMap.getOrElse("hbaseTable", ""),
      KVMap.getOrElse("family", ""),
      KVMap.getOrElse("selectFields", ""),
      KVMap.getOrElse("rowKey", "")

    )


  }
}
