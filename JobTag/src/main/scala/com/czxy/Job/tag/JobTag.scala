package com.czxy.Job.tag

import java.util.Properties

import com.czxy.Job.bean.{HBaseMeta, TagRule}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

// 用于开发【计算】职业的标签
object JobTag {

  //程序的入口
  def main(args: Array[String]): Unit = {
    //1、创建SparkSession对象，调用sparkSQL【用于读取mysql、hbase数据】
    val spark: SparkSession = SparkSession.builder().appName("JobTag").master("local[*]").getOrCreate()

    //2、连接mysql数据库
    //jdbc(url: String, table: String, properties: Properties)
    var url = "jdbc:mysql://bd001:3306/tags_new?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&user=root&password=123456"
    var table = "tbl_basic_tag" //mysql数据表的表名
    var properties: Properties = new Properties()
    val mysqlConn: DataFrame = spark.read.jdbc(url, table, properties)

    //用于步骤3：引入隐式转换
    import spark.implicits._
    //用于步骤4：引入scala 和 java 相互之间的转换
    import scala.collection.JavaConverters._
    //用于步骤6：自定义函数 udf
    import org.apache.spark.sql.functions._

    //3、读取四级标签数据
    val fourTagsDS: Dataset[Row] = mysqlConn.select("rule").where("id=64")

    val fourTagsMap: Map[String, String] = fourTagsDS.map(row => {
      //先使用  "##" 切分，再使用 "=" 切分
      row
        .getAs("rule")
        .toString.split("##")
        .map(kv => {
          val arr: Array[String] = kv.split("=")
          (arr(0), arr(1))
        })
    }).collectAsList().get(0).toMap

    //3.1、将 fourTagsMap 转换成样例类 HBaseMeta
    var hbaseMeta: HBaseMeta = toHBaseMeta(fourTagsMap)
    //输出 查看效果
    //id,job
    println(hbaseMeta.selectFields)

    //4、读取五级标签数据
    val fiveTagsDS: Dataset[Row] = mysqlConn.select("id", "rule").where("pid=64")
    //封装成 TagRule 样例类
    val fiveTagsList: List[TagRule] = fiveTagsDS.map(row => {
      val id: Int = row.getAs("id").toString.toInt
      val rule: String = row.getAs("rule").toString
      //封装
      TagRule(id, rule)
    }).collectAsList().asScala.toList

    //输出 查看效果
    //65  1
    //66  2
    //67  3
    //68  4
    //69  5
    //70  6
    for (a <- fiveTagsList) {
      println(a.id + "  " + a.rule)
    }

    //5、读取hbase数据
    val hbaseDatas: DataFrame = spark.read.format("com.czxy.Job.tools.HBaseDataSource")
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
    val jobNewTags: DataFrame = hbaseDatas.select('id.as("userId"), getTags('job).as("tagsId"))
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
    var getAllTags=udf((oldTagsId:String,newTagsId:String)=>{
      if (oldTagsId==""){
        newTagsId
      }else if (newTagsId==""){
        oldTagsId
      }else if(oldTagsId=="" && newTagsId==""){
        ""
      }else{
        //拼接历史数据和新数据(可能有重复的数据)
        val allTags=oldTagsId+","+newTagsId
        //对重复数据去重,使用逗号分隔，返回字符串类型数据
        allTags.split(",").distinct.mkString(",")
      }
    })

    //7、解决数据覆盖的问题【职业标签会覆盖前面所有的标签】
    //读取 hbase中test表中的 历史标签数据，追加新计算出来的标签到历史数据，最后覆盖写入hbase
    //a、读取 hbase中test表中的 历史标签数据【不是职业标签，是已经计算出来的其他标签】
    val oldTags: DataFrame = spark.read.format("com.czxy.Job.tools.HBaseDataSource")
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts) //192.168.10.20
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort) //2181
      .option(HBaseMeta.HBASETABLE, "test")
      .option(HBaseMeta.FAMILY, "detail")
      .option(HBaseMeta.SELECTFIELDS, "userId,tagsId")
      .load()
    oldTags.show()

    //b、追加新计算出来的标签到历史数据
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
    allTags.write.format("com.czxy.Job.tools.HBaseDataSource")
          .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
          .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
          .option(HBaseMeta.HBASETABLE, "test")
          .option(HBaseMeta.FAMILY, "detail")
          .option(HBaseMeta.SELECTFIELDS, "userId,tagsId")
          .save()
      }


    //3.1步骤  将 fourTagsMap 转换成样例类 HBaseMeta
    def toHBaseMeta(fourTagsMap: Map[String, String]): HBaseMeta = {
      HBaseMeta(
        fourTagsMap.getOrElse(HBaseMeta.INTYPE, ""),
        fourTagsMap.getOrElse(HBaseMeta.ZKHOSTS, ""),
        fourTagsMap.getOrElse(HBaseMeta.ZKPORT, ""),
        fourTagsMap.getOrElse(HBaseMeta.HBASETABLE, ""),
        fourTagsMap.getOrElse(HBaseMeta.FAMILY, ""),
        fourTagsMap.getOrElse(HBaseMeta.SELECTFIELDS, ""),
        fourTagsMap.getOrElse(HBaseMeta.ROWKEY, "")
      )
    }
}

