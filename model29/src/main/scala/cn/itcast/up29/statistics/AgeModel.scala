package cn.itcast.up29.statistics

import java.util

import cn.itcast.up29.base.BaseModel
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Column, DataFrame, Row}

class AgeModel extends BaseModel{

  def main(args: Array[String]): Unit = {
    execute()
  }

  /**
    * 定义标签ID
    *
    * @return
    */
  override def getTagID(): Int = 274

  /**
    * 开始计算
    */
  override def compute(fiveDF: DataFrame, sourceDF: DataFrame): DataFrame = {
    import scala.collection.JavaConverters._
    import org.apache.spark.sql.functions._
    val rules: List[Row] = fiveDF.collectAsList().asScala.toList

    val replace: Column = regexp_replace(sourceDF.col("birthday"), "-", "").cast(LongType)
    val getTagID: Column = when(replace.between(19600101, 19691231), 301)
      .when(replace.between(19700101, 19791231), 302)
      .when(replace.between(19800101, 19891231), 303)
      .when(replace.between(19900101, 19901231), 304)
      .when(replace.between(20000101, 20101231), 305)
    sourceDF.select(
      sourceDF.col("id") as "userid",
      getTagID as "tagIds"
    )
  }
}
