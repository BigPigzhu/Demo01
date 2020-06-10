package cn.itcast.up29.bean

case class HBaseMeta (
                       inType: String,
                       zkHosts: String,
                       zkPort: String,
                       hbaseTable: String,
                       family: String,
                       selectFields: String,
                       rowKey: String
                     )
object HBaseMeta{
  val INTYPE = "inType"
  val ZKHOSTS = "zkHosts"
  val ZKPORT = "zkPort"
  val HBASETABLE = "hbaseTable"
  val FAMILY = "family"
  val SELECTFIELDS = "selectFields"
  val ROWKEY = "rowKey"
}