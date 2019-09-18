package cn.wrp

/**
  * * @Author LYleonard
  * * @Date 2019/9/18 15:22
  * * @Description 自定义的二次排序scala版本的key
  **/
class SecondarySortKey(val first: Int, val second: Int) extends Ordered[SecondarySortKey] with Serializable {
  override def compare(that: SecondarySortKey): Int = {
    if (this.first - that.first != 0){
      this.first - that.first
    } else {
      this.second - that.second
    }
  }
}
