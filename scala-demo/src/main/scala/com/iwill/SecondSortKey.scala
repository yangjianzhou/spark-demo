package com.iwill

class SecondSortKey(val first: Int, val second: Int) extends Ordered[SecondSortKey] with Serializable {

  override def compare(that: SecondSortKey): Int = {

    if (this.first - that.first != 0) {
      this.first - that.first
    } else {
      that.second - this.second
    }

  }
}
