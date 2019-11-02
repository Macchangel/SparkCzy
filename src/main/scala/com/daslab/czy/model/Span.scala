package com.daslab.czy.model

class Span(val begin: Int, val end: Int) extends Serializable {
  override def toString: String = "<" + begin + "," + end + ">"
//    "<begin at: >" + begin + "(inclusive); end at: " + end + "(exclusive)."
}
