package com.daslab.czy.model

class Tuple(val spans: Seq[Span]) extends Serializable {

    def addSpan(newSpan: Span): Seq[Span] ={
      val res = spans:+newSpan
      res
    }

  def combineWithTuple(tupleA: Seq[Span]): Seq[Span] ={
    val res = spans ++ tupleA
    res
  }

  override def toString: String = {
    var res = ""
    for(span <- spans){
      res += span.toString
    }
    res
  }
}
