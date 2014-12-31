package com.cloudera.dataflow.dsl

import com.google.cloud.dataflow.sdk.PipelineResult
import com.google.cloud.dataflow.sdk.testing.DataflowAssert
import com.google.cloud.dataflow.sdk.values.KV
import org.scalatest.FlatSpec

class LiterallyCountStuff extends Job {
  override def createPipeline() = {
    val inputdata: RichPCollection[String] = Create.of(List("stuff", "more stuff"))
    val splitLowerCase: RichPCollection[String] = inputdata.flatMap(_.split("\\s+")).map(_
      .toLowerCase)
    val allCounts = splitLowerCase.countAll()
    val perElemCounts = splitLowerCase.countPerElement()
    DataflowAssert.that(splitLowerCase).containsInAnyOrder("stuff", "stuff", "more")
    DataflowAssert.that(allCounts).containsInAnyOrder(3L)
    DataflowAssert.that(perElemCounts).containsInAnyOrder(KV.of("stuff", 2L), KV.of("more", 1L))
  }
}

class TestLiteralCountingJob extends FlatSpec {
  val myjob = new LiterallyCountStuff()
  val result: PipelineResult = myjob.run()
}

class IOJob extends Job {
  override def createPipeline() = {
    val input = Create.text(getClass.getResource("/wordcountex.txt").getPath)
    val perElemcount = input.flatMap(_.split("\\s+")).map(_
      .toLowerCase).countPerElement()
    val format = perElemcount.map( kv =>
      s"${kv._1} : ${kv._2.toString}"
    )
    format.writeText("out")
  }
}

class TestIOJob extends FlatSpec {
  val myjob = new IOJob
  myjob.run()

}
