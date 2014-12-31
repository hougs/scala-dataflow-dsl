package com.cloudera.dataflow.dsl

import org.scalatest.FlatSpec

class ExampleJob extends Job {
  override def createPipeline() = {
    val inputdata: RichPCollection[String] = Create.of(List("stuff", "more stuff"))
    implicitly(pipeline)
  }
}

class TestJob extends FlatSpec {
  val myjob = new ExampleJob()
  myjob.run()
}
