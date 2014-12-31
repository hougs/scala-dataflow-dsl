scala-dataflow-dsl
==================

A Scala Interface for dataflow

This library allows you to write dataflow jobs in a native scala collections like way, by
implementing a Job class. For example, we can count words in a list of strings by running the
following job class:

    class LiterallyCountStuff extends Job {
      override def createPipeline() = {
        val inputdata: RichPCollection[String] = Create.of(List("stuff", "more stuff"))
        val splitLowerCase: RichPCollection[String] = inputdata.flatMap(_.split("\\s+")).map(_
          .toLowerCase)
        val allCounts = splitLowerCase.countAll()
        val perElemCounts = splitLowerCase.countPerElement()
      }
    }