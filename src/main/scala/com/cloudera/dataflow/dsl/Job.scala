package com.cloudera.dataflow.dsl

import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import com.google.cloud.dataflow.sdk.transforms.{Create => DataflowCreate}
import com.google.cloud.dataflow.sdk.values.{KV, PCollection}

import scala.collection.JavaConversions

// Wrapper class for defining extra methods on PCollections
class RichPCollection[S](val pc: PCollection[S]) {

}

object Create {
  def of[T](iter: Iterable[T])(implicit p: Pipeline): PCollection[T]= {
    p.apply(DataflowCreate.of(JavaConversions.asJavaIterable(iter)))
  }

}

class Job() {
  /** Default pipeline options. Override this value for alternate options. */
  val pipelineOptions: PipelineOptions = PipelineOptionsFactory.create()
  implicit val pipeline: Pipeline = Pipeline.create(pipelineOptions)

  /** Override this method to define a new, better pipeline. */
  def createPipeline(): Pipeline = {pipeline}

  def run(): Unit = {
    createPipeline()
    pipeline.run()
  }
  implicit def pCollectionToRichPCollection[S](pc: PCollection[S]) = new RichPCollection[S](pc)

  implicit def tuple2kv[K, V](kv: KV[K, V]) = (kv.getKey, kv.getValue)

  implicit def kv2tuple2[K, V](kv: (K, V)) = KV.of(kv._1, kv._2)
}
