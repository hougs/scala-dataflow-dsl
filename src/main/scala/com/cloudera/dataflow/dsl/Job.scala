package com.cloudera.dataflow.dsl

import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.coders.{Coder, CoderRegistry}
import com.google.cloud.dataflow.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import com.google.cloud.dataflow.sdk.transforms.{Create => DataflowCreate, Count, DoFn,
PTransform, ParDo}
import com.google.cloud.dataflow.sdk.values.{KV, PCollection}
import com.google.common.reflect.TypeToken

import scala.collection.JavaConversions
import scala.reflect.ClassTag


// Wrapper class for defining extra methods on PCollections
class RichPCollection[S](val pc: PCollection[S]) {

  // Initialize a coder registry; we'll likely want some Scala types in here at some point
  lazy val coders = {
    val registry = new CoderRegistry
    registry.registerStandardCoders()
    registry
  }

  // Coder lookup
  def getCoder[T](ct: ClassTag[T]): Coder[T] = {
    return coders.getDefaultCoder(TypeToken.of(ct.runtimeClass)).asInstanceOf[Coder[T]]
  }

  def flatMap[T: ClassTag](f: S => TraversableOnce[T]): PCollection[T] = {
    val doFunction: DoFn[S, T] = new DoFn[S, T] {
      override def processElement(context: DoFn[S, T]#ProcessContext): Unit = {
        for (x <- f(context.element())){
          context.output(x)
        }
      }
    }
    val flatMapTransform = new PTransform[PCollection[S],
      PCollection[T]]() {
      override def apply(input: PCollection[S]) = {
        input.apply(ParDo.of(doFunction)).setCoder(getCoder(implicitly[ClassTag[T]]))
      }
    }
    pc.apply(flatMapTransform)
  }

  def count[T]() = {
    val countTransform = Count.globally()
    pc.apply(countTransform)
  }

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
