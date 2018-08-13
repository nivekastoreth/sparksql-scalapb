package org.apache.spark.scalapb_hack

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import scalapb.{GeneratedMessageCompanion, GeneratedOneof}

import scala.reflect.ClassTag

class GeneratedOneOfUDT[T >: Null <: GeneratedOneof](implicit ct: ClassTag[T]) extends MessageUDT[T] {
  override def sqlType: DataType = {
    // ObjectType(ct.runtimeClass)
    StructType(Array.empty[StructField])

//    StructType(Array(StructField("", ObjectType(ct.runtimeClass))))
//    ArrayType(ObjectType(ct.runtimeClass))
  }

  override def serialize(obj: T): Any = {
    // org.apache.spark.unsafe.types.UTF8String.fromString(obj.name)
    //UTF8String.fromString(a.toString)
    val value = obj.value
    val r = new GenericInternalRow(Array[Any](Some(value)))
    r
  }

  override def deserialize(datum: Any): T = {
    //cmp.fromName(datum.asInstanceOf[UTF8String].toString).get
    val a = datum
    a.asInstanceOf[T]
  }

  override def userClass: Class[T] = {
    val value = ct.runtimeClass
    val tValue = value.asInstanceOf[Class[T]]
    tValue
  }
}
