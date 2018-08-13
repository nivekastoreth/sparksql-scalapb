package org.apache.spark.scalapb_hack

import org.apache.spark.sql.types.{UDTRegistration, UserDefinedType}

abstract class MessageUDT[MessageType >: Null] extends UserDefinedType[MessageType]

object MessageUDTRegistry {
  def register(a: String, b: String): Unit = {
    UDTRegistration.register(a, b)
  }
}
