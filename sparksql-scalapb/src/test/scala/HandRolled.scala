import com.example.protos.oneof.OneOfTest.OneOfTestLens
import com.example.protos.oneof.{ABValue, AValue, BValue, OneOfTest}
import org.apache.spark.sql.types._
import org.apache.spark.scalapb_hack.{MessageUDT, MessageUDTRegistry}
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.unsafe.types.UTF8String
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}
import scalapb.spark.ProtoSQL

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

class HandRolledUDT_InnerOf(implicit
  tt: TypeTag[ABValue.InnerOf],
  ct: ClassTag[ABValue.InnerOf]
) extends MessageUDT[ABValue.InnerOf] {

  val aType = ProtoSQL.schemaFor[AValue]
  val bType = ProtoSQL.schemaFor[BValue]

  override def sqlType: DataType = {

    val aType = ProtoSQL.schemaFor[AValue]
    val bType = ProtoSQL.schemaFor[BValue]

    val struct = StructType(Array.empty[StructField])
    val structWithElements = struct
      .add(StructField("a", /*aType*/StringType, nullable = true))
      .add(StructField("b", /*bType*/IntegerType, nullable = true))
    structWithElements

    //aType

//    import org.apache.spark.sql.catalyst.ScalaReflection._
//    val dataType = dataTypeFor[T forSome { type T <: ABValue.InnerOf }]
//    dataType

    //StringType
  }

  override def serialize(obj: ABValue.InnerOf): Any = {
    val aObj = obj.asInstanceOf[ABValue.InnerOf.A]
    val aVal = aObj.value
    val aDat = ProtoSQL.messageToData(aVal)
    val aRow = ProtoSQL.messageToRow(aVal)

//    aRow

    val res: Array[Any] = obj match {
      case ABValue.InnerOf.Empty    => Array()
      case ABValue.InnerOf.A(value) => Array(ProtoSQL.messageToRow(value), null)
      case ABValue.InnerOf.B(value) => Array(null, ProtoSQL.messageToRow(value))
    }

//    val gen = new GenericArrayData(res.productIterator.toArray)
//    gen

    val row = new GenericInternalRow(2)
    (0 until 2).foreach(t =>
      row.setNullAt(t)
    )
    obj match {
      case ABValue.InnerOf.Empty    =>
      case ABValue.InnerOf.A(value) => row.update(0,
//        ProtoSQL.messageToData(value)
        UTF8String.fromString(value.strValue)
      )
      case ABValue.InnerOf.B(value) => row.update(1,
//        ProtoSQL.messageToData(value)
        value.intValue
      )
    }
    row



//    val row = new GenericInternalRow(res)
//    row

    //res.productIterator.toList.head

//    val msg = obj.value.asInstanceOf[T forSome { type T <: GeneratedMessage with Message[T] }]
//    val row = ProtoSQL.messageToData(msg)
//    val gRow = new GenericInternalRow(row.toArray)
//    gRow
  }

  override def deserialize(datum: Any): ABValue.InnerOf = {
    val res = datum match {
      case row: InternalRow =>

        val _a = row.get(0, aType)
        val _b = row.get(0, bType)


        val a =
          if (row.isNullAt(0)) null
          else ABValue.InnerOf.A(AValue(
            row.getString(0)
          ))

        val b =
          if (row.isNullAt(1)) null
          else ABValue.InnerOf.B(BValue(
            row.getInt(1)
          ))



        //val str = row.getUTF8String(0)

        val r = Option(a).getOrElse(b)
        r

      case other => {
        val a = other
        null
      }
    }

    res
  }

  override def userClass: Class[ABValue.InnerOf] = {
    val value = ct.runtimeClass
    val tValue = value.asInstanceOf[Class[ABValue.InnerOf]]
    tValue
  }
}

object HandRolled {
  class InnerABValue extends HandRolledUDT_InnerOf

  def register(): Unit = { }

  import scala.reflect.runtime.universe.typeOf

  MessageUDTRegistry.register(
    ScalaReflection.getClassNameFromType(typeOf[ABValue.InnerOf]),
    classOf[InnerABValue].getName
  )

  MessageUDTRegistry.register(
    classOf[ABValue.InnerOf].getName,
    classOf[InnerABValue].getName
  )
}
