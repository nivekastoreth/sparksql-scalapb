package scalapb

import com.google.protobuf.Descriptors._
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.compiler.PluginProtos.{CodeGeneratorRequest, CodeGeneratorResponse}

import scala.collection.JavaConverters._
import scalapb.compiler.{DescriptorPimps, FunctionalPrinter, GeneratorParams}
import scalapb.options.compiler.Scalapb


class UdtGeneratorHandler(request: CodeGeneratorRequest, flatPackage: Boolean = false) extends DescriptorPimps {
  val params = GeneratorParams(
    flatPackage = flatPackage ||
      request.getParameter.split(",").contains("flat_package"))

  def generate: CodeGeneratorResponse = {
    val fileDescByName: Map[String, FileDescriptor] =
      request.getProtoFileList.asScala.foldLeft[Map[String, FileDescriptor]](Map.empty) {
        case (acc, fp) =>
          val deps = fp.getDependencyList.asScala.map(acc)
          acc + (fp.getName -> FileDescriptor.buildFrom(fp, deps.toArray))
      }

    val b = CodeGeneratorResponse.newBuilder

    request.getFileToGenerateList.asScala.foreach {
      name =>
        val fileDesc = fileDescByName(name)
        val responseFile = generateFile(fileDesc)
        b.addFile(responseFile)
    }
    b.build()
  }

  def allEnums(f: FileDescriptor): Seq[EnumDescriptor] = {
    f.getEnumTypes.asScala ++ f.getMessageTypes.asScala.flatMap(allEnums)
  }

  def allEnums(f: Descriptor): Seq[EnumDescriptor] = {
    f.getEnumTypes.asScala ++ f.nestedTypes.flatMap(allEnums)
  }

  def allOneOfs(f: FileDescriptor): Seq[OneofDescriptor] = {
    f.getMessageTypes.asScala.flatMap(allOneOfs)
  }

  def allOneOfs(f: Descriptor): Seq[OneofDescriptor] = {
    f.getOneofs.asScala ++ f.nestedTypes.flatMap(allOneOfs)
  }

  def udtName(d: EnumDescriptor): String = d.scalaTypeName.replace(".", "__")

  def udtName(d: OneofDescriptor): String = d.scalaTypeName.replace(".", "__")

  def generateEnum(fp: FunctionalPrinter, d: EnumDescriptor): FunctionalPrinter = {
    fp.add(s"class ${udtName(d)}")
      .indent
      .add(s"extends _root_.org.apache.spark.scalapb_hack.GeneratedEnumUDT[${d.scalaTypeName}]")
      .outdent
  }

  def generateOneOf(fp: FunctionalPrinter, d: OneofDescriptor): FunctionalPrinter = {
    fp.add(s"class ${udtName(d)}")
      .indent
      .add(s"extends _root_.org.apache.spark.scalapb_hack.GeneratedOneOfUDT[${d.scalaTypeName}]")
      .outdent
  }

  def generateFile(fileDesc: FileDescriptor): CodeGeneratorResponse.File = {
    val b = CodeGeneratorResponse.File.newBuilder()
    b.setName(s"${fileDesc.scalaDirectory}/${fileDesc.fileDescriptorObjectName}Udt.scala")
    val fp = FunctionalPrinter()
      .add(s"package ${fileDesc.scalaPackageName}")
      .add("")
      .add(s"object ${fileDesc.fileDescriptorObjectName}Udt {")
      .indent
      .add("// Declare Enum UDTs")
      .print(allEnums(fileDesc))(generateEnum)
      .newline
      .add("// Declare OneOf UDTs")
      .print(allOneOfs(fileDesc))(generateOneOf)
      .newline
      .add("def register(): Unit = { } // actual work happens at the constructor.")
      .add("")
      .print(fileDesc.getDependencies.asScala)(
        (fp, dep) => fp.add(s"${dep.scalaPackageName}.${dep.fileDescriptorObjectName}Udt.register()")
      )
      .newline
      .add("// Register Enum UDTs")
      .print(allEnums(fileDesc))(
        (fp, d) => fp
          .add(s"""_root_.org.apache.spark.scalapb_hack.MessageUDTRegistry.register(""")
          .indent
          .add(s"""classOf[${d.scalaTypeName}].getName,""")
          .add(s"""classOf[${fileDesc.scalaPackageName}.${fileDesc.fileDescriptorObjectName}Udt.${udtName(d)}].getName""")
          .outdent
          .add(")")
          .add("")
      )
      .newline
      .add("// Register OneOf UDTs")
      .print(allOneOfs(fileDesc))(
        (fp, d) => fp
          .add(s"""_root_.org.apache.spark.scalapb_hack.MessageUDTRegistry.register(""")
          .indent
          .add(s"""org.apache.spark.sql.catalyst.ScalaReflection.getClassNameFromType(""")
          .indent
          .add(s"""scala.reflect.runtime.universe.typeOf[${d.scalaTypeName}]),""")
          .outdent
          .add(s"""classOf[${fileDesc.scalaPackageName}.${fileDesc.fileDescriptorObjectName}Udt.${udtName(d)}].getName""")
          .outdent
          .add(")")
          .add("")
          .add(s"""_root_.org.apache.spark.scalapb_hack.MessageUDTRegistry.register(""")
          .indent
          .add(s"""classOf[${d.scalaTypeName}].getName,""")
          .add(s"""classOf[${fileDesc.scalaPackageName}.${fileDesc.fileDescriptorObjectName}Udt.${udtName(d)}].getName""")
          .outdent
          .add(")")
          .add("")
      )
      .newline
      .add("// ===> Test 4")
      .newline
      .print(allOneOfs(fileDesc))(
        (printer, oneOf: OneofDescriptor) => {
          val odp = new OneofDescriptorPimp(oneOf)
          println(oneOf)
          println(odp)

          val a = 0

          printer
            .add(
              s"""
                 | OneOf           : ${oneOf.getName}
                 |   fullName      : ${oneOf.getFullName}
                 |   scalaTypeName : ${oneOf.scalaTypeName}
                 |   scalaTypeName : ${oneOf.scalaTypeName}
                 |   scalaName     : ${oneOf.scalaName}
                 |   fields        :
                 |     ${oneOf.getFields.asScala.map(f => s"${f.getName} => javaType:${f.getJavaType}").mkString("\n     ")}
                 |
               """.stripMargin.split("\n").map(s => s"// $s") : _*
            )
            .newline
        }
      )
      .outdent
      .add("}")

    b.setContent(fp.result)
    b.build
  }
}

class UdtGenerator(flatPackage: Boolean) extends protocbridge.ProtocCodeGenerator {
  def run(requestBytes: Array[Byte]): Array[Byte] = {
    val registry = ExtensionRegistry.newInstance()
    Scalapb.registerAllExtensions(registry)
    val request = CodeGeneratorRequest.parseFrom(requestBytes, registry)
    new UdtGeneratorHandler(request, flatPackage).generate.toByteArray
  }

}

object UdtGenerator extends UdtGenerator(flatPackage = false)
