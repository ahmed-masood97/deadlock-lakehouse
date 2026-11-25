package com.ahmedmasood.deadlock.contracts

import java.io.File

import com.ahmedmasood.deadlock.core.PipelineConfig
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.spark.sql.types._

/** One field of a source contract. Nested `fields` describe structs or arrays of structs. */
final case class FieldSpec(
    name: String,
    @JsonProperty("type") dataType: String,
    required: Option[Boolean],
    accepted: Option[Seq[String]],
    @JsonDeserialize(contentAs = classOf[java.lang.Double]) min: Option[Double],
    @JsonDeserialize(contentAs = classOf[java.lang.Double]) max: Option[Double],
    @JsonProperty("min_items") @JsonDeserialize(contentAs = classOf[java.lang.Integer]) minItems: Option[Int],
    @JsonProperty("max_items") @JsonDeserialize(contentAs = classOf[java.lang.Integer]) maxItems: Option[Int],
    pii: Option[Boolean],
    format: Option[String],
    description: Option[String],
    fields: Option[Seq[FieldSpec]]
) {
  def isRequired: Boolean = required.getOrElse(false)
  def isPii: Boolean = pii.getOrElse(false)
  def children: Seq[FieldSpec] = fields.getOrElse(Nil)
  def isArrayOfStruct: Boolean = dataType == "array" && children.nonEmpty
  def isStruct: Boolean = dataType == "struct"
}

/** A versioned agreement with a data producer. See contracts/README in docs/design.md. */
final case class Contract(
    name: String,
    version: Int,
    owner: String,
    description: String,
    grain: String,
    @JsonProperty("primary_key") primaryKey: Seq[String],
    @JsonProperty("freshness_sla_hours") @JsonDeserialize(contentAs = classOf[java.lang.Integer]) freshnessSlaHours: Option[Int],
    evolution: String,
    fields: Seq[FieldSpec]
) {

  /** The Spark schema used to parse raw payloads. Derived from the contract, never hand-written. */
  lazy val sparkSchema: StructType = StructType(fields.map(f => StructField(f.name, Contract.toSparkType(f), nullable = true)))

  /** Dotted paths of fields marked as PII, e.g. players[].account_id. Used for governance checks. */
  lazy val piiPaths: Seq[String] = {
    def walk(prefix: String, fs: Seq[FieldSpec]): Seq[String] = fs.flatMap { f =>
      val path = if (prefix.isEmpty) f.name else s"$prefix.${f.name}"
      val self = if (f.isPii) Seq(path) else Nil
      val nestedPrefix = if (f.dataType == "array") s"$path[]" else path
      self ++ walk(nestedPrefix, f.children)
    }
    walk("", fields)
  }
}

object Contract {

  def load(cfg: PipelineConfig, file: String): Contract = load(new File(cfg.contractsDir, file))

  def load(file: File): Contract = {
    require(file.exists(), s"Contract not found: ${file.getAbsolutePath}")
    val c = PipelineConfig.yaml.readValue(file, classOf[Contract])
    require(c.primaryKey.nonEmpty, s"Contract ${c.name} must declare a primary key")
    require(Set("additive", "strict").contains(c.evolution), s"Unknown evolution policy '${c.evolution}'")
    c
  }

  private val ArrayOfPrimitive = """array<(\w+)>""".r

  def toSparkType(f: FieldSpec): DataType = f.dataType match {
    case "array" if f.children.nonEmpty => ArrayType(StructType(f.children.map(c => StructField(c.name, toSparkType(c)))))
    case "struct"                       => StructType(f.children.map(c => StructField(c.name, toSparkType(c))))
    case ArrayOfPrimitive(inner)        => ArrayType(primitive(inner, f.name))
    case other                          => primitive(other, f.name)
  }

  private def primitive(t: String, field: String): DataType = t match {
    case "string"    => StringType
    case "int"       => IntegerType
    case "long"      => LongType
    case "double"    => DoubleType
    case "boolean"   => BooleanType
    case "timestamp" => TimestampType
    case other       => throw new IllegalArgumentException(s"Unsupported contract type '$other' for field '$field'")
  }
}
