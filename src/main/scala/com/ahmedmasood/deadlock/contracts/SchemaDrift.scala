package com.ahmedmasood.deadlock.contracts

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/** A field the producer sent that the contract does not know about (or vice versa). */
final case class DriftFinding(fieldPath: String, change: String, recordsAffected: Long, sampleRecordKey: String)

/**
 * Detects schema drift between what a producer actually sends and what its contract declares.
 *
 * For every object level in the contract (the root, each struct, the first element of each array
 * of structs) it collects the JSON keys present in the raw payloads and compares them with the
 * declared fields. Additive changes are normal for a live game API (new stats appear with patches),
 * so they are logged rather than failing the pipeline; a contract with `evolution: strict` turns
 * them into errors.
 */
object SchemaDrift {

  /** (JSON path to an object level, declared field names at that level, display prefix). */
  private[contracts] def levels(contract: Contract): Seq[(String, Set[String], String)] = {
    def walk(jsonPath: String, display: String, fs: Seq[FieldSpec]): Seq[(String, Set[String], String)] = {
      val here = (jsonPath, fs.map(_.name).toSet, display)
      here +: fs.flatMap { f =>
        val prefix = if (display.isEmpty) f.name else s"$display.${f.name}"
        if (f.isArrayOfStruct) walk(s"$jsonPath.${f.name}[0]", s"$prefix[]", f.children)
        else if (f.isStruct) walk(s"$jsonPath.${f.name}", prefix, f.children)
        else Nil
      }
    }
    walk("$", "", contract.fields)
  }

  def detect(raw: DataFrame, contract: Contract, payloadCol: String = "payload", keyCol: String = "record_key"): Seq[DriftFinding] = {
    val findings = levels(contract).flatMap { case (path, declared, display) =>
      val obj: Column = if (path == "$") col(payloadCol) else get_json_object(col(payloadCol), path)
      val observed = raw
        .select(col(keyCol).as("k"), explode_outer(json_object_keys(obj)).as("field"))
        .where(col("field").isNotNull)
        .groupBy("field")
        .agg(count(lit(1)).as("n"), min("k").as("sample"))
        .collect()
        .map(r => (r.getString(0), r.getLong(1), Option(r.getString(2)).getOrElse("")))

      val added = observed.filterNot { case (f, _, _) => declared.contains(f) }.map { case (f, n, sample) =>
        DriftFinding(if (display.isEmpty) f else s"$display.$f", "added", n, sample)
      }
      // A declared field never seen in a non-empty batch is worth knowing about (possible removal).
      val seen = observed.map(_._1).toSet
      val missing =
        if (observed.isEmpty) Nil
        else (declared -- seen).toSeq.sorted.map(f => DriftFinding(if (display.isEmpty) f else s"$display.$f", "absent_in_batch", 0L, ""))
      added.toSeq ++ missing
    }
    findings.sortBy(f => (f.change, f.fieldPath))
  }
}
