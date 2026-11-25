package com.ahmedmasood.deadlock.core

import java.io.File

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/** Typed view of conf/pipeline.yaml. */
final case class PipelineConfig(
    catalog: CatalogConfig,
    @JsonProperty("landing_dir") landingDir: String,
    @JsonProperty("contracts_dir") contractsDir: String,
    @JsonProperty("player_hash_salt_env") playerHashSaltEnv: String,
    spark: SparkTuning,
    quality: QualityConfig,
    sources: Seq[SourceConfig]
) {
  def source(name: String): SourceConfig =
    sources.find(_.name == name).getOrElse(throw new IllegalArgumentException(s"Unknown source '$name'"))

  /** Salt for hashing player ids. A fixed dev salt is fine locally; production reads a secret. */
  def playerHashSalt: String = sys.env.getOrElse(playerHashSaltEnv, "local-dev-salt")
}

final case class CatalogConfig(
    name: String,
    @JsonProperty("type") catalogType: String,
    warehouse: String,
    uri: Option[String]
)

final case class SparkTuning(@JsonProperty("shuffle_partitions") shufflePartitions: Int)

final case class QualityConfig(@JsonProperty("block_publish_on") blockPublishOn: String)

final case class SourceConfig(
    name: String,
    kind: String,
    contract: String,
    @JsonProperty("bronze_table") bronzeTable: String,
    @JsonProperty("record_key") recordKey: String,
    @JsonProperty("event_time") eventTime: Option[String],
    api: ApiConfig
)

final case class ApiConfig(
    @JsonProperty("base_url") baseUrl: String,
    endpoint: String,
    @JsonProperty("watermark_param") watermarkParam: Option[String],
    @JsonProperty("page_size") pageSize: Option[Int],
    @JsonProperty("max_pages") maxPages: Option[Int],
    params: Option[Map[String, String]]
)

object PipelineConfig {
  private[deadlock] val yaml: ObjectMapper = {
    val m = new ObjectMapper(new YAMLFactory())
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m
  }

  def load(path: String = "conf/pipeline.yaml"): PipelineConfig = {
    val f = new File(path)
    require(f.exists(), s"Config file not found: ${f.getAbsolutePath}")
    yaml.readValue(f, classOf[PipelineConfig])
  }
}
