package com.ahmedmasood.deadlock.core

/** Every table the platform owns. Names are relative to the configured catalog. */
object Tables {
  // Bronze: raw payloads, append-only, exactly as received.
  val RawMatches = "bronze.raw_matches"
  val RawHeroes  = "bronze.raw_heroes"
  val RawItems   = "bronze.raw_items"
  val RawPatches = "bronze.raw_patches"

  // Silver: parsed, contract-validated, deduplicated, conformed types.
  val Matches       = "silver.matches"
  val MatchPlayers  = "silver.match_players"
  val ItemPurchases = "silver.item_purchases"
  val Quarantine    = "silver.quarantine"

  // Gold: dimensional model (published through write-audit-publish).
  val DimHero          = "gold.dim_hero"
  val DimItem          = "gold.dim_item"
  val DimPatch         = "gold.dim_patch"
  val DimPlayer        = "gold.dim_player"
  val FctPlayerMatch   = "gold.fct_player_match"
  val FctItemPurchase  = "gold.fct_item_purchase"
  val FctPlayerEconomy = "gold.fct_player_economy"

  // Marts: business-facing aggregates built in SQL.
  val MartHeroPatch    = "gold.mart_hero_patch_performance"
  val MartItemEconomy  = "gold.mart_item_unit_economics"
  val MartEconomyDaily = "gold.mart_economy_daily"

  // Ops: the platform's own observability and state.
  val JobRuns          = "ops.job_runs"
  val DqResults        = "ops.dq_results"
  val Watermarks       = "ops.watermarks"
  val Lineage          = "ops.lineage"
  val SchemaDrift      = "ops.schema_drift"
  val SchemaMigrations = "ops.schema_migrations"
  val BackfillDiffs    = "ops.backfill_diffs"

  /** Gold tables written inside a write-audit-publish branch, in publish order (dims before facts). */
  val WapTables: Seq[String] = Seq(
    DimHero, DimItem, DimPatch, DimPlayer,
    FctPlayerMatch, FctItemPurchase, FctPlayerEconomy,
    MartHeroPatch, MartItemEconomy, MartEconomyDaily
  )

  /** Unknown-member surrogate key used when a fact arrives before its dimension row. */
  val UnknownSk: Long = -1L
}
