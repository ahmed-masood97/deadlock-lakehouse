package com.ahmedmasood.deadlock

import com.ahmedmasood.deadlock.contracts.{Contract, ContractValidator, SchemaDrift}
import org.apache.spark.sql.types.{ArrayType, LongType, StructType}
import org.scalatest.funsuite.AnyFunSuite

class ContractSpec extends AnyFunSuite {
  private lazy val spark = SharedSpark.spark
  private lazy val matches = Contract.load(SharedSpark.config, "matches.v1.yaml")

  private def player(slot: Int, team: String, hero: Any = 1): String =
    s"""{"account_id": ${1000 + slot}, "player_slot": $slot, "hero_id": $hero, "team": "$team", "kills": 3, "deaths": 2,
       | "items": [{"game_time_s": 60, "item_id": 123}], "final_stats": {"net_worth": 1000}}""".stripMargin

  private def matchJson(winning: String = "Team0", duration: Int = 1800, players: Seq[String] = (1 to 12).map(i => player(i, if (i <= 6) "Team0" else "Team1"))): String =
    s"""{"match_id": 42, "start_time": "2026-10-01 12:00:00", "duration_s": $duration, "winning_team": "$winning",
       | "match_mode": "Ranked", "game_mode": "Normal", "players": [${players.mkString(",")}]}""".stripMargin.replace("\n", " ")

  private def violationsOf(json: String): Seq[String] = {
    import spark.implicits._
    val df = Seq(("k", json)).toDF("record_key", "payload")
    val (valid, invalid) = ContractValidator.parseAndValidate(df, matches)
    if (valid.count() == 1) Nil else invalid.select("_violations").as[Seq[String]].head()
  }

  test("contract compiles to a nested Spark schema") {
    val players = matches.sparkSchema("players").dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    assert(players("account_id").dataType == LongType)
    assert(players("items").dataType.isInstanceOf[ArrayType])
    assert(players("final_stats").dataType.isInstanceOf[StructType])
  }

  test("PII fields are discoverable from the contract") {
    assert(matches.piiPaths == Seq("players[].account_id"))
  }

  test("a record that honours the contract has no violations") {
    assert(violationsOf(matchJson()) == Nil)
  }

  test("enum and range violations are reported with the field path") {
    val v = violationsOf(matchJson(winning = "Spectator", duration = 5))
    assert(v.contains("winning_team: not in [Team0,Team1]"))
    assert(v.contains("duration_s: below min 60.0"))
  }

  test("rules inside arrays of structs are checked without exploding") {
    val badPlayers = (1 to 11).map(i => player(i, "Team0")) :+ player(12, "Team9", hero = "null")
    val v = violationsOf(matchJson(players = badPlayers))
    assert(v.contains("players[].hero_id: required"))
    assert(v.contains("players[].team: not in [Team0,Team1]"))
  }

  test("array size limits are enforced") {
    assert(violationsOf(matchJson(players = Seq(player(1, "Team0")))).contains("players: fewer than 2 items"))
  }

  test("malformed JSON gets one clear reason") {
    assert(violationsOf("""{"match_id": 1, "start_time": "2026-""") == Seq("payload: malformed JSON"))
  }

  test("schema drift reports undeclared fields at every level") {
    import spark.implicits._
    val withExtra = matchJson().replaceFirst("\\{", """{"rewards_eligible": true, """)
      .replace("\"kills\": 3", "\"kills\": 3, \"hero_xp\": 10")
    val df = Seq(("42", withExtra), ("43", matchJson())).toDF("record_key", "payload")
    val findings = SchemaDrift.detect(df, matches).filter(_.change == "added")
    assert(findings.map(_.fieldPath).toSet == Set("rewards_eligible", "players[].hero_xp"))
    assert(findings.find(_.fieldPath == "rewards_eligible").get.recordsAffected == 1)
  }

  test("every bundled contract loads and declares a primary key") {
    Seq("matches.v1.yaml", "heroes.v1.yaml", "items.v1.yaml", "patches.v1.yaml").foreach { f =>
      val c = Contract.load(SharedSpark.config, f)
      assert(c.primaryKey.nonEmpty, f)
      assert(ContractValidator.describe(c).nonEmpty, f)
    }
  }
}
