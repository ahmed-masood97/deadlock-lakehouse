package com.ahmedmasood.deadlock.ingest

import java.io.File
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.hashing.MurmurHash3

import com.ahmedmasood.deadlock.core.{Log, PipelineConfig}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode

/**
 * Generates realistic landing files in exactly the shape the live API returns, so the whole
 * platform runs offline, deterministically, and in CI.
 *
 * Hero and item names are the game's; numeric ids, balance numbers and match outcomes are
 * synthetic. The generator deliberately injects the problems a production pipeline must survive:
 *
 *  - a season with three patches; patch 2 reprices every item tier (SCD2 on dim_item)
 *  - patch 3 reworks a hero, disables an item and releases a new hero (SCD2 on dim_hero)
 *  - the new hero shows up in matches one day before the hero snapshot lists it (late dimension)
 *  - duplicate matches re-sent in a later batch, and late-arriving matches (two days late)
 *  - a small share of contract-breaking records (bad enum, impossible duration, broken JSON)
 *  - new, undeclared fields appearing after patch 3 (schema drift)
 *  - a handful of players whose soul sources do not add up to net worth (reconciliation)
 */
object SyntheticDeadlock {

  final case class Hero(id: Int, name: String, heroType: String, complexity: Int, maxHealth: Double)
  final case class Item(id: Long, className: String, name: String, slot: String, tier: Int, itemType: String = "upgrade")
  final case class Patch(day: Int, title: String)

  private val mapper = new ObjectMapper()
  private val tsFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  private def h(id: Int, name: String, t: String, c: Int, hp: Double) = Hero(id, name, t, c, hp)
  val baseHeroes: Seq[Hero] = Seq(
    h(1, "Infernus", "mystic", 1, 600), h(2, "Seven", "mystic", 1, 620), h(3, "Vindicta", "marksman", 2, 520),
    h(4, "Lady Geist", "mystic", 2, 640), h(6, "Abrams", "brawler", 1, 780), h(7, "Wraith", "marksman", 2, 560),
    h(8, "McGinnis", "marksman", 1, 600), h(10, "Paradox", "assassin", 3, 580), h(11, "Dynamo", "mystic", 2, 640),
    h(12, "Kelvin", "mystic", 2, 700), h(13, "Haze", "assassin", 2, 540), h(15, "Bebop", "brawler", 2, 720),
    h(17, "Grey Talon", "marksman", 2, 540), h(18, "Mo & Krill", "brawler", 3, 760), h(19, "Shiv", "assassin", 2, 650),
    h(20, "Ivy", "mystic", 2, 560), h(25, "Warden", "brawler", 1, 740), h(27, "Yamato", "assassin", 3, 600),
    h(31, "Lash", "brawler", 3, 680), h(35, "Viscous", "mystic", 3, 660)
  )
  val newHero: Hero = h(58, "Holliday", "marksman", 2, 580) // released in patch 3

  private def item(name: String, slot: String, tier: Int): Item = {
    val cls = "upgrade_" + name.toLowerCase.replaceAll("[^a-z]+", "_")
    Item((MurmurHash3.stringHash(cls) & 0x7fffffff).toLong, cls, name, slot, tier)
  }
  val items: Seq[Item] = Seq(
    item("Basic Magazine", "weapon", 1), item("Close Quarters", "weapon", 1), item("Headshot Booster", "weapon", 1),
    item("High-Velocity Mag", "weapon", 1), item("Active Reload", "weapon", 2), item("Fleetfoot", "weapon", 2),
    item("Kinetic Dash", "weapon", 2), item("Long Range", "weapon", 2), item("Burst Fire", "weapon", 3),
    item("Tesla Bullets", "weapon", 3), item("Toxic Bullets", "weapon", 3), item("Alchemical Fire", "weapon", 3),
    item("Crippling Headshot", "weapon", 4), item("Glass Cannon", "weapon", 4), item("Ricochet", "weapon", 4),
    item("Extra Health", "vitality", 1), item("Extra Regen", "vitality", 1), item("Healing Rite", "vitality", 1),
    item("Sprint Boots", "vitality", 1), item("Bullet Lifesteal", "vitality", 2), item("Enduring Spirit", "vitality", 2),
    item("Healbane", "vitality", 2), item("Return Fire", "vitality", 2), item("Fortitude", "vitality", 3),
    item("Lifestrike", "vitality", 3), item("Veil Walker", "vitality", 3), item("Leech", "vitality", 4),
    item("Colossus", "vitality", 4), item("Unstoppable", "vitality", 4), item("Extra Charge", "spirit", 1),
    item("Mystic Burst", "spirit", 1), item("Extra Spirit", "spirit", 1), item("Mystic Reach", "spirit", 2),
    item("Improved Cooldown", "spirit", 2), item("Quicksilver Reload", "spirit", 2), item("Duration Extender", "spirit", 2),
    item("Knockdown", "spirit", 3), item("Superior Cooldown", "spirit", 3), item("Improved Spirit", "spirit", 3),
    item("Boundless Spirit", "spirit", 4), item("Echo Shard", "spirit", 4), item("Refresher", "spirit", 4)
  )
  /** Ability unlocks also appear in a player's item timeline; they are not shop purchases. */
  val abilityItems: Seq[Item] = (1 to 4).map { i =>
    val cls = s"ability_signature_$i"
    Item((MurmurHash3.stringHash(cls) & 0x7fffffff).toLong, cls, s"Signature Ability $i", "", 0, "ability")
  }
  val disabledInPatch3: Item = items.find(_.name == "Ricochet").get

  /** Patch schedule relative to the season start (day 0). */
  val patches: Seq[Patch] = Seq(Patch(-5, "Season Kickoff Update"), Patch(9, "Economy Rebalance Update"), Patch(20, "New Hero: Holliday"))

  /** Tier prices per patch: patch 2 reprices every tier (the classic SCD2 case). */
  def tierCost(patchIdx: Int, tier: Int): Long =
    if (patchIdx == 0) Map(1 -> 500L, 2 -> 1250L, 3 -> 3000L, 4 -> 6200L)(tier)
    else Map(1 -> 800L, 2 -> 1600L, 3 -> 3200L, 4 -> 6400L)(tier)

  def patchIndexForDay(day: Int): Int = patches.lastIndexWhere(_.day <= day).max(0)

  /** Hero "strength" per patch drives win rates, so patch-over-patch shifts are visible in the marts. */
  def heroStrength(hero: Hero, patchIdx: Int): Double = {
    val base = ((MurmurHash3.stringHash(hero.name) % 100) / 100.0) * 0.6 // roughly -0.6..0.6
    val patchShift = ((MurmurHash3.stringHash(s"${hero.name}#$patchIdx") % 100) / 100.0) * 0.5
    if (hero.id == 6 && patchIdx >= 2) base + 0.8 // patch 3 Abrams rework: a strong buff
    else base + patchShift
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Writes landing files for every day in [from, to]. `seasonStart` anchors the patch schedule, so
   * a range can be generated in several chunks (e.g. to demo incremental runs) with one history.
   */
  def generate(cfg: PipelineConfig, seasonStart: LocalDate, from: LocalDate, to: LocalDate, matchesPerDay: Int, seed: Long = 42L): Unit = {
    val heroesDir = Landing.sourceDir(cfg, "heroes")
    val itemsDir = Landing.sourceDir(cfg, "items")
    val patchesDir = Landing.sourceDir(cfg, "patches")
    val matchesDir = Landing.sourceDir(cfg, "matches")
    var date = from
    var totalMatches = 0
    val lateQueue = scala.collection.mutable.Map[LocalDate, ArrayBuffer[String]]()

    while (!date.isAfter(to)) {
      val day = (date.toEpochDay - seasonStart.toEpochDay).toInt
      val pIdx = patchIndexForDay(day)

      // Reference snapshots: on patch days, plus a weekly unchanged snapshot (should be a no-op for SCD2).
      if (date == from || patches.exists(_.day == day) || day == patches.last.day + 1 || day % 7 == 0) {
        val heroLines = heroSnapshot(day).map(mapper.writeValueAsString)
        Landing.writeLines(new File(heroesDir, s"snapshot_date=$date"), "part-00001.ndjson", heroLines.iterator)
        val itemLines = itemSnapshot(pIdx).map(mapper.writeValueAsString)
        Landing.writeLines(new File(itemsDir, s"snapshot_date=$date"), "part-00001.ndjson", itemLines.iterator)
        val patchLines = patches.filter(_.day <= day).map(p => mapper.writeValueAsString(patchJson(seasonStart, p)))
        Landing.writeLines(new File(patchesDir, s"snapshot_date=$date"), "part-00001.ndjson", patchLines.iterator)
      }

      val rnd = new Random(seed * 1000003L + date.toEpochDay)
      val today = ArrayBuffer[String]()
      val later = ArrayBuffer[String]()
      (0 until matchesPerDay).foreach { i =>
        val matchId = date.toEpochDay * 100000L + i
        val line = matchJson(rnd, matchId, date, day, pIdx)
        val r = rnd.nextDouble()
        if (r < 0.02) later += line // arrives two days late
        else today += line
        if (r > 0.99) lateQueue.getOrElseUpdate(date.plusDays(1), ArrayBuffer()) += line // re-sent duplicate
      }
      lateQueue.getOrElseUpdate(date.plusDays(2), ArrayBuffer()) ++= later
      today ++= lateQueue.remove(date).getOrElse(Nil)
      // A few records that break the contract, plus one line of broken JSON.
      today += brokenRecord(rnd, date, 1, "Spectator")
      today += brokenRecord(rnd, date, 2, "Team0").replace("\"duration_s\":", "\"duration_s\":-")
      if (day % 5 == 0) today += """{"match_id": 1, "start_time": "2026-"""

      today.grouped(1000).zipWithIndex.foreach { case (chunk, idx) =>
        Landing.writeLines(new File(matchesDir, s"batch=$date"), f"part-${idx + 1}%05d.ndjson", chunk.iterator)
      }
      totalMatches += today.size
      date = date.plusDays(1)
    }
    // Records still queued to arrive after `to` are flushed into the last batch so nothing is lost.
    val leftovers = lateQueue.values.flatten.toSeq
    if (leftovers.nonEmpty)
      Landing.writeLines(new File(matchesDir, s"batch=$to"), "part-99999.ndjson", leftovers.iterator)
    totalMatches += leftovers.size
    Log.info("synthetic_generated", "from" -> from, "to" -> to, "match_records" -> totalMatches, "landing" -> cfg.landingDir)
  }

  private def heroSnapshot(day: Int): Seq[ObjectNode] = {
    val pIdx = patchIndexForDay(day)
    // The new hero's reference data lands one day AFTER the hero is playable (late-arriving dimension).
    val released = day >= patches(2).day + 1
    val heroes = baseHeroes ++ (if (released) Seq(newHero) else Nil)
    heroes.map { hero =>
      val n = mapper.createObjectNode()
      val reworked = hero.id == 6 && pIdx >= 2
      n.put("id", hero.id).put("class_name", "hero_" + hero.name.toLowerCase.replaceAll("[^a-z]+", ""))
        .put("name", hero.name).put("hero_type", if (reworked) "assassin" else hero.heroType)
        .put("complexity", hero.complexity).put("player_selectable", true).put("disabled", false).put("in_development", false)
      n.putObject("starting_stats").putObject("max_health").put("value", if (reworked) hero.maxHealth + 120 else hero.maxHealth)
      n
    }
  }

  private def itemSnapshot(pIdx: Int): Seq[ObjectNode] =
    (items ++ abilityItems).map { it =>
      val n = mapper.createObjectNode()
      n.put("id", it.id).put("class_name", it.className).put("name", it.name).put("type", it.itemType)
      if (it.itemType == "upgrade") {
        n.put("item_slot_type", it.slot).put("item_tier", it.tier).put("cost", tierCost(pIdx, it.tier))
          .put("shopable", true).put("disabled", pIdx >= 2 && it.id == disabledInPatch3.id)
      } else n.put("disabled", false)
      n
    }

  private def patchJson(seasonStart: LocalDate, p: Patch): ObjectNode = {
    val n = mapper.createObjectNode()
    val pub = seasonStart.plusDays(p.day.toLong).atTime(0, 0).atOffset(ZoneOffset.UTC)
    n.put("title", p.title).put("pub_date", pub.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)).put("link", s"https://forums.playdeadlock.com/threads/${p.day + 1000}/")
      .put("author", "Yoshi").put("dc_creator", "Yoshi")
    n.putObject("guid").put("is_perma_link", false).put("text", s"patch-${seasonStart.plusDays(p.day.toLong)}")
    n.putObject("category").put("domain", "https://forums.playdeadlock.com/forums/changelog.10/").put("text", "Changelog")
    n
  }

  private def matchJson(rnd: Random, matchId: Long, date: LocalDate, day: Int, pIdx: Int): String = {
    val pool = baseHeroes ++ (if (day >= patches(2).day) Seq(newHero) else Nil)
    val picks = rnd.shuffle(pool).take(12)
    val badge = (1 + rnd.nextInt(11)) * 10 + (1 + rnd.nextInt(6))
    val strength0 = picks.take(6).map(heroStrength(_, pIdx)).sum
    val strength1 = picks.drop(6).map(heroStrength(_, pIdx)).sum
    val p0 = 1.0 / (1.0 + math.exp(-(strength0 - strength1) * 0.9))
    val winner = if (rnd.nextDouble() < p0) 0 else 1
    val duration = 1300 + rnd.nextInt(1900)
    val start = LocalDateTime.of(date, java.time.LocalTime.ofSecondOfDay(rnd.nextInt(86400).toLong))

    val m = mapper.createObjectNode()
    m.put("match_id", matchId).put("start_time", start.format(tsFmt)).put("duration_s", duration)
      .put("winning_team", s"Team$winner").put("match_outcome", "TeamWin")
      .put("match_mode", if (rnd.nextDouble() < 0.7) "Ranked" else "Unranked").put("game_mode", "Normal")
      .put("average_badge_team0", badge).put("average_badge_team1", badge + rnd.nextInt(3) - 1).put("not_scored", false)
    m.putArray("banned_hero_ids")
    if (pIdx >= 2) m.put("rewards_eligible", true) // undeclared field from patch 3 on: schema drift
    val players = m.putArray("players")
    picks.zipWithIndex.foreach { case (hero, slot) =>
      val team = if (slot < 6) 0 else 1
      val won = team == winner
      val p = players.addObject()
      val spm = 1100 + (if (won) 250 else 0) + rnd.nextInt(400) + badge * 2
      val netWorth = (spm * duration / 60.0).toLong
      val kills = rnd.nextInt(if (won) 14 else 9)
      val deaths = rnd.nextInt(if (won) 7 else 11)
      p.put("account_id", 1000000L + (math.abs(rnd.nextGaussian()) * 900).toLong + rnd.nextInt(2500))
        .put("player_slot", slot + 1).put("hero_id", hero.id).put("team", s"Team$team")
        .put("kills", kills).put("deaths", deaths).put("assists", rnd.nextInt(20)).put("net_worth", netWorth)
        .put("last_hits", 80 + rnd.nextInt(220)).put("denies", rnd.nextInt(25)).put("ability_points", 20 + rnd.nextInt(16))
        .put("assigned_lane", 1 + slot % 3).put("player_level", 20 + rnd.nextInt(16))
        .put("abandon_match_time_s", if (rnd.nextDouble() < 0.005) rnd.nextInt(duration) else 0)
        .put("mvp_rank", if (won) 1 + rnd.nextInt(3) else 0)
        .put("hero_build_id", 100000L + hero.id * 1000L + rnd.nextInt(40))
      if (pIdx >= 2) p.put("hero_xp", 1000 + rnd.nextInt(3000)) // undeclared player-level field: drift
      addItems(rnd, p.putArray("items"), hero, netWorth, duration, pIdx)
      addFinalStats(rnd, p.putObject("final_stats"), netWorth, kills)
    }
    mapper.writeValueAsString(m)
  }

  private def addItems(rnd: Random, arr: com.fasterxml.jackson.databind.node.ArrayNode, hero: Hero, netWorth: Long, duration: Int, pIdx: Int): Unit = {
    abilityItems.zipWithIndex.foreach { case (a, i) =>
      arr.addObject().put("game_time_s", 30 + i * 240).put("item_id", a.id).put("upgrade_id", 0).put("sold_time_s", 0).put("flags", 0).put("imbued_ability_id", 0)
    }
    val preferred = hero.heroType match {
      case "marksman" => Seq("weapon", "weapon", "vitality", "spirit")
      case "mystic"   => Seq("spirit", "spirit", "vitality", "weapon")
      case "brawler"  => Seq("vitality", "vitality", "weapon", "spirit")
      case _          => Seq("weapon", "spirit", "vitality")
    }
    // A typical build: about four items per tier, bought in tier order, until souls run out.
    val budget = (netWorth * (0.82 + rnd.nextDouble() * 0.12)).toLong
    var spent = 0L
    var t = 60
    val owned = scala.collection.mutable.Set[Long]()
    for (tier <- 1 to 4; _ <- 1 to 4) {
      val slot = preferred(rnd.nextInt(preferred.size))
      val available = items.filter(i => i.tier == tier && !owned.contains(i.id) && !(pIdx >= 2 && i.id == disabledInPatch3.id))
      val inSlot = available.filter(_.slot == slot)
      val pool = if (inSlot.nonEmpty) inSlot else available
      if (pool.nonEmpty) {
        val it = pool(rnd.nextInt(pool.size))
        val cost = tierCost(pIdx, it.tier)
        if (spent + cost <= budget) {
          t = math.min(duration - 5, t + 45 + rnd.nextInt(120))
          val sold = it.tier == 1 && rnd.nextDouble() < 0.25 // starter items get sold to free a slot
          arr.addObject().put("game_time_s", t).put("item_id", it.id).put("upgrade_id", 0)
            .put("sold_time_s", if (sold) math.min(duration - 1, t + 900) else 0).put("flags", 0).put("imbued_ability_id", 0)
          owned += it.id
          spent += cost
        }
      }
    }
  }

  /** Soul sources sum to net worth + souls lost on death, except for a few planted bad rows. */
  private def addFinalStats(rnd: Random, fs: ObjectNode, netWorth: Long, kills: Int): Unit = {
    val lost = (netWorth * (0.02 + rnd.nextDouble() * 0.04)).toLong
    val earned = netWorth + lost
    val shares = Seq("gold_lane_creep" -> 0.34, "gold_lane_creep_orbs" -> 0.08, "gold_neutral_creep" -> 0.17,
      "gold_neutral_creep_orbs" -> 0.04, "gold_player" -> (0.10 + kills * 0.006), "gold_player_orbs" -> 0.04,
      "gold_boss" -> 0.06, "gold_boss_orb" -> 0.02, "gold_treasure" -> 0.07, "gold_denied" -> 0.01)
    val total = shares.map(_._2).sum
    var allocated = 0L
    shares.zipWithIndex.foreach { case ((k, s), i) =>
      val v = if (i == shares.size - 1) earned - allocated else (earned * s / total).toLong
      fs.put(k, v); allocated += v
    }
    val corrupt = rnd.nextDouble() < 0.003
    fs.put("net_worth", if (corrupt) netWorth + 5000 + rnd.nextInt(5000) else netWorth)
    fs.put("gold_death_loss", lost)
    fs.put("player_damage", 8000L + rnd.nextInt(30000)).put("player_healing", rnd.nextInt(9000).toLong)
      .put("boss_damage", rnd.nextInt(12000).toLong).put("creep_damage", 20000L + rnd.nextInt(40000))
      .put("shots_hit", 300L + rnd.nextInt(900)).put("shots_missed", 400L + rnd.nextInt(1500))
  }

  private def brokenRecord(rnd: Random, date: LocalDate, n: Int, team: String): String = {
    val m = mapper.createObjectNode()
    m.put("match_id", date.toEpochDay * 100000L + 90000 + n).put("start_time", date.atTime(12, 0).format(tsFmt))
      .put("duration_s", 1800).put("winning_team", team).put("match_outcome", "TeamWin")
      .put("match_mode", "Ranked").put("game_mode", "Normal")
    val players = m.putArray("players")
    (0 until 12).foreach { s =>
      players.addObject().put("account_id", 42L + s).put("player_slot", s + 1).put("hero_id", baseHeroes(s).id).put("team", if (s < 6) "Team0" else "Team1")
    }
    mapper.writeValueAsString(m)
  }
}
