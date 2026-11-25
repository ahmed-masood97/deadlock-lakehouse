package com.ahmedmasood.deadlock.ingest

import java.net.URI
import java.net.URLEncoder
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration

import scala.annotation.tailrec
import scala.util.Random

import com.ahmedmasood.deadlock.core.{ApiConfig, Log}

final case class ApiError(status: Int, body: String) extends RuntimeException(s"HTTP $status: ${body.take(300)}")

/**
 * HTTP client for the community Deadlock API (https://api.deadlock-api.com).
 *
 * Production concerns handled here rather than in every caller:
 *  - retries with exponential backoff and jitter on 429 / 5xx / network errors
 *  - honours `Retry-After` when the API rate-limits us
 *  - timeouts on connect and on every request
 *  - an identifying User-Agent, as public APIs ask
 */
class DeadlockApiClient(maxAttempts: Int = 6, baseBackoffMs: Long = 1000, maxBackoffMs: Long = 60000) {

  private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).followRedirects(HttpClient.Redirect.NORMAL).build()

  def get(api: ApiConfig, extraParams: Map[String, String] = Map.empty): String = {
    val params = api.params.getOrElse(Map.empty) ++ extraParams
    val query = params.toSeq.sortBy(_._1).map { case (k, v) => s"${enc(k)}=${enc(v)}" }.mkString("&")
    val url = s"${api.baseUrl.stripSuffix("/")}${api.endpoint}${if (query.isEmpty) "" else "?" + query}"
    send(url, attempt = 1)
  }

  @tailrec
  private def send(url: String, attempt: Int): String = {
    val request = HttpRequest.newBuilder(URI.create(url))
      .timeout(Duration.ofSeconds(120))
      .header("Accept", "application/json, application/x-ndjson")
      .header("User-Agent", "deadlock-lakehouse/0.1 (portfolio data pipeline)")
      .GET()
      .build()

    val outcome: Either[(Throwable, Option[Long]), String] =
      try {
        val resp = http.send(request, HttpResponse.BodyHandlers.ofString())
        resp.statusCode() match {
          case s if s >= 200 && s < 300 => Right(resp.body())
          case s if s == 429 || s >= 500 =>
            val retryAfterMs = Option(resp.headers().firstValue("Retry-After").orElse(null)).flatMap(v => scala.util.Try(v.trim.toLong * 1000).toOption)
            Left((ApiError(s, resp.body()), retryAfterMs))
          case s => throw ApiError(s, resp.body()) // 4xx other than 429: our bug, do not retry
        }
      } catch {
        case e: ApiError         => throw e
        case e: java.io.IOException => Left((e, None))
      }

    outcome match {
      case Right(body) => body
      case Left((err, _)) if attempt >= maxAttempts =>
        Log.error("api_request_failed", "url" -> url, "attempts" -> attempt, "error" -> err.getMessage)
        throw err
      case Left((err, retryAfter)) =>
        val backoff = retryAfter.getOrElse(math.min(maxBackoffMs, baseBackoffMs * (1L << (attempt - 1))) + Random.nextInt(500))
        Log.warn("api_request_retry", "url" -> url, "attempt" -> attempt, "sleep_ms" -> backoff, "error" -> err.getMessage)
        Thread.sleep(backoff)
        send(url, attempt + 1)
    }
  }

  private def enc(s: String) = URLEncoder.encode(s, "UTF-8")
}
