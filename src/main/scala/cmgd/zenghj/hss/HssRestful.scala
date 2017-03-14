package cmgd.zenghj.hss

/**
  * Created by cookeem on 16/11/4.
  */
import java.net.InetAddress

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{RejectionHandler, Route, StandardRoute}
import akka.stream.ActorMaterializer
import cmgd.zenghj.hss.common.CommonUtils._
import cmgd.zenghj.hss.restful.RestUtils._
import cmgd.zenghj.hss.es.EsUtils._
import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime

import scala.async.Async._

/**
  * Created by cookeem on 16/8/10.
  */
object HssRestful extends App {

  val httpConfigStr = s"""
    akka.http {
      server {
        remote-address-header = on
        raw-request-uri-header = on
      }
    }
  """

  val configHttp = ConfigFactory.parseString(httpConfigStr)
  implicit val system = ActorSystem("hss-restful", configHttp)
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val route =
    get {
      pathSingleSlash {
        redirect("/index.html", StatusCodes.PermanentRedirect)
      }
    } ~
    queryRoute ~
    get {
      pathPrefix("") {
        getFromDirectory("www")
      }
    } ~
    extractRequest { request =>
      badRequest(request)
    }

  def badRequest(request: HttpRequest): StandardRoute = {
    val method = request.method.value.toLowerCase
    val path = request.getUri().path()
    val queryString = request.getUri().rawQueryString().orElse("")
    method match {
      case _ =>
        complete((StatusCodes.NotFound, "404 error, resource not found!"))
    }
  }

  //路由日志记录
  def logDuration(inner: Route): Route = { ctx =>
    val rejectionHandler = RejectionHandler.default
    val start = System.currentTimeMillis()
    // handling rejections here so that we get proper status codes
    val innerRejectionsHandled = handleRejections(rejectionHandler)(inner)
    mapResponse { resp =>
      val currentTime = new DateTime()
      val currentTimeStr = currentTime.toString("yyyy-MM-dd HH:mm:ss")
      val duration = System.currentTimeMillis() - start
      var remoteAddress = ""
      var userAgent = ""
      var rawUri = ""
      ctx.request.headers.foreach(header => {
        if (header.name() == "X-Real-Ip") {
          remoteAddress = header.value()
        }
        if (header.name() == "Remote-Address") {
          remoteAddress = header.value()
        }
        if (header.name() == "User-Agent") {
          userAgent = header.value()
        }
        if (header.name() == "Raw-Request-URI") {
          rawUri = header.value()
        }
      })
      async {
        val mapPattern = Seq("css", "images", "js", "lib")
        var isIgnore = false
        mapPattern.foreach(mp =>
          isIgnore = isIgnore || rawUri.startsWith(s"/$mp/")
        )
        if (!isIgnore) {
          println(s"# $currentTimeStr ${ctx.request.uri} [$remoteAddress] [${ctx.request.method.name}] [${resp.status.value}] [$userAgent] took: ${duration}ms")
        }
      }
      resp
    }(innerRejectionsHandled)(ctx)
  }

  //等待elasticsearch连接成功
  var probeEs = esIndexInit()
  while (probeEs != "") {
    consoleLog("ERROR", s"Connect elasticsearch error, wait 5 seconds to reconnect!")
    Thread.sleep(5 * 1000)
    esConn = esConnect()
    esClient = esConn._1
    esBulkProcessor = esConn._2
    esBulkRef = esConn._3
    probeEs = esIndexInit()
  }
  consoleLog("SUCCESS", s"Connect elasticsearch success")

  Http().bindAndHandle(logDuration(route), "0.0.0.0", configHttpPort)
  consoleLog("INFO", s"Http server started at http://0.0.0.0:$configHttpPort")
}
