package cmgd.zenghj.hss.restful

/**
  * Created by cookeem on 16/11/4.
  */
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import cmgd.zenghj.hss.common.CommonUtils._
import cmgd.zenghj.hss.es.EsUtils._
import akka.http.scaladsl.server.Directives._
import play.api.libs.json.Json

import scala.async.Async._
import scala.concurrent.ExecutionContext

object RestUtils {
  def queryRoute(implicit ec: ExecutionContext) = post {
    path("json" / "query") {
      formFieldMap { params =>
        val fieldsStr = paramsGetString(params, "fields", "") //逗号分隔格式
        val page = paramsGetInt(params, "page", 1)
        val count = paramsGetInt(params, "count", 10)
        val descSortNum = paramsGetInt(params, "descSort", 1) //等于1表示true
        val fromStartTime = paramsGetString(params, "fromStartTime", "")
        val toStartTime = paramsGetString(params, "toStartTime", "")
        //json字符串格式: [{"field":"f1", "term":"t1"},{"field":"f2", "term":"t2"}]
        val termFieldsStr = paramsGetString(params, "termFields", "")
        val termFieldsJsonStr = termFieldsStr
        val jsonFuture = async {
          val fields = fieldsStr.split(",")
          val descSort = if (descSortNum == 1) true else false
          val termFieldMaps: Array[Map[String, String]] = Json.parse(termFieldsJsonStr).as[Array[Map[String, String]]]
          val termFields = termFieldMaps.map{ m =>
            val field = m("field")
            val term = m("term")
            (field, term)
          }
          val json = esQuery(
            fields = fields,
            page = page,
            count = count,
            descSort = descSort,
            fromStartTime = fromStartTime,
            toStartTime = toStartTime,
            termFields = termFields
          )
          val jsonBody = Json.stringify(json)
          HttpEntity(ContentTypes.`application/json`, jsonBody)
        } recover {
          case e: Throwable =>
            val errmsg = s"query error: fieldsStr = $fieldsStr, fromStartTime = $fromStartTime, toStartTime = $toStartTime, termFieldsStr = $termFieldsStr. ${e.getMessage}, ${e.getCause}, ${e.getClass}, ${e.getStackTrace.mkString("\n")}"
            consoleLog("ERROR", errmsg)
            val json = Json.obj(
              "errmsg" -> errmsg
            )
            val jsonBody = Json.stringify(json)
            HttpEntity(ContentTypes.`application/json`, jsonBody)
        }
        complete(jsonFuture)
      }
    }
  }


}
