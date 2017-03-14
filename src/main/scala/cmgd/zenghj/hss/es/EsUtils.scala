package cmgd.zenghj.hss.es

import java.io.FileReader

import cmgd.zenghj.hss.common.CommonUtils._
import java.net.InetAddress

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.elasticsearch.common.xcontent.XContentFactory._
import org.apache.commons.csv.CSVFormat
import org.elasticsearch.action.bulk.{BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.action.bulk.BulkProcessor.Listener
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory}
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders}
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient
import play.api.libs.json.{JsValue, Json}

import scala.collection.JavaConversions._

/**
  * Created by cookeem on 16/11/1.
  */
object EsUtils {
  implicit val system = ActorSystem("es-stream")
  implicit val materializer = ActorMaterializer()

  var esConn = esConnect()
  var esClient = esConn._1
  var esBulkProcessor = esConn._2
  var esBulkRef = esConn._3

  def esConnect(): (PreBuiltXPackTransportClient, BulkProcessor, ActorRef) = {
    var client: PreBuiltXPackTransportClient = null
    var bulkProcessor: BulkProcessor = null
    var actorRef: ActorRef = null
    try {
      //创建client
      Settings.builder().put()
      val settingsBuilder = Settings.builder().put("cluster.name", configEsClusterName)
      if (configEsUserName != "" && configEsPassword != "") {
        settingsBuilder.put("xpack.security.user", s"$configEsUserName:$configEsPassword")
      }
      val settings = settingsBuilder.build()
      client = new PreBuiltXPackTransportClient(settings)
      configEsHosts.foreach { case (host, port) =>
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port))
      }
      bulkProcessor = BulkProcessor
        .builder(client, new Listener {
          override def beforeBulk(l: Long, bulkRequest: BulkRequest) = {}

          override def afterBulk(l: Long, bulkRequest: BulkRequest, bulkResponse: BulkResponse) = {}

          override def afterBulk(l: Long, bulkRequest: BulkRequest, throwable: Throwable) = {}
        })
        .setBulkActions(1000)
        .setConcurrentRequests(10)
        .setFlushInterval(TimeValue.timeValueSeconds(5))
        .build()

      //把source暴露成一个actorRef,用于接收消息
      val source: Source[(String, XContentBuilder), ActorRef] = Source.actorRef[(String, XContentBuilder)](Int.MaxValue, OverflowStrategy.fail)
      actorRef = Flow[(String, XContentBuilder)]
        .to(Sink.foreach { case (typeName, jsonContent) =>
          bulkProcessor.add(new IndexRequest(configEsIndexName, typeName).source(jsonContent))
        })
        .runWith(source)
    } catch { case e: Throwable =>
      consoleLog("ERROR", s"Connect to elasticsearch error: ${e.getClass}, ${e.getMessage}, ${e.getCause}")
    }
    (client, bulkProcessor, actorRef)
  }

  def esIndexInit() = {
    var errmsg = ""
    try {
      val isExists = esClient.admin().indices().prepareExists(configEsIndexName).execute().actionGet().isExists
      if (!isExists) {
        val indexMapping = esClient.admin().indices().prepareCreate(configEsIndexName)
          .setSettings(
            XContentFactory.jsonBuilder()
              .startObject()
                .field("number_of_shards", configEsNumberOfShards)
                .field("number_of_replicas", configEsNumberOfReplicas)
              .endObject()
          )
          .addMapping(configEsTypeNameEric,
            XContentFactory.jsonBuilder()
              .startObject()
                .startObject(configEsTypeNameEric)
                  .startObject("properties")
                    .startObject("SubLogId")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("Target")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("ExecuteTime")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("FullRequest")
                      .field("type", "string")
                      .field("analyzer", "standard")
                    .endObject()
                    .startObject("LogType(v1.1)")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    //注意,使用了analyzer的字段不能进行排序
                    .startObject("StartTime")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("Operation")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("ResponseCode")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("TransactionId")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("Hostname")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("User")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("Protocol")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("RootLogId")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("FullResponse")
                      .field("type", "string")
                      .field("analyzer", "standard")
                    .endObject()
                    .startObject("Instance")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("Status")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                  .endObject()
                .endObject()
              .endObject()
          )
          .addMapping(configEsTypeNameHuawei,
            XContentFactory.jsonBuilder()
              .startObject()
                .startObject(configEsTypeNameHuawei)
                  .startObject("properties")
                    .startObject("SERIAL_NO")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("HLR_INDEX")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("OPERATOR_NAME")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("OPERATION_TIME")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("MML_COMMAND")
                      .field("type", "string")
                      .field("analyzer", "standard")
                    .endObject()
                    .startObject("CMDRESULT")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("BATCH_TASK_ID")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("COMMAND_NO")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("MSG_TYPE")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("IMSI_NO")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("MSISDN_NO")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("ERRORCODE")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                  .endObject()
                .endObject()
              .endObject()
          )
        indexMapping.execute().actionGet()
        Thread.sleep(1 * 1000)
        consoleLog("SUCCESS", s"elasticsearch index $configEsIndexName created")
      }
    } catch {
      case e: Throwable =>
        errmsg = s"elasticsearch index init error: ${e.getClass}, ${e.getMessage}, ${e.getCause}"
        consoleLog("ERROR", errmsg)
    }
    errmsg
  }

  def esBulkInsertEric(filenameTxt: String): Int = {
    val startTime = System.currentTimeMillis()
    var recordCount = 0
    var errmsg = ""
    if (esClient != null) {
      try {
        val in = new FileReader(filenameTxt)
        val records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in)
        var i = 0
        records.foreach { csvRecord =>
          i += 1
          val record = csvRecord.toMap
          val filter = record.containsKey("Operation") && (
            record("Operation") == "MOD_LCK" || record("Operation") == "MOD_TPLEPS"
            )
          if (filter) {
            val jsonContent: XContentBuilder = jsonBuilder().startObject()
            record.foreach { case (k, v) =>
              jsonContent.field(k, v)
            }
            jsonContent.endObject()
            esBulkRef ! (configEsTypeNameEric, jsonContent)
            recordCount += 1
          }
        }
        records.close()
        in.close()

        val sleepMs = recordCount / 10
        consoleLog("INFO", s"eric record: filename = $filenameTxt wait for $sleepMs ms")
        Thread.sleep(sleepMs)

        val duration = Math.round(System.currentTimeMillis() - startTime)
        consoleLog("SUCCESS", s"eric record write to elasticsearch success: filename = $filenameTxt [$recordCount / $i] records # took $duration ms")
      } catch { case e: Throwable =>
        recordCount = -1
        errmsg = s"eric record write to elasticsearch error: filename = $filenameTxt, ${e.getClass}, ${e.getMessage}, ${e.getCause}"
        consoleLog("ERROR", errmsg)
      }
    } else {
      recordCount = -1
      errmsg = s"eric record write to elasticsearch error: filename = $filenameTxt, elasticsearch connect error"
      consoleLog("ERROR", errmsg)
    }
    recordCount
  }

  def esBulkInsertHuawei(filenameTxt: String): Int = {
    val startTime = System.currentTimeMillis()
    var recordCount = 0
    var errmsg = ""
    if (esClient != null) {
      try {
        val in = new FileReader(filenameTxt)
        val records = CSVFormat.RFC4180.parse(in)
        val headerHeads = "SERIAL_NO,HLR_INDEX,OPERATOR_NAME,OPERATION_TIME".split(",").zipWithIndex
        val headerLasts = "CMDRESULT,BATCH_TASK_ID,COMMAND_NO,MSG_TYPE,IMSI_NO,MSISDN_NO,ERRORCODE".split(",").zipWithIndex
        val headerHeadLength = headerHeads.length
        val headerLastLength = headerLasts.length
        var i = 0
        records.foreach { csvRecord =>
          i += 1
          if (i % 10000 == 0) {
            consoleLog("DEBUG", s"process file record $i")
          }
          if (i != 1) {
            val arr = csvRecord.toArray
            val arrLen = arr.length
            var record = Map[String, String]()
            if (arrLen > headerHeadLength) {
              headerHeads.foreach { case (key, index) =>
                record = record ++ Map(key -> arr(index))
                arr(index) = ""
              }
              headerLasts.foreach { case (key, index) =>
                val idx = arrLen - headerLastLength + index
                record = record ++ Map(key -> arr(idx))
                arr(idx) = ""
              }
              val mmlcmd = arr.filter(_ != "").mkString(",")
              record = record ++ Map("MML_COMMAND" -> mmlcmd)
            }
            val filter = record.containsKey("MML_COMMAND") && (
              (record("MML_COMMAND").startsWith("MOD TPLEPSSER:ISDN=") && record("MML_COMMAND").indexOf("TPLID=") > -1) ||
                (record("MML_COMMAND").startsWith("MOD EPSSER:ISDN=") && record("MML_COMMAND").indexOf("PROV=ADDPDNCNTX,EPSAPNQOSTPLID=") > -1)
              )
            if (filter) {
              val jsonContent: XContentBuilder = jsonBuilder().startObject()
              record.foreach { case (k, v) =>
                jsonContent.field(k, v)
              }
              jsonContent.endObject()
              esBulkRef ! (configEsTypeNameHuawei, jsonContent)
              recordCount += 1
            }
          }
        }
        records.close()
        in.close()

        val sleepMs = recordCount / 10
        consoleLog("INFO", s"huawei record: filename = $filenameTxt wait for $sleepMs ms")
        Thread.sleep(sleepMs)

        val duration = Math.round(System.currentTimeMillis() - startTime)
        consoleLog("SUCCESS", s"huawei record write to elasticsearch success: filename = $filenameTxt [$recordCount / $i] records # took $duration ms")
      } catch { case e: Throwable =>
        recordCount = -1
        errmsg = s"huawei record write to elasticsearch error: filename = $filenameTxt, ${e.getClass}, ${e.getMessage}, ${e.getCause}, ${e.getStackTrace.mkString("\n")}"
        consoleLog("ERROR", errmsg)
      }
    } else {
      recordCount = -1
      errmsg = s"hauwei record write to elasticsearch error: filename = $filenameTxt, elasticsearch connect error"
      consoleLog("ERROR", errmsg)
    }
    recordCount
  }

  def esQuery(searchType: Int = 0, fields: Array[String] = Array[String](), page: Int = 1, count: Int = 10, descSort: Boolean = true, fromStartTime: String = "", toStartTime: String = "", termFields: Array[(String, String)] = Array[(String, String)]()): JsValue = {
    var success = 0
    var errmsg = ""
    var rscount = 0L
    var took = 0L
    var data = Array[JsValue]()
    var typeName = configEsTypeNameEric
    if (searchType == 1) {
      typeName = configEsTypeNameHuawei
    }
    if (esClient != null) {
      try {
        val request = esClient.prepareSearch(configEsIndexName)
          .setTypes(typeName)
          .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
        if (fields.nonEmpty) {
          request.setFetchSource(fields, Array[String]())
        }
        var iPage = 1
        var iCount = 10
        if (page > 0) {
          iPage = page
        }
        if (count > 0) {
          iCount = count
        }
        request.setFrom((iPage - 1) * iCount).setSize(iCount)
        if (searchType == 1) {
          if (descSort) {
            request.addSort("OPERATION_TIME", SortOrder.DESC)
          } else {
            request.addSort("OPERATION_TIME", SortOrder.ASC)
          }
          val highlightBuilder = new HighlightBuilder()
          highlightBuilder.preTags("""##begin##""").postTags("""##end##""")
          request.highlighter(highlightBuilder)
          if (fromStartTime != "" && toStartTime != "" && fromStartTime < toStartTime) {
            request.setPostFilter(
              QueryBuilders.rangeQuery("OPERATION_TIME")
                .gte(fromStartTime)
                .lte(toStartTime)
            )
          }
          val query: BoolQueryBuilder = QueryBuilders.boolQuery()
          if (termFields.nonEmpty) {
            termFields.foreach { case (field, term) =>
              if (term.trim != "") {
                highlightBuilder.field(field, 0, 0)
                if (field == "MML_COMMAND" || field == "IMSI_NO" || field == "MSISDN_NO") {
                  term.split("\\W+").foreach { str =>
                    query.must(QueryBuilders.termQuery(field, str.toLowerCase))
                  }
                } else {
                  query.must(QueryBuilders.termQuery(field, term))
                }
              }
            }
          }
          request.highlighter(highlightBuilder)
          request.setQuery(query)
        } else {
          if (descSort) {
            request.addSort("StartTime", SortOrder.DESC)
          } else {
            request.addSort("StartTime", SortOrder.ASC)
          }
          val highlightBuilder = new HighlightBuilder()
          highlightBuilder.preTags("""##begin##""").postTags("""##end##""")
          request.highlighter(highlightBuilder)
          if (fromStartTime != "" && toStartTime != "" && fromStartTime < toStartTime) {
            request.setPostFilter(
              QueryBuilders.rangeQuery("StartTime")
                .gte(fromStartTime)
                .lte(toStartTime)
            )
          }
          val query: BoolQueryBuilder = QueryBuilders.boolQuery()
          if (termFields.nonEmpty) {
            termFields.foreach { case (field, term) =>
              if (term.trim != "") {
                highlightBuilder.field(field, 0, 0)
                if (field == "FullRequest" || field == "FullResponse" || field == "Target" || field == "RootLogId") {
                  term.split("\\W+").foreach { str =>
                    query.must(QueryBuilders.termQuery(field, str.toLowerCase))
                  }
                } else {
                  query.must(QueryBuilders.termQuery(field, term))
                }
              }
            }
          }
          request.highlighter(highlightBuilder)
          request.setQuery(query)
        }
        //执行查询
        val response = request.execute().actionGet()

        took = response.getTook.getMillis
        rscount = response.getHits.getTotalHits
        response.getHits.getHits.foreach { sh =>
          val item = sh.getSource.map { case (k, v) =>
            if (sh.highlightFields.containsKey(k)) {
              (k, sh.highlightFields.get(k).fragments().mkString)
            } else {
              (k, v.toString)
            }
          }.toMap
          data = data :+ Json.toJson(item)
        }
        success = 1
      } catch {
        case e: Throwable =>
          consoleLog("ERROR", s"esQuery error: fields = ${fields.mkString(",")}, fromStartTime = $fromStartTime, toStartTime = $toStartTime, termFields = ${termFields.mkString(",")}. ${e.getClass}, ${e.getMessage}, ${e.getCause}")
          errmsg = s"esQuery error: fields = ${fields.mkString(",")}, fromStartTime = $fromStartTime, toStartTime = $toStartTime, termFields = ${termFields.mkString(",")}."
      }
    } else {
      errmsg = s"esQuery error: elasticsearch connect error"
      consoleLog("ERROR", errmsg)
    }
    Json.obj(
      "success" -> success,
      "errmsg" -> errmsg,
      "took" -> took,
      "rscount" -> rscount,
      "data" -> data
    )
  }
}
