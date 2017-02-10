package cmgd.zenghj.hss.es

import java.io.FileReader

import cmgd.zenghj.hss.common.CommonUtils._
import java.net.InetAddress

import org.elasticsearch.common.xcontent.XContentFactory._
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory}
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders}
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.transport.client.PreBuiltTransportClient
import play.api.libs.json.{JsValue, Json}

import scala.collection.JavaConversions._

/**
  * Created by cookeem on 16/11/1.
  */
object EsUtils {
  var client: PreBuiltTransportClient = _

  try {
    //创建client
    val settings = Settings
      .builder()
      .put("cluster.name", configEsClusterName).build()
    client = new PreBuiltTransportClient(settings)

    configEsHosts.foreach { case (host, port) =>
      client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port))
    }
  } catch { case e: Throwable =>
    consoleLog("ERROR", s"Connect to elasticsearch error: ${e.getClass}, ${e.getMessage}, ${e.getCause}, ${e.getStackTrace.mkString("\n")}")
  }

  indexInit()

  def indexInit() = {
    try {
      val isExists = client.admin().indices().prepareExists(configEsIndexName).execute().actionGet().isExists
      if (!isExists) {
        val indexMapping = client.admin().indices().prepareCreate(configEsIndexName)
          .setSettings(
            XContentFactory.jsonBuilder()
              .startObject()
              .field("number_of_shards", configEsNumberOfShards)
              .field("number_of_replicas", configEsNumberOfReplicas)
              .endObject()
          )
          .addMapping(configEsTypeName,
            XContentFactory.jsonBuilder()
              .startObject()
              .startObject(configEsTypeName)
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
              .startObject("LogType")
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
        indexMapping.execute().actionGet()
        Thread.sleep(1000)
        consoleLog("SUCCESS", s"elasticsearch index $configEsIndexName created")
      }
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"elasticsearch index init error: ${e.getClass}, ${e.getMessage}, ${e.getCause}, ${e.getStackTrace.mkString("\n")}")
    }
  }

  def bulkInsert(filename: String): Int = {
    val startTime = System.currentTimeMillis()
    var recordCount = 0
    var errmsg = ""
    if (client != null) {
      try {
        val in = new FileReader(filename)
        val records: CSVParser = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in)
        val bulkRequest = client.prepareBulk()
        records.map(_.toMap).filter { record =>
          record("Operation") == "MOD_LCK" || record("Operation") == "MOD_TPLEPS"
        }.foreach { record =>
          val jsonContent: XContentBuilder = jsonBuilder().startObject()
          record.foreach { case (k, v) =>
            jsonContent.field(k, v)
          }
          jsonContent.endObject()
          bulkRequest.add(
            client.prepareIndex(configEsIndexName, configEsTypeName).setSource(jsonContent)
          )
          recordCount = recordCount + 1
        }
        val bulkResponse = bulkRequest.get()
        if (bulkResponse.hasFailures) {
          errmsg = s"Write to elasticsearch error: filename = $filename, ${bulkResponse.buildFailureMessage()}"
          consoleLog("ERROR", errmsg)
        }
        val duration = Math.round(System.currentTimeMillis() - startTime)
        consoleLog("SUCCESS", s"file bulk insert to elasticsearch success: filename = $filename $recordCount records # took $duration ms")
      } catch { case e: Throwable =>
        recordCount = -1
        errmsg = s"Write to elasticsearch error: filename = $filename, ${e.getClass}, ${e.getMessage}, ${e.getCause}, ${e.getLocalizedMessage}, ${e.getStackTrace.mkString("\n")}"
        consoleLog("ERROR", errmsg)
      }
    } else {
      recordCount = -1
      errmsg = s"Write to elasticsearch error: filename = $filename, elasticsearch connect error"
      consoleLog("ERROR", errmsg)
    }
    recordCount
  }

  def esQuery(fields: Array[String] = Array[String](), page: Int = 1, count: Int = 10, descSort: Boolean = true, fromStartTime: String = "", toStartTime: String = "", termFields: Array[(String, String)] = Array[(String, String)]()): JsValue = {
    var success = 0
    var errmsg = ""
    var rscount = 0L
    var took = 0L
    var data = Array[JsValue]()
    if (client != null) {
      try {
        val request = client.prepareSearch(configEsIndexName)
          .setTypes(configEsTypeName)
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
          consoleLog("ERROR", s"esQuery error: fields = ${fields.mkString(",")}, fromStartTime = $fromStartTime, toStartTime = $toStartTime, termFields = ${termFields.mkString(",")}. ${e.getMessage}, ${e.getCause}, ${e.getClass}, ${e.getStackTrace.mkString("\n")}")
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
