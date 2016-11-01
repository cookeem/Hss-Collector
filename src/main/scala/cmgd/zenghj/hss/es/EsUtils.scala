package cmgd.zenghj.hss.es

import java.io.FileReader

import cmgd.zenghj.hss.common.CommonUtils._
import java.net.InetAddress

import org.elasticsearch.common.xcontent.XContentFactory._
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory}

import scala.collection.JavaConversions._

/**
  * Created by cookeem on 16/11/1.
  */
object EsUtils {
  var client: TransportClient = _

  try {
    //创建client
    val settings = Settings
      .settingsBuilder()
      .put("cluster.name", configEsClusterName).build()
    client = TransportClient
      .builder()
      .settings(settings)
      .build()

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
              .field("analyzer", "standard")
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
              .field("analyzer", "standard")
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
    try {
      val in = new FileReader(filename)
      val records: CSVParser = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in)
      val bulkRequest = client.prepareBulk()
      records.foreach { record =>
        val jsonContent: XContentBuilder = jsonBuilder().startObject()
        record.toMap.foreach { case (k, v) =>
          jsonContent.field(k, v)
          recordCount = recordCount + 1
        }
        jsonContent.endObject()
        bulkRequest.add(
          client.prepareIndex(configEsIndexName, configEsTypeName).setSource(jsonContent)
        )
      }
      val bulkResponse = bulkRequest.get()
      if (bulkResponse.hasFailures) {
        errmsg = s"Write to elasticsearch error: ${bulkResponse.buildFailureMessage()}"
        consoleLog("ERROR", errmsg)
      }
      val duration = Math.round(System.currentTimeMillis() - startTime)
      consoleLog("SUCCESS", s"file bulk insert to elasticsearch success: filename = $filename $recordCount records # took $duration ms")
    } catch { case e: Throwable =>
      errmsg = s"Write to elasticsearch error: ${e.getClass}, ${e.getMessage}, ${e.getCause}, ${e.getStackTrace.mkString("\n")}"
      consoleLog("ERROR", errmsg)
    }
    recordCount
  }

}
