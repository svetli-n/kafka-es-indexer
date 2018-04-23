import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

class ElasticIndexer {

  private int id;
  private TransportClient esClient;
  private String clusterName;
  private String clusterIP;
  private int clusterPort;
  private String indexName;
  private String docType;

  final static Logger logger = Logger.getLogger(ElasticIndexer.class);

  public ElasticIndexer(Properties props, int id) {

    this.id = id;
    this.clusterName = props.getProperty("es.clusterName");
    this.clusterIP = props.getProperty("es.clusterIP");
    this.clusterPort = Integer.valueOf(props.getProperty("es.clusterPort"));
    this.indexName = props.getProperty("es.indexName");
    this.docType = props.getProperty("es.docType");

    Settings settings = Settings.settingsBuilder()
        .put("cluster.name", clusterName)
        .build();

    try {
      esClient = TransportClient.builder().settings(settings).build()
          .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(clusterIP), clusterPort));
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }

  }


  public void bulkIndex(List<String> docs) {

    BulkRequestBuilder bulkRequest = esClient.prepareBulk();

    for (String doc : docs) {
        bulkRequest.add(esClient.prepareIndex(indexName, docType).setSource(doc));
    }
    BulkResponse bulkResponse = bulkRequest.get();

    if (bulkResponse.hasFailures()) {
      logger.error(String.format(bulkResponse.buildFailureMessage()));
    }
    else {
      logger.info(String.format("sent %d docs to elaticsearch ", docs.size()));
    }
  }


  void stop() {
    esClient.close();
    logger.info("Closing es client");
  }

}
